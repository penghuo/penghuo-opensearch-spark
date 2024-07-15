/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.auth;

import static org.apache.http.protocol.HttpCoreContext.HTTP_TARGET_HOST;

import com.amazonaws.DefaultRequest;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.Signer;
import com.amazonaws.http.HttpMethodName;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Logger;

import org.apache.http.Header;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpException;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.message.BasicHeader;
import org.apache.http.protocol.HttpContext;

/**
 * From https://github.com/opensearch-project/sql-jdbc/blob/main/src/main/java/org/opensearch/jdbc/transport/http/auth/aws/AWSRequestSigningApacheInterceptor.java
 * An {@link HttpRequestInterceptor} that signs requests using any AWS {@link Signer}
 * and {@link AWSCredentialsProvider}.
 */
public class AWSRequestSigningApacheInterceptor implements HttpRequestInterceptor {
  private static final Logger LOG = Logger.getLogger(AWSRequestSigningApacheInterceptor.class.getName());

  /**
   * The service that we're connecting to. Technically not necessary.
   * Could be used by a future Signer, though.
   */
  private final String service;

  /**
   * The particular signer implementation.
   */
  private final Signer signer;

  /**
   * The source of AWS credentials for signing.
   */
  private final AWSCredentialsProvider awsCredentialsProvider;

  /**
   *
   * @param service service that we're connecting to
   * @param signer particular signer implementation
   * @param awsCredentialsProvider source of AWS credentials for signing
   */
  public AWSRequestSigningApacheInterceptor(final String service,
      final Signer signer,
      final AWSCredentialsProvider awsCredentialsProvider) {
    this.service = service;
    this.signer = signer;
    this.awsCredentialsProvider = awsCredentialsProvider;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void process(final HttpRequest request, final HttpContext context)
      throws HttpException, IOException {
    URIBuilder uriBuilder;
    try {
      uriBuilder = new URIBuilder(request.getRequestLine().getUri());
    } catch (URISyntaxException e) {
      throw new IOException("Invalid URI" , e);
    }

    // Copy Apache HttpRequest to AWS DefaultRequest
    DefaultRequest<?> signableRequest = new DefaultRequest<>(service);

    HttpHost host = (HttpHost) context.getAttribute(HTTP_TARGET_HOST);
    if (host != null) {
      signableRequest.setEndpoint(URI.create(host.toURI()));
    }
    final HttpMethodName httpMethod =
        HttpMethodName.fromValue(request.getRequestLine().getMethod());
    signableRequest.setHttpMethod(httpMethod);
    try {
      signableRequest.setResourcePath(uriBuilder.build().getRawPath());
    } catch (URISyntaxException e) {
      throw new IOException("Invalid URI" , e);
    }

    if (request instanceof HttpEntityEnclosingRequest) {
      HttpEntityEnclosingRequest httpEntityEnclosingRequest =
          (HttpEntityEnclosingRequest) request;
      if (httpEntityEnclosingRequest.getEntity() != null) {
        signableRequest.setContent(httpEntityEnclosingRequest.getEntity().getContent());
      }
    }
    signableRequest.setParameters(nvpToMapParams(uriBuilder.getQueryParams()));
    signableRequest.setHeaders(headerArrayToMap(request.getAllHeaders()));

    LOG.info("SignableRequest Endpoint: " + signableRequest.getEndpoint());
    LOG.info("SignableRequest ResourcePath: " + signableRequest.getResourcePath());
    LOG.info("SignableRequest HttpMethod: " + signableRequest.getHttpMethod());
    LOG.info("SignableRequest Parameters: " + signableRequest.getParameters());
    LOG.info("SignableRequest Headers: " + signableRequest.getHeaders());

    // Sign it
    signer.sign(signableRequest, awsCredentialsProvider.getCredentials());

    // Now copy everything back
    request.setHeaders(mapToHeaderArray(signableRequest.getHeaders()));
    if (request instanceof HttpEntityEnclosingRequest) {
      HttpEntityEnclosingRequest httpEntityEnclosingRequest =
          (HttpEntityEnclosingRequest) request;
      if (httpEntityEnclosingRequest.getEntity() != null) {
        BasicHttpEntity basicHttpEntity = new BasicHttpEntity();
        basicHttpEntity.setContent(signableRequest.getContent());
        httpEntityEnclosingRequest.setEntity(basicHttpEntity);
      }
    }
  }

  /**
   *
   * @param params list of HTTP query params as NameValuePairs
   * @return a multimap of HTTP query params
   */
  static Map<String, List<String>> nvpToMapParams(final List<NameValuePair> params) {
    Map<String, List<String>> parameterMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    for (NameValuePair nvp : params) {
        LOG.info("name: " + nvp.getName() + " value: " + nvp.getValue());
        List<String> argsList =
            parameterMap.computeIfAbsent(nvp.getName(), k -> new ArrayList<>());
        argsList.add(nvp.getValue());
      }
    return parameterMap;
  }

  /**
   * @param headers modeled Header objects
   * @return a Map of header entries
   */
  private static Map<String, String> headerArrayToMap(final Header[] headers) {
    Map<String, String> headersMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    for (Header header : headers) {
      LOG.info("name: " + header.getName() + " value: " + header.getValue());
      if (!skipHeader(header)) {
        headersMap.put(header.getName(), header.getValue());
      }
    }
    return headersMap;
  }

  /**
   * @param header header line to check
   * @return true if the given header should be excluded when signing
   */
  static boolean skipHeader(final Header header) {
    return ("content-length".equalsIgnoreCase(header.getName())
            && "0".equals(header.getValue())) // Strip Content-Length: 0
            || "host".equalsIgnoreCase(header.getName()) // Host comes from endpoint
            || "connection".equalsIgnoreCase(header.getName()); // Skip setting Connection manually
  }

  /**
   * @param mapHeaders Map of header entries
   * @return modeled Header objects
   */
  private static Header[] mapToHeaderArray(final Map<String, String> mapHeaders) {
    Header[] headers = new Header[mapHeaders.size()];
    int i = 0;
    for (Map.Entry<String, String> headerEntry : mapHeaders.entrySet()) {
      headers[i++] = new BasicHeader(headerEntry.getKey(), headerEntry.getValue());
    }
    return headers;
  }
}
