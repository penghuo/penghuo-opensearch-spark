/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.flint.core.IRestHighLevelClient;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.Optional;
import java.util.logging.Logger;

import static org.opensearch.flint.core.metrics.MetricConstants.REQUEST_METADATA_READ_METRIC_PREFIX;

public class OpenSearchSliceQueryReader extends OpenSearchReader {

  private static final Logger LOG = Logger.getLogger(OpenSearchQueryReader.class.getName());

  private boolean called = false;

  private int sliceId;

  public OpenSearchSliceQueryReader(IRestHighLevelClient client, String indexName, SearchSourceBuilder searchSourceBuilder) {
    super(client, new SearchRequest().source(searchSourceBuilder));
    sliceId = searchSourceBuilder.slice().getId();
    LOG.info("slice " + sliceId + " size " + searchSourceBuilder.size());
  }

  /**
   * search.
   */
  Optional<SearchResponse> search(SearchRequest request) {
    Optional<SearchResponse> response = Optional.empty();
    if (called) {
      return response;
    }
    try {
      LOG.info("START slice " + sliceId + " Request " + request.getDescription());
      response = Optional.of(client.search(request, RequestOptions.DEFAULT));
      LOG.info( "DONE slice " + sliceId + " DONE length " + response.get().getHits().getHits().length);
      called = true;
      IRestHighLevelClient.recordOperationSuccess(REQUEST_METADATA_READ_METRIC_PREFIX);
    } catch (Exception e) {
      IRestHighLevelClient.recordOperationFailure(REQUEST_METADATA_READ_METRIC_PREFIX, e);
    }
    return response;
  }

  /**
   * nothing to clean
   */
  void clean() throws IOException {}
}

