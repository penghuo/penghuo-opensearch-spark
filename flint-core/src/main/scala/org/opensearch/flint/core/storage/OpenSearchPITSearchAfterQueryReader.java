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
import java.util.Arrays;
import java.util.Optional;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class OpenSearchPITSearchAfterQueryReader extends OpenSearchReader {

  private static final Logger LOG = Logger.getLogger(OpenSearchPITSearchAfterQueryReader.class.getName());

  private Object[] search_after = null;

  public OpenSearchPITSearchAfterQueryReader(IRestHighLevelClient client, String indexName, SearchSourceBuilder searchSourceBuilder) {
    super(client, new SearchRequest().source(searchSourceBuilder));
  }

  /**
   * search.
   */
  Optional<SearchResponse> search(SearchRequest request) {
    try {
      Optional<SearchResponse> response;
      if (search_after != null) {
        LOG.info("add search_after " + Arrays.stream(search_after).map(Object::toString).collect(
            Collectors.joining(",")));
        SearchSourceBuilder source = request.source();
        source.searchAfter(search_after);
      }
      LOG.info("START Request " + request.getDescription());
      response = Optional.of(client.search(request, RequestOptions.DEFAULT));
      int length = response.get().getHits().getHits().length;
      LOG.info( "DONE length " + length);
      // no more result
      if (response.get().getHits().getHits().length == 0) {
        return Optional.empty();
      }
      // get search_after key
      search_after = response.get().getHits().getAt(length - 1).getSortValues();
      LOG.info("get search_after " + Arrays.stream(search_after).map(Object::toString).collect(
          Collectors.joining(",")));
      return response;
    } catch (Exception e) {
      LOG.warning(e.getMessage());
      throw new RuntimeException(e);
    }
  }

  /**
   * nothing to clean
   */
  void clean() throws IOException {}
}
