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

public class OpenSearchPITSearchAfterQueryReader extends OpenSearchReader {

  private static final Logger LOG = Logger.getLogger(OpenSearchQueryReader.class.getName());

  private int sliceId;

  private Object search_after = null;

  public OpenSearchPITSearchAfterQueryReader(IRestHighLevelClient client, String indexName, SearchSourceBuilder searchSourceBuilder) {
    super(client, new SearchRequest().source(searchSourceBuilder));
  }

  /**
   * search.
   */
  Optional<SearchResponse> search(SearchRequest request) {
    Optional<SearchResponse> response = Optional.empty();
    try {
      if (search_after != null) {
        SearchSourceBuilder source = request.source();
        source.searchAfter(new Object[] {search_after});
        LOG.info("add search_after" + search_after);
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
      search_after = response.get().getHits().getAt(length - 1).getId();
      LOG.info("get search_after " + search_after);
    } catch (Exception e) {
    }
    return response;
  }

  /**
   * nothing to clean
   */
  void clean() throws IOException {}
}
