package org.opensearch.flint.core.storage;

import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.common.xcontent.XContentType;

import java.io.IOException;

public class OpenSearchUpdater {
  private final String indexName;

  private RestHighLevelClient client;

  public OpenSearchUpdater(RestHighLevelClient client, String indexName) {
    this.client = client;
    this.indexName = indexName;
  }

  public void update(String id, String doc) {
    try {
      UpdateRequest
          updateRequest =
          new UpdateRequest(indexName, id).doc(doc, XContentType.JSON)
              .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
      client.update(updateRequest, RequestOptions.DEFAULT);
    } catch (IOException e) {
      throw new RuntimeException(String.format(
          "Failed to execute update request on index: %s, id: %s",
          indexName,
          id), e);
    }
  }

  public void close() {
    try {
      if (client != null) {
        client.close();
        client = null;
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
