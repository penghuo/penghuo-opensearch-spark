/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.table

import org.opensearch.flint.core.FlintOptions
import org.opensearch.flint.core.storage.{FlintOpenSearchClient, FlintReader, OpenSearchClientUtils, OpenSearchPITSearchAfterQueryReader}
import org.opensearch.flint.core.table.OpenSearchIndexShardTable.SHARD_ID_PREFERENCE
import org.opensearch.flint.table.{MetaData, OpenSearchIndexTable, Table}
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.search.sort.SortOrder

class OpenSearchIndexShardTable(metaData: MetaData, option: FlintOptions, shardId: Int)
    extends OpenSearchIndexTable(metaData, option) {
  override def snapshot(): Table = {
    throw new UnsupportedOperationException("Can't slice OpenSearchIndexShardTable")
  }

  override def slice(): Seq[Table] = {
    throw new UnsupportedOperationException("Can't slice OpenSearchIndexShardTable")
  }

  override def createReader(query: String): FlintReader = {
    new OpenSearchPITSearchAfterQueryReader(
      OpenSearchClientUtils.createClient(option),
      searchSourceBuilder(query),
      name,
      SHARD_ID_PREFERENCE(shardId))
  }

  protected def searchSourceBuilder(query: String): SearchSourceBuilder = {
    new SearchSourceBuilder()
      .query(FlintOpenSearchClient.queryBuilder(query))
      .size(pageSize)
      .sort("_doc", SortOrder.ASC)
  }
}

object OpenSearchIndexShardTable {
  val SHARD_ID_PREFERENCE: Int => String = shardId =>
    if (shardId == -1) "" else s"_shards:$shardId"
}
