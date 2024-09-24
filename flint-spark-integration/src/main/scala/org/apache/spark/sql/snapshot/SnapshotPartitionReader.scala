/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.snapshot

import scala.collection.mutable.ArrayBuffer

import org.apache.lucene.index._
import org.apache.lucene.search._
import org.opensearch.snapshot.utils.{SnapshotParams, SnapshotUtil}

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types._

class SnapshotPartitionReader(
    snapshotParams: SnapshotParams,
    schema: StructType,
    snapshotInputPartition: SnapshotInputPartition,
    query: Query,
    pushedSort: String,
    pushedLimit: Int,
    requiredSchema: StructType)
    extends PartitionReader[InternalRow]
    with Logging {

  private var indexReader: IndexReader = null
  private var indexSearcher: IndexSearcher = null
  private var taskId = -1L

  var rows: ArrayBuffer[InternalRow] = ArrayBuffer.empty
  var init = true

  override def next(): Boolean = {
    if (init) {
      init = false
      taskId = TaskContext.get.taskAttemptId()

      var startTime = System.currentTimeMillis()
      indexReader = DirectoryReader.open(
        SnapshotUtil.getRemoteSnapShotDirectory(
          snapshotParams,
          snapshotInputPartition.snapshotUUID,
          snapshotInputPartition.indexId,
          snapshotInputPartition.shardId))
      indexSearcher = new IndexSearcher(indexReader)
      var endTime = System.currentTimeMillis()
      logInfo(
        s"TID-$taskId, Shard-${snapshotInputPartition.shardId} Time taken to init: ${endTime -
            startTime} ms")

      startTime = System.currentTimeMillis()
      indexSearcher.search(query, SearchAfterCollector(requiredSchema, rows))
      endTime = System.currentTimeMillis()
      logInfo(
        s"TID-$taskId, Shard-${snapshotInputPartition.shardId} Time taken to " +
          s"search: ${endTime -
              startTime} ms")
    }

    rows.iterator.hasNext
  }

  override def get(): InternalRow = {
    rows.iterator.next()
  }

  override def close(): Unit = {
    if (indexReader != null) {
      logInfo(s"Close ${snapshotInputPartition.shardId}")
      indexReader.close()
    }
  }
}
