/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.snapshot

import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.repositories.IndexId
import org.opensearch.snapshot.utils.{SnapshotParams, SnapshotTableMetadata}
import org.opensearch.snapshots.SnapshotInfo

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.ExecutorCacheTaskLocation
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.{BlockManager, BlockManagerId}

class SnapshotBatch(
    val schema: StructType,
    val snapshotParams: SnapshotParams,
    val snapshotTableMetadata: SnapshotTableMetadata,
    pushedPredicates: Array[Predicate],
    pushedSort: String,
    pushedLimit: Int,
    requiredSchema: StructType,
    hasAggregations: Boolean)
    extends Batch
    with Logging {

  override def planInputPartitions(): Array[InputPartition] = {
    val indexMetadata: IndexMetadata = snapshotTableMetadata.getIndexMetadata
    val snapshotInfo: SnapshotInfo = snapshotTableMetadata.getSnapshotInfo
    val indexId: IndexId = snapshotTableMetadata.getIndexId
    val executors = executorLocations

    for (exec <- executors) {
      logInfo(s"total: ${executors.length}, executor: $exec")
    }

    (0 until indexMetadata.getNumberOfShards).map { i =>
//      val preferredLocations = {
//        if (executors.size == 1) executors
//        else {
//          val rotationIndex = i % executors.length
//          executors.drop(rotationIndex) ++ executors.take(rotationIndex)
//        }
//      }
      val preferredLocations = Seq(executors(i % executors.length))
      logInfo(s"Shard-$i, preferredLocations:${preferredLocations.toString()}")
      new SnapshotInputPartition(
        snapshotInfo.snapshotId().getUUID,
        indexId.getId,
        indexId.getName,
        i.toString,
        preferredLocations.toArray[String])
    }.toArray
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    new SnapshotPartitionReaderFactory(
      schema,
      snapshotParams,
      pushedPredicates,
      pushedSort,
      pushedLimit,
      requiredSchema: StructType,
      hasAggregations)
  }

  def executorLocations: Seq[String] = {
    val driverBlockManager: BlockManager = SparkEnv.get.blockManager
    val executorBlockManagerIds: Seq[BlockManagerId] = fetchPeers(driverBlockManager)
    executorBlockManagerIds
      .map(toExecutorLocation)
      .sorted
      .distinct
  }

  private def fetchPeers(blockManager: BlockManager): Seq[BlockManagerId] = {
    val master = blockManager.master
    val id = blockManager.blockManagerId
    master.getPeers(id)
  }

  private def toExecutorLocation(id: BlockManagerId): String =
    ExecutorCacheTaskLocation.apply(id.host, id.executorId).toString
}
