/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.snapshot

import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.repositories.IndexId
import org.opensearch.snapshot.utils.{SnapshotParams, SnapshotTableMetadata}
import org.opensearch.snapshots.SnapshotInfo

import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType

class SnapshotBatch(
    val schema: StructType,
    val snapshotParams: SnapshotParams,
    val snapshotTableMetadata: SnapshotTableMetadata,
    pushedPredicates: Array[Predicate],
    pushedSort: String,
    pushedLimit: Int,
    requiredSchema: StructType)
    extends Batch {

  override def planInputPartitions(): Array[InputPartition] = {
    val indexMetadata: IndexMetadata = snapshotTableMetadata.getIndexMetadata
    val snapshotInfo: SnapshotInfo = snapshotTableMetadata.getSnapshotInfo
    val indexId: IndexId = snapshotTableMetadata.getIndexId

    (0 until indexMetadata.getNumberOfShards).map { i =>
      new SnapshotInputPartition(
        snapshotInfo.snapshotId().getUUID,
        indexId.getId,
        indexId.getName,
        i.toString)
    }.toArray
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    new SnapshotPartitionReaderFactory(
      schema,
      snapshotParams,
      pushedPredicates,
      pushedSort,
      pushedLimit,
      requiredSchema: StructType)
  }
}
