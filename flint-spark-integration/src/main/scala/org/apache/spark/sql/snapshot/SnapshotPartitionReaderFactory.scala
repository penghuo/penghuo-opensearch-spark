/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.snapshot

import org.opensearch.snapshot.utils.SnapshotParams

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

class SnapshotPartitionReaderFactory(
    schema: StructType,
    snapshotParams: SnapshotParams,
    pushedPredicates: Array[Predicate],
    pushedSort: String,
    pushedLimit: Int,
    requiredSchema: StructType)
    extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val query = QueryCompiler(schema).compile(pushedPredicates)
    new SnapshotPartitionReader(
      snapshotParams,
      schema,
      partition.asInstanceOf[SnapshotInputPartition],
      query,
      pushedSort,
      pushedLimit,
      requiredSchema)
  }

  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] = {
    val query = QueryCompiler(schema).compile(pushedPredicates)
    new SnapshotPartitionColumnReader(
      snapshotParams,
      schema,
      partition.asInstanceOf[SnapshotInputPartition],
      query,
      pushedSort,
      pushedLimit,
      requiredSchema)
  }

  override def supportColumnarReads(partition: InputPartition): Boolean = true
}
