/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.snapshot

import org.opensearch.snapshot.utils.{SnapshotParams, SnapshotTableMetadata}

import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{Batch, Scan}
import org.apache.spark.sql.types.StructType

class SnapshotScan(
    schema: StructType,
    snapshotParams: SnapshotParams,
    snapshotTableMetadata: SnapshotTableMetadata,
    pushedPredicates: Array[Predicate],
    pushedSort: String,
    pushedLimit: Int,
    requiredSchema: StructType)
    extends Scan {
  override def readSchema(): StructType = requiredSchema

  override def description(): String = snapshotParams.getSnapshotName

  override def toBatch: Batch =
    new SnapshotBatch(
      schema,
      snapshotParams,
      snapshotTableMetadata,
      pushedPredicates,
      pushedSort,
      pushedLimit,
      requiredSchema: StructType)
}
