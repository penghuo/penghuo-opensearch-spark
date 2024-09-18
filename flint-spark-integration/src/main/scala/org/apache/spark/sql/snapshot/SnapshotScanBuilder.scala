/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.snapshot

import org.opensearch.flint.spark.skipping.bloomfilter.BloomFilterMightContain
import org.opensearch.snapshot.utils.{SnapshotParams, SnapshotTableMetadata}

import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownV2Filters}
import org.apache.spark.sql.flint.storage.FlintQueryCompiler
import org.apache.spark.sql.types.StructType

class SnapshotScanBuilder(
    schema: StructType,
    snapshotParams: SnapshotParams,
    snapshotTableMetadata: SnapshotTableMetadata)
    extends ScanBuilder
    with SupportsPushDownV2Filters {

  private var pushedPredicate = Array.empty[Predicate]

  override def build(): Scan =
    new SnapshotScan(schema, snapshotParams, snapshotTableMetadata, pushedPredicate)

  override def pushPredicates(predicates: Array[Predicate]): Array[Predicate] = {
    val (pushed, unSupported) =
      predicates.partition(FlintQueryCompiler(schema).compile(_).nonEmpty)
    pushedPredicate = pushed
    unSupported
  }

  override def pushedPredicates(): Array[Predicate] = pushedPredicate
    .filterNot(_.name().equalsIgnoreCase(BloomFilterMightContain.NAME))
}
