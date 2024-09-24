/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.snapshot

import org.opensearch.flint.spark.skipping.bloomfilter.BloomFilterMightContain
import org.opensearch.snapshot.utils.{SnapshotParams, SnapshotTableMetadata}

import org.apache.spark.sql.connector.expressions.{NamedReference, SortOrder}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownRequiredColumns, SupportsPushDownTopN, SupportsPushDownV2Filters}
import org.apache.spark.sql.types.StructType

class SnapshotScanBuilder(
    schema: StructType,
    snapshotParams: SnapshotParams,
    snapshotTableMetadata: SnapshotTableMetadata)
    extends ScanBuilder
    with SupportsPushDownV2Filters
    with SupportsPushDownTopN
    with SupportsPushDownRequiredColumns {

  private var pushedPredicate = Array.empty[Predicate]
  private var pushedSort: String = ""
  private var pushedLimit = 100
  private var requiredSchema: StructType = null

  override def build(): Scan =
    new SnapshotScan(
      schema,
      snapshotParams,
      snapshotTableMetadata,
      pushedPredicate,
      pushedSort,
      pushedLimit,
      requiredSchema)

  override def pushPredicates(predicates: Array[Predicate]): Array[Predicate] = {
    val (pushed, unSupported) =
      predicates.partition(p => {
        val q = QueryCompiler(schema).compile(p)
        !q.toString.equalsIgnoreCase("*:*")
      })
    pushedPredicate = pushed

    // FIXME, assume we can pushdown everything
//    unSupported
    Array.empty[Predicate]
  }

  override def pushedPredicates(): Array[Predicate] = pushedPredicate
    .filterNot(_.name().equalsIgnoreCase(BloomFilterMightContain.NAME))

  override def pushTopN(orders: Array[SortOrder], limit: Int): Boolean = {
    if (orders.size > 1) {
      return false
    }
    pushedLimit = limit
    orders.head.expression() match {
      case reference: NamedReference => pushedSort = reference.fieldNames().head
      case _ => None
    }
    true
  }

  override def pruneColumns(requiredSchema: StructType): Unit = {
    this.requiredSchema = requiredSchema
  }
}
