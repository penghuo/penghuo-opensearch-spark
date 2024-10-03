/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.flint

import org.opensearch.flint.spark.skipping.bloomfilter.BloomFilterMightContain

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownAggregates, SupportsPushDownV2Filters}
import org.apache.spark.sql.execution.AggPushDownUtils
import org.apache.spark.sql.flint.config.FlintSparkConf
import org.apache.spark.sql.flint.storage.FlintQueryCompiler
import org.apache.spark.sql.types.StructType

case class FlintScanBuilder(
    tables: Seq[org.opensearch.flint.core.Table],
    schema: StructType,
    options: FlintSparkConf)
    extends ScanBuilder
    with SupportsPushDownV2Filters
    with SupportsPushDownAggregates
    with Logging {

  private var pushedPredicate = Array.empty[Predicate]
  var pushedAggregations = Option.empty[Aggregation]
  var finalSchema = schema

  override def build(): Scan = {
    FlintScan(tables, finalSchema, options, pushedPredicate, pushedAggregations)
  }

  override def pushPredicates(predicates: Array[Predicate]): Array[Predicate] = {
    val (pushed, unSupported) =
      predicates.partition(FlintQueryCompiler(schema).compile(_).nonEmpty)
    pushedPredicate = pushed
    unSupported
  }

  override def pushedPredicates(): Array[Predicate] = pushedPredicate
    .filterNot(_.name().equalsIgnoreCase(BloomFilterMightContain.NAME))

  override def pushAggregation(aggregation: Aggregation): Boolean = {
    logInfo(s"Pushed aggregation: $aggregation")

    // FIXME. Learn from Spark ParquetScanBuilder
    AggPushDownUtils.getSchemaForPushedAggregation(
      aggregation,
      schema,
      Set.empty,
      Seq.empty) match {
      case Some(schema) =>
        finalSchema = schema
        this.pushedAggregations = Some(aggregation)
        true
      case _ => false
    }
  }
}
