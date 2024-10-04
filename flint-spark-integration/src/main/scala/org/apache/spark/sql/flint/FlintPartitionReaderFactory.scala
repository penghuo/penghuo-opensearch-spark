/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.flint

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.flint.config.FlintSparkConf
import org.apache.spark.sql.flint.storage.FlintQueryCompiler
import org.apache.spark.sql.types.StructType

case class FlintPartitionReaderFactory(
    schema: StructType,
    options: FlintSparkConf,
    pushedPredicates: Array[Predicate],
    pushedAggregate: Option[Aggregation])
    extends PartitionReaderFactory
    with Logging {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val query = FlintQueryCompiler(schema).compile(pushedPredicates)
    val aggBuilder = pushedAggregate.map(agg => FlintQueryCompiler(schema).compileAgg(agg))
    val table = partition.asInstanceOf[OpenSearchSplit].table
    logInfo(s"prepare table")
    table.prepare()
    val reader = table.createReader(query, aggBuilder)
    new FlintPartitionReader(
      reader,
      schema,
      options,
      reader.queryReader(),
      pushedAggregate.isDefined)
  }
}
