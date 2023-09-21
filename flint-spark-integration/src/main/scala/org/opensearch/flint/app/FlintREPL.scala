/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.app

import org.opensearch.flint.app.FlintCommand.serialize
import org.opensearch.flint.core.{FlintClient, FlintClientBuilder}
import org.opensearch.flint.core.storage.{FlintReader, OpenSearchUpdater}

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.flint.config.FlintSparkConf
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}

/**
 * Spark SQL Application entrypoint
 *
 * @param args(0)
 *   sql query
 * @param args(1)
 *   opensearch index name
 * @param args(2-6)
 *   opensearch connection values required for flint-integration jar. host, port, scheme, auth,
 *   region respectively.
 * @return
 *   write sql query result to given opensearch index
 */
object FlintREPL extends Logging {

  def execute(query: String, spark: SparkSession, resultIndex: String): Unit = {
    val result: DataFrame = spark.sql(query)

    // Get Data
    val data = getFormattedData(result, spark)

    // Write data to OpenSearch index
    data.write
      .format("flint")
      .mode("append")
      .save(resultIndex)
  }

  def read(indexName: String, flintClient: FlintClient): FlintReader = {
    val dsl = ""
    flintClient.createReader(indexName, dsl)
  }

  def update(flintCommand: FlintCommand, updater: OpenSearchUpdater, queryIndex: String): Unit = {
    updater.update(flintCommand.id, serialize(flintCommand))
  }

  def main(args: Array[String]) {
    // parse argument
    val queryIndex = args(0)
    val resultIndex = args(1)

    // init SparkContext
    val conf: SparkConf = new SparkConf()
      .setAppName("FlintJob")
      .set("spark.sql.extensions", "org.opensearch.flint.spark.FlintSparkExtensions")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    // init OpenSearch
    val flintClient = FlintClientBuilder.build(FlintSparkConf().flintOptions())

    val dsl = """{
                |  "term": {
                |    "status": {
                |      "value": "pending"
                |    }
                |  }
                |}""".stripMargin
    val flintUpdater = flintClient.createUpdater(queryIndex)

    while (true) {
      logInfo(s"""read from ${queryIndex}""")
      val flintReader = flintClient.createReader(queryIndex, dsl)
      while (flintReader.hasNext) {
        val command = flintReader.next()
        logInfo(s"""raw command: ${command}""")
        val flintCommand = FlintCommand.deserialize(command)
        logInfo(s"""command: ${flintCommand}""")
        flintCommand.running()
        logInfo(s"""command running: ${flintCommand}""")
        update(flintCommand, flintUpdater, queryIndex)
        try {
          execute(flintCommand.query, spark, resultIndex)
          flintCommand.complete()
          logInfo(s"""command complete: ${flintCommand}""")
          update(flintCommand, flintUpdater, queryIndex)
        } catch {
          case e: Exception =>
            flintCommand.fail()
            logInfo(s"""command fail: ${flintCommand}""")
            update(flintCommand, flintUpdater, queryIndex)
        }
      }
      flintReader.close()
      Thread.sleep(1000)
    }
    flintUpdater.close()
  }

  /**
   * Create a new formatted dataframe with json result, json schema and EMR_STEP_ID.
   *
   * @param result
   *   sql query result dataframe
   * @param spark
   *   spark session
   * @return
   *   dataframe with result, schema and emr step id
   */
  def getFormattedData(result: DataFrame, spark: SparkSession): DataFrame = {
    // Create the schema dataframe
    val schemaRows = result.schema.fields.map { field =>
      Row(field.name, field.dataType.typeName)
    }
    val resultSchema = spark.createDataFrame(
      spark.sparkContext.parallelize(schemaRows),
      StructType(
        Seq(
          StructField("column_name", StringType, nullable = false),
          StructField("data_type", StringType, nullable = false))))

    // Define the data schema
    val schema = StructType(
      Seq(
        StructField("result", ArrayType(StringType, containsNull = true), nullable = true),
        StructField("schema", ArrayType(StringType, containsNull = true), nullable = true),
        StructField("stepId", StringType, nullable = true),
        StructField("applicationId", StringType, nullable = true)))

    // Create the data rows
    val rows = Seq(
      (
        result.toJSON.collect.toList.map(_.replaceAll("'", "\\\\'").replaceAll("\"", "'")),
        resultSchema.toJSON.collect.toList.map(_.replaceAll("\"", "'")),
        sys.env.getOrElse("SERVERLESS_EMR_JOB_ID", "unknown"),
        spark.sparkContext.applicationId))

    // Create the DataFrame for data
    spark.createDataFrame(rows).toDF(schema.fields.map(_.name): _*)
  }
}
