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

  val queryIndex = "datasource_repl_index"

  def execute(query: String, spark: SparkSession, resultIndex: String, queryId: String): Unit = {
    val result: DataFrame = spark.sql(query)

    // Get Data
    val data = getFormattedData(result, spark, queryId)

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
    updater.update(flintCommand.queryId, serialize(flintCommand))
  }

  def main(args: Array[String]) {
    val Array(query, resultIndex, wait, sessionId) = args;

    // init SparkContext
    val conf: SparkConf = new SparkConf()
      .setAppName("FlintJob")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    try {
      if (wait.equalsIgnoreCase("wait")) {
        logInfo(s"""streaming query ${query}""")
        execute(query, spark, resultIndex, sys.env.getOrElse("SERVERLESS_EMR_JOB_ID", "unknown"))
        spark.streams.awaitAnyTermination()
      } else {
        val flintClient = FlintClientBuilder.build(FlintSparkConf().flintOptions())
        val dsl = """{
                    |  "bool": {
                    |    "must": [
                    |      {
                    |        "term": {
                    |          "state": "PENDING"
                    |        }
                    |      },
                    |      {
                    |        "term": {
                    |          "type": "request"
                    |        }
                    |      }
                    |    ]
                    |  }
                    |}""".stripMargin
        val flintUpdater = flintClient.createUpdater(queryIndex)

        val jobId = sys.env.getOrElse("SERVERLESS_EMR_JOB_ID", "unknown");
        val flintJob = new FlintInstance(jobId, sessionId, "RUNNING")
        flintUpdater.update(flintJob.sessionId, FlintInstance.serialize(flintJob))
        logInfo(
          s"""update job {"jobid": ${flintJob.jobId}, "sessionId": ${flintJob.sessionId}} from ${queryIndex}""".stripMargin)

        while (true) {
          logInfo(s"""read from ${queryIndex}""")
          val flintReader = flintClient.createReader(queryIndex, dsl, "submitTime")
          while (flintReader.hasNext) {
            val command = flintReader.next()
            logInfo(s"""raw command: ${command}""")
            val flintCommand = FlintCommand.deserialize(command)
            logInfo(s"""command: ${flintCommand}""")
            flintCommand.running()
            logInfo(s"""command running: ${flintCommand}""")
            update(flintCommand, flintUpdater, queryIndex)
            try {
              execute(flintCommand.query, spark, resultIndex, flintCommand.queryId)
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
          Thread.sleep(100)
        }
        flintUpdater.close()
      }
    } finally {
      spark.stop()
    }

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
  def getFormattedData(result: DataFrame, spark: SparkSession, queryId: String): DataFrame = {
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
        queryId,
        spark.sparkContext.applicationId))

    // Create the DataFrame for data
    spark.createDataFrame(rows).toDF(schema.fields.map(_.name): _*)
  }
}
