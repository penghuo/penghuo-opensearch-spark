/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark

import java.io.IOException

import org.opensearch.action.search.SearchRequest
import org.opensearch.action.support.WriteRequest
import org.opensearch.action.update.UpdateRequest
import org.opensearch.client.RequestOptions
import org.opensearch.common.xcontent.XContentType
import org.opensearch.flint.OpenSearchSuite
import org.opensearch.flint.app.FlintCommand
import org.opensearch.index.query.QueryBuilders
import org.opensearch.search.builder.SearchSourceBuilder

import org.apache.spark.sql.{DataFrame, ExplainSuiteHelper, QueryTest, SparkSession}
import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.sql.types._

class ConsistencyITSuite
    extends QueryTest
    with StreamTest
    with FlintSuite
    with OpenSearchSuite
    with ExplainSuiteHelper {

  import testImplicits._

  val requestIndex = ".query_execution_request_mys3"
  val resultIndex = "query_execution_result_mys3"
  val N = 50

  test("strong consistency - read after write") {
    val options = openSearchOptions

    for (iter <- 1 to N) {
      logInfo(s">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
      logInfo(s"iteration: ${iter}")
      val queryId = s"${iter}"

      val command =
        new FlintCommand("running", "query", queryId, queryId, 1700090926955L, Some(""))
      logInfo(s"$queryId upsert command = success")
      upsert(command.statementId, FlintCommand.serialize(command))
      logInfo(s"$queryId upsert command = success done")

      Thread.sleep(3000)
      getFormattedData(spark, queryId).write
        .format("flint")
        .options(options)
        .mode("append")
        .save(resultIndex)

      Thread.sleep(2000)
      command.complete()
      logInfo(s"$queryId update command = success")
      update(command.statementId, FlintCommand.serialize(command))
      logInfo(s"$queryId update command = success done")
    }
  }

  def getFormattedData(spark: SparkSession, queryId: String): DataFrame = {
    val schema = StructType(
      Seq(
        StructField("result", ArrayType(StringType, containsNull = true), nullable = true),
        StructField("schema", ArrayType(StringType, containsNull = true), nullable = true),
        StructField("jobRunId", StringType, nullable = true),
        StructField("applicationId", StringType, nullable = true),
        StructField("dataSourceName", StringType, nullable = true),
        StructField("status", StringType, nullable = true),
        StructField("error", StringType, nullable = true),
        StructField("queryId", StringType, nullable = true),
        StructField("queryText", StringType, nullable = true),
        StructField("sessionId", StringType, nullable = true),
        // number is not nullable
        StructField("updateTime", LongType, nullable = false),
        StructField("queryRunTime", LongType, nullable = true)))

    // Create the data rows
    val rows = Seq(
      (
        Array(
          "{'@timestamp':'1998-06-10T13:41:26.000Z','clientip':'87.175.1.0','request':'GET " +
            "/images/backg4.gif HTTP/1.0','status':304,'size':0,'year':1998,'month':6,'day':10}"),
        Array(
          "{'column_name':'Letter','data_type':'string'}",
          "{'column_name':'Number','data_type':'integer'}"),
        "jobId",
        "appId",
        "dataSource",
        "SUCCESS",
        "",
        queryId,
        "query",
        "sessionId",
        1700090926955L,
        1700090926955L))

    // Create the DataFrame for data
    spark.createDataFrame(rows).toDF(schema.fields.map(_.name): _*)
  }

  def upsert(id: String, doc: String): Unit = { // we might need to keep the updater for a long time. Reusing the client may not work as the temporary
    try {
      val updateRequest = new UpdateRequest(requestIndex, id)
        .doc(doc, XContentType.JSON)
        .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL)
        .docAsUpsert(true)
      openSearchClient.update(updateRequest, RequestOptions.DEFAULT)
    } catch {
      case e: IOException =>
        throw new RuntimeException(
          String.format(
            "Failed to execute update request on index: %s, id: %s",
            requestIndex,
            id),
          e)
    }
  }

  def update(id: String, doc: String): Unit = {
    try {
      try {
        val updateRequest = new UpdateRequest(requestIndex, id)
          .doc(doc, XContentType.JSON)
          .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL)
        openSearchClient.update(updateRequest, RequestOptions.DEFAULT)
      } catch {
        case e: IOException =>
          throw new RuntimeException(
            String.format(
              "Failed to execute update request on index: %s, id: %s",
              requestIndex,
              id),
            e)
      }
    }
  }

  test("strong consistency - read after write5") {
    for (iter <- 1 to 20) {
      val indexName = "t0004"
      val mappings =
        """{
          |  "properties": {
          |    "aInt": {
          |      "type": "integer"
          |    }
          |  }
          |}""".stripMargin
      val options = openSearchOptions
      withIndexName(indexName) {
        logInfo(s"iteration: ${iter}")
        index(indexName, oneNodeSetting, mappings, Seq.empty)
        val data = Seq(iter)
        data
          .toDF("aInt")
          .coalesce(1)
          .write
          .format("flint")
          .options(options)
          .mode("append")
          .save(indexName)

        val searchRequest = new SearchRequest
        searchRequest.indices(indexName)
        val searchSourceBuilder = new SearchSourceBuilder
        val query = QueryBuilders.termQuery("aInt", iter)
        searchSourceBuilder.query(query)
        searchRequest.source(searchSourceBuilder)

        val response = openSearchClient.search(searchRequest, RequestOptions.DEFAULT)
        logInfo(s"response: ${response}")
        assert(response.getHits.getTotalHits.value == 1)
      }
    }
  }
}
