/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.opensearch.table

import org.opensearch.flint.spark.mv.FlintSparkMaterializedView.getFlintIndexName
import org.scalatest.matchers.must.Matchers.defined
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class OpenSearchTableITSuite extends OpenSearchCatalogSuite {

  def multipleShardsIndex(indexName: String): Unit = {
    val twoShards = """{
                           |  "number_of_shards": "2",
                           |  "number_of_replicas": "0"
                           |}""".stripMargin

    val mappings = """{
                     |  "properties": {
                     |    "accountId": {
                     |      "type": "keyword"
                     |    },
                     |    "eventName": {
                     |      "type": "keyword"
                     |    },
                     |    "eventSource": {
                     |      "type": "keyword"
                     |    },
                     |    "time": {
                     |      "type": "date"
                     |    }
                     |  }
                     |}""".stripMargin
    val docs = Seq("""{
                     |  "accountId": "123",
                     |  "eventName": "event",
                     |  "eventSource": "source",
                     |  "time": "2015-01-01T12:10:30Z"
                     |}""".stripMargin)
    index(indexName, twoShards, mappings, docs)
  }

  test("Partition works correctly when indices include multiple shards") {
    val indexName1 = "t0001"
    withIndexName(indexName1) {
      multipleShardsIndex(indexName1)
      val df = spark.sql(s"""
        SELECT accountId, eventName, eventSource
        FROM ${catalogName}.default.`t0001`""")

      assert(df.rdd.getNumPartitions == 2)
    }
  }

  test("Partition works correctly when query wildcard index") {
    val indexName1 = "t0001"
    val indexName2 = "t0002"
    withIndexName(indexName1) {
      withIndexName(indexName2) {
        simpleIndex(indexName1)
        simpleIndex(indexName2)
        val df = spark.sql(s"""
        SELECT accountId, eventName, eventSource
        FROM ${catalogName}.default.`t0001,t0002`""")

        assert(df.rdd.getNumPartitions == 2)
      }
    }
  }

  val indexName = "mv-test"
  private val testTable = s"`$catalogName`.`default`.`$indexName`"
  private val testMvName = s"$catalogName.default.mv_test_metrics"
  private val testQuery =
    s"""
       | SELECT
       |   COUNT(*) AS count
       | FROM $testTable
       | GROUP BY TUMBLE(time, '10 Minutes')
       |""".stripMargin

  private val testFlintIndex = getFlintIndexName(testMvName)

  test("create materialized view with streaming job options") {

    withIndexName(indexName) {
      multipleShardsIndex(indexName)
      withTempDir { checkpointDir =>
        sql(s"""
               | CREATE MATERIALIZED VIEW $testMvName
               | AS $testQuery
               | WITH (
               |   auto_refresh = true,
               |   refresh_interval = '5 Seconds',
               |   checkpoint_location = '${checkpointDir.getAbsolutePath}',
               |   watermark_delay = '1 Second',
               |   output_mode = 'complete',
               |   index_settings = '{"number_of_shards": 3, "number_of_replicas": 2}',
               |   extra_options = '{"$catalogName.default.`mv-test`": {"start_timestamp": "1"}}'
               | )
               |""".stripMargin)

        val index = flint.describeIndex(testFlintIndex)
        index shouldBe defined

        val options = index.get.options
        options.extraSourceOptions(s"$catalogName.default.mv_test") shouldBe Map(
          "maxFilesPerTrigger" -> "1")
        options.extraSinkOptions() shouldBe Map.empty
      }
    }
  }
}
