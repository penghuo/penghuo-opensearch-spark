/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.opensearch.catalog

import org.opensearch.flint.OpenSearchSuite

import org.apache.spark.FlintSuite
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.streaming.StreamTest

class OpenSearchCatalogITSuite
    extends QueryTest
    with StreamTest
    with FlintSuite
    with OpenSearchSuite {

  private val catalogName = "dev"

  override def beforeAll(): Unit = {
    super.beforeAll()

    spark.conf.set(
      s"spark.sql.catalog.${catalogName}",
      "org.apache.spark.opensearch.catalog.OpenSearchCatalog")
    spark.conf.set(s"spark.sql.catalog.${catalogName}.port", s"$openSearchPort")
    spark.conf.set(s"spark.sql.catalog.${catalogName}.host", openSearchHost)
  }

  test("Load single index as table") {
    val indexName = "t0001"
    withIndexName(indexName) {
      simpleIndex(indexName)
      val df = spark.sql(s"""
        SELECT accountId, eventName, eventSource
        FROM ${catalogName}.default.${indexName}""")

      assert(df.count() == 1)
      checkAnswer(df, Row("123", "event", "source"))
    }
  }
}
