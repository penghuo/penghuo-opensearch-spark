/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.opensearch.snapshot

class OSSnapshotITSuite extends OSSnapshotCatalogITSuite {
  test("Basic") {
    val tableName: String = "`s3snapshot`.`default`.`quickwit-generated-logs-v1_001`"
    spark.sql("SELECT * FROM " + tableName + " limit 10").show()
  }
}
