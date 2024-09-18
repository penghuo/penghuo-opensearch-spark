/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.opensearch.snapshot

class OSSnapshotITSuite extends OSSnapshotCatalogITSuite {

  test("Sanity") {
    val tableName: String = "`s3snapshot`.`default`.`quickwit-generated-logs-v1_001`"
    val startTime = System.currentTimeMillis()
    spark
      .sql(s"SELECT * FROM $tableName limit 10")
      .show()
    val endTime = System.currentTimeMillis()

    println(s"Time taken: ${endTime - startTime} ms")
  }

  /**
   * { "track_total_hits": false, "size" : 100, "query": { "term": { "message": { "value": "queen"
   * } } }, "sort": [ { "timestamp": {"order": "desc"} } ] }
   */
  test("Term") {
    val tableName: String = "`s3snapshot`.`default`.`quickwit-generated-logs-v1_001`"
    val startTime = System.currentTimeMillis()
    spark
      .sql(s"SELECT * FROM $tableName WHERE message = 'queen' ORDER BY timestamp DESC limit 100")
      .show()
    val endTime = System.currentTimeMillis()

    println(s"Time taken: ${endTime - startTime} ms")
  }
}
