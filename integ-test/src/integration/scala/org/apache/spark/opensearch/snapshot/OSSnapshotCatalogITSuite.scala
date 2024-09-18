/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.opensearch.snapshot

import org.opensearch.flint.spark.FlintSparkSuite
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider

class OSSnapshotCatalogITSuite extends FlintSparkSuite {
  override lazy val catalogName = "s3snapshot"

  override def beforeAll(): Unit = {
    super.beforeAll()

//    val credentialsProvider = DefaultCredentialsProvider.create()
//    val credentials = credentialsProvider.resolveCredentials()

    spark.conf.set(
      "spark.sql.catalog.s3snapshot",
      "org.apache.spark.sql.snapshot.OSSnapshotCatalog")
    spark.conf.set("spark.sql.catalog.s3snapshot.s3.bucket", "flint-data-dp-us-west-2-beta")
    spark.conf.set("spark.sql.catalog.s3snapshot.s3.region", "us-west-2")
//    spark.conf.set("spark.sql.catalog.s3snapshot.s3.access.key", credentials.accessKeyId())
//    spark.conf.set("spark.sql.catalog.s3snapshot.s3.secret.key", credentials.secretAccessKey())
    spark.conf.set("spark.sql.catalog.s3snapshot.snapshot.name", "s001")
    spark.conf.set(
      "spark.sql.catalog.s3snapshot.snapshot.base.path",
      "data/quickwit/generated-logs-v1/213_snapshot_001")
  }
}
