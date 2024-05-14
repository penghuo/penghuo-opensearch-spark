/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.sessioncatalog

import org.opensearch.flint.spark.FlintSparkSuite

import org.apache.spark.SparkConf

trait FlintSessionCatalogSuit extends FlintSparkSuite {
  // Override catalog name
  override lazy protected val catalogName: String = "mycatalog"

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
      // Set Iceberg-specific Spark configurations
      .set("spark.sql.catalog.mycatalog", "org.opensearch.sql.FlintDelegatingSessionCatalog")
      .set("spark.sql.defaultCatalog", s"${catalogName}")
    conf
  }
}
