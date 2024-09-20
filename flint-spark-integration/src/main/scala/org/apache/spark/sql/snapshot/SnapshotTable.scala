/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.snapshot

import java.util

import org.opensearch.snapshot.utils.{SnapshotParams, SnapshotTableMetadata}

import org.apache.spark.sql.connector.catalog.{SupportsRead, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

case class SnapshotTable(
    override val schema: StructType,
    private val snapshotParams: SnapshotParams,
    private val snapshotTableMetadata: SnapshotTableMetadata)
    extends SupportsRead {

  override def name(): String = snapshotParams.getTableName

  override def capabilities(): util.Set[TableCapability] =
    util.EnumSet.of(TableCapability.BATCH_READ)

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new SnapshotScanBuilder(schema, snapshotParams, snapshotTableMetadata)
}
