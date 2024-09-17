/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.snapshot

import org.opensearch.snapshot.utils.{SnapshotParams, SnapshotTableMetadata}

import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import org.apache.spark.sql.types.StructType

class SnapshotScanBuilder(
    schema: StructType,
    snapshotParams: SnapshotParams,
    snapshotTableMetadata: SnapshotTableMetadata)
    extends ScanBuilder {
  override def build(): Scan = new SnapshotScan(schema, snapshotParams, snapshotTableMetadata)
}
