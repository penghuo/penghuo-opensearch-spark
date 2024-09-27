/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.snapshot

import org.apache.spark.sql.connector.read.InputPartition

case class SnapshotInputPartition(
    val snapshotUUID: String,
    val indexId: String,
    val indexName: String,
    val shardId: String,
    override val preferredLocations: Array[String])
    extends InputPartition {}
