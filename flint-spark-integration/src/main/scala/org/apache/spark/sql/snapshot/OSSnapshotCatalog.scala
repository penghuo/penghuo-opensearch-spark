/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.snapshot

import java.util

import org.opensearch.snapshot.utils.{SnapshotParams, SnapshotUtil}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.analysis.{NoSuchTableException, TableAlreadyExistsException}
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class OSSnapshotCatalog extends CatalogPlugin with TableCatalog with Logging {
  private var cname: String = null
  private var snapshotName: String = null
  private var s3Bucket: String = null
  private var s3Region: String = null
  private var s3AccessKey: String = null
  private var s3SecretKey: String = null
  private var basePath: String = null

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    this.cname = name
    this.snapshotName = options.get("snapshot.name")
    this.s3Bucket = options.get("s3.bucket")
    this.s3Region = options.get("s3.region")
    this.s3AccessKey = options.get("s3.access.key")
    this.s3SecretKey = options.get("s3.secret.key")
    this.basePath = options.get("snapshot.base.path")
  }

  override def name: String = this.cname

  override def listTables(namespace: Array[String]): Array[Identifier] =
    Array.empty[Identifier]

  @throws[NoSuchTableException]
  override def loadTable(ident: Identifier): Table = {
    val snapshotParams = new SnapshotParams.Builder()
      .snapshotName(snapshotName)
      .s3Bucket(s3Bucket)
      .basePath(basePath)
      .s3Region(s3Region)
      .tableName(ident.name)
      .build
    try {
      logInfo(
        s"namespace: ${ident.namespace().mkString("Array(", ", ", ")")}, name: ${ident.name}")
      val s3BlobStore = SnapshotUtil.createS3BlobStore(snapshotParams)
      try {
        val snapshotTableMetadata =
          SnapshotUtil.getSnapshotTableMetadata(s3BlobStore, snapshotParams)
        val schema =
          SnapshotUtil.createSparkSchema(snapshotTableMetadata.getIndexMetadata.mapping)
        val snapshotTable = SnapshotTable(schema, snapshotParams, snapshotTableMetadata)
        logInfo(s"tableName: ${snapshotTable.name()}")
        snapshotTable
      } catch {
        case e: Exception =>
          throw new NoSuchTableException(ident)
      } finally if (s3BlobStore != null) s3BlobStore.close()
    }
  }

  override def invalidateTable(ident: Identifier): Unit = {

    // Implement cache invalidation if you're caching tables
  }

  @throws[TableAlreadyExistsException]
  override def createTable(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): Table = {
    // Implement logic to create a new table
    // This might involve creating a new snapshot or mapping an existing one
    throw new UnsupportedOperationException("Not implemented yet")
  }

  @throws[NoSuchTableException]
  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    // Implement logic to alter a table
    // This might be limited for snapshots, perhaps only allowing metadata changes
    throw new UnsupportedOperationException("Not implemented yet")
  }

  override def dropTable(ident: Identifier): Boolean = {
    // Implement logic to drop a table
    // This might involve deleting snapshot metadata, but probably not the actual snapshot
    throw new UnsupportedOperationException("Not implemented yet")
  }

  @throws[NoSuchTableException]
  @throws[TableAlreadyExistsException]
  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
    // Implement logic to rename a table
    // This might involve updating snapshot metadata
    throw new UnsupportedOperationException("Not implemented yet")
  }
}
