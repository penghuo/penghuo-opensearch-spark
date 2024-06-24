/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.opensearch.catalog

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.analysis.{NoSuchNamespaceException, NoSuchTableException}
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.flint.FlintTable
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * A Spark TableCatalog implementation of OpenSearch.
 */
class OpenSearchCatalog extends CatalogPlugin with TableCatalog with Logging {

  private val DEFAULT_NAMESPACE: Array[String] = Array("default")
  private var catalogName: String = _
  private var options: CaseInsensitiveStringMap = _

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    this.catalogName = name
    this.options = options
  }

  override def name(): String = catalogName

  @throws[NoSuchNamespaceException]
  override def listTables(namespace: Array[String]): Array[Identifier] = Array.empty

  @throws[NoSuchTableException]
  override def loadTable(ident: Identifier): Table = {
    logInfo(s"Loading table ${ident.name()}")
    if (!ident.namespace().exists(_.equalsIgnoreCase(DEFAULT_NAMESPACE(0)))) {
      throw new NoSuchTableException(ident.namespace().mkString("."), ident.name())
    }

    val conf = new java.util.HashMap[String, String](options.asCaseSensitiveMap())
    conf.put("path", ident.name())

    FlintTable(conf, Option.empty)
  }

  override def createTable(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: java.util.Map[String, String]): Table = {
    throw new UnsupportedOperationException("OpenSearchCatalog does not support createTable")
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    throw new UnsupportedOperationException("OpenSearchCatalog does not support alterTable")
  }

  override def dropTable(ident: Identifier): Boolean = {
    throw new UnsupportedOperationException("OpenSearchCatalog does not support dropTable")
  }

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
    throw new UnsupportedOperationException("OpenSearchCatalog does not support renameTable")
  }
}
