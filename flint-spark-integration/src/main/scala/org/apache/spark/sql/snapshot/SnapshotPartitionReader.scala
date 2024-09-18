/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.snapshot

import java.io.IOException
import java.util.{Iterator, NoSuchElementException}

import scala.collection.JavaConverters

import org.apache.lucene.document.Document
import org.apache.lucene.index.{DirectoryReader, IndexReader}
import org.apache.lucene.search.{IndexSearcher, MatchAllDocsQuery, Query, ScoreDoc}
import org.opensearch.snapshot.utils.{SnapshotParams, SnapshotUtil}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.json.{CreateJacksonParser, JacksonParser, JSONOptions, JSONOptionsInRead}
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, FailureSafeParser}
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

class SnapshotPartitionReader(
    snapshotParams: SnapshotParams,
    schema: StructType,
    snapshotInputPartition: SnapshotInputPartition,
    query: Query)
    extends PartitionReader[InternalRow]
    with Logging {
  private val indexReader: IndexReader = DirectoryReader.open(
    SnapshotUtil.getRemoteSnapShotDirectory(
      snapshotParams,
      snapshotInputPartition.snapshotUUID,
      snapshotInputPartition.indexId,
      snapshotInputPartition.shardId))
  private val indexSearcher: IndexSearcher = new IndexSearcher(indexReader)
  private var scoreDocs: Iterator[ScoreDoc] = null

  private val conf: CaseInsensitiveMap[String] = CaseInsensitiveMap(Map.empty[String, String])
  private val parsedOptions: JSONOptions =
    new JSONOptionsInRead(conf, SQLConf.get.sessionLocalTimeZone, "")
  private val parser: JacksonParser =
    new JacksonParser(schema, parsedOptions, true, Seq.empty[Filter])

  private val safeParser: FailureSafeParser[String] = new FailureSafeParser[String](
    input => parser.parse(input, CreateJacksonParser.string, UTF8String.fromString),
    parsedOptions.parseMode,
    schema,
    parsedOptions.columnNameOfCorruptRecord)

  override def next(): Boolean = {
    if (scoreDocs == null) {
      val startTime = System.currentTimeMillis()
      scoreDocs = JavaConverters.asJavaIterator(
        indexSearcher
          .search(query, 10)
          .scoreDocs
          .iterator)
      val endTime = System.currentTimeMillis()
      logInfo(s"Time taken to search: ${endTime - startTime} ms")
    }
    scoreDocs.hasNext
  }

  override def get(): InternalRow = {
    try {
      val startTime = System.currentTimeMillis()
      val scoreDoc = scoreDocs.next()
      val doc = indexSearcher.doc(scoreDoc.doc)
      val row = convertToInternalRow(doc)
      val endTime = System.currentTimeMillis()
      logInfo(s"Time taken to convert to internal row: ${endTime - startTime} ms")
      row
    } catch {
      case e: IOException =>
        throw new NoSuchElementException(s"Failed to retrieve next document: ${e.getMessage}")
    }
  }

  private def convertToInternalRow(doc: Document): InternalRow = {
    val sourceBytes = doc.getBinaryValue("_source")
    val results = safeParser.parse(sourceBytes.utf8ToString())
    results.next()
  }

  override def close(): Unit = {
    if (indexReader != null) {
      logInfo(s"Close ${snapshotInputPartition.shardId}")
      indexReader.close()
    }
  }
}
