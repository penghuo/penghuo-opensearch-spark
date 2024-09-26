/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.snapshot

import java.io.IOException
import java.util.{Iterator, NoSuchElementException}

import scala.collection.JavaConverters

import com.fasterxml.jackson.core.{JsonFactory, JsonParser}
import org.apache.lucene.document.Document
import org.apache.lucene.index.{DirectoryReader, IndexReader}
import org.apache.lucene.search._
import org.apache.lucene.search.SortField.Type
import org.opensearch.snapshot.utils.{SnapshotParams, SnapshotUtil}

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, FailureSafeParser}
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.flint.datatype.FlintDataType.DATE_FORMAT_PARAMETERS
import org.apache.spark.sql.flint.json.{FlintCreateJacksonParser, FlintJacksonParser, JSONOptions, JSONOptionsInRead}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

class SnapshotPartitionReader(
    snapshotParams: SnapshotParams,
    schema: StructType,
    snapshotInputPartition: SnapshotInputPartition,
    query: Query,
    pushedSort: String,
    pushedLimit: Int,
    requiredSchema: StructType)
    extends PartitionReader[InternalRow]
    with Logging {

  private var indexReader: IndexReader = null
  private var indexSearcher: IndexSearcher = null
  private var scoreDocs: Iterator[ScoreDoc] = null

  private var taskId = TaskContext.get.taskAttemptId()

  private val parsedOptions: JSONOptions =
    new JSONOptionsInRead(
      CaseInsensitiveMap(DATE_FORMAT_PARAMETERS),
      SQLConf.get.sessionLocalTimeZone,
      "")
  private val parser: FlintJacksonParser =
    new FlintJacksonParser(requiredSchema, parsedOptions, allowArrayAsStructs = true)
  lazy val stringParser: (JsonFactory, String) => JsonParser =
    FlintCreateJacksonParser.string(_: JsonFactory, _: String)
  lazy val safeParser = new FailureSafeParser[String](
    input => parser.parse(input, stringParser, UTF8String.fromString),
    parser.options.parseMode,
    requiredSchema,
    parser.options.columnNameOfCorruptRecord)

  override def next(): Boolean = {
    if (scoreDocs == null) {
      var startTime = System.currentTimeMillis()
      indexReader = DirectoryReader.open(
        SnapshotUtil.getRemoteSnapShotDirectory(
          snapshotParams,
          snapshotInputPartition.snapshotUUID,
          snapshotInputPartition.indexId,
          snapshotInputPartition.shardId))
      indexSearcher = new IndexSearcher(indexReader)
      var endTime = System.currentTimeMillis()
      logInfo(
        s"TID-$taskId, Shard-${snapshotInputPartition.shardId} Time taken to init: ${endTime - startTime} ms")

      startTime = System.currentTimeMillis()
      var sortField = new Sort(SortField.FIELD_DOC)
      if (!pushedSort.isBlank && !pushedSort.isEmpty) {
        // FIXME, assume is timestamp
        sortField = new Sort(new SortedNumericSortField(pushedSort, Type.LONG, true))
      }

      val collectorManager =
        new TopFieldCollectorManager(sortField, pushedLimit, 1)
      scoreDocs = JavaConverters.asJavaIterator(
        indexSearcher.search(query, collectorManager).scoreDocs.iterator)
      endTime = System.currentTimeMillis()
      logInfo(
        s"TID-$taskId, Shard-${snapshotInputPartition.shardId} ime taken to search: ${endTime - startTime} ms")
    }
    scoreDocs.hasNext
  }

  override def get(): InternalRow = {
    try {
      val scoreDoc = scoreDocs.next()
      val doc = indexSearcher.doc(scoreDoc.doc)
      val row = convertToInternalRow(doc)
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
