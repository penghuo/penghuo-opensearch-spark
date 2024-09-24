/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.snapshot

import java.io.IOException
import java.util
import java.util.{Iterator, NoSuchElementException}

import scala.collection.JavaConverters

import com.fasterxml.jackson.core.{JsonFactory, JsonParser}
import org.apache.lucene.document.{Document, InetAddressPoint}
import org.apache.lucene.index._
import org.apache.lucene.search._
import org.apache.lucene.util.BytesRef
import org.opensearch.common.network.InetAddresses
import org.opensearch.snapshot.utils.{SnapshotParams, SnapshotUtil}

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, FailureSafeParser}
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.flint.datatype.FlintDataType.DATE_FORMAT_PARAMETERS
import org.apache.spark.sql.flint.json.{FlintCreateJacksonParser, FlintJacksonParser, JSONOptions, JSONOptionsInRead}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
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
  private var scoreDocs: Iterator[Int] = null
  private val docValueIterators: util.Map[String, DocIdSetIterator] =
    new util.LinkedHashMap[String, DocIdSetIterator]

  private var fields: util.Map[String, String] = new util.HashMap[String, String]

  private var recordCnt = 0

  private var taskId = -1L

  private val parsedOptions: JSONOptions =
    new JSONOptionsInRead(
      CaseInsensitiveMap(DATE_FORMAT_PARAMETERS),
      SQLConf.get.sessionLocalTimeZone,
      "")
  private val parser: FlintJacksonParser =
    new FlintJacksonParser(schema, parsedOptions, allowArrayAsStructs = true)
  lazy val stringParser: (JsonFactory, String) => JsonParser =
    FlintCreateJacksonParser.string(_: JsonFactory, _: String)
  lazy val safeParser = new FailureSafeParser[String](
    input => parser.parse(input, stringParser, UTF8String.fromString),
    parser.options.parseMode,
    schema,
    parser.options.columnNameOfCorruptRecord)

  override def next(): Boolean = {
    if (scoreDocs == null) {
      taskId = TaskContext.get.taskAttemptId()

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
        s"TID-$taskId, Shard-${snapshotInputPartition.shardId} Time taken to init: ${endTime -
            startTime} ms")

      startTime = System.currentTimeMillis()
//      var sortField = new Sort(SortField.FIELD_DOC)
//      if (!pushedSort.isBlank && !pushedSort.isEmpty) {
//        sortField = new Sort(new SortedNumericSortField(pushedSort, Type.LONG, true))
//      }
//
//      val collectorManager =
//        new TopFieldCollectorManager(sortField, pushedLimit, 1)
//      scoreDocs = JavaConverters.asJavaIterator(
//        indexSearcher.search(query, collectorManager).scoreDocs.iterator)

      val docIds: util.LinkedList[Int] = new util.LinkedList[Int]
      indexSearcher.search(
        query,
        new Collector {
          override def getLeafCollector(context: LeafReaderContext): LeafCollector =
            new LeafCollector {
              override def setScorer(scorer: Scorable): Unit = {}

              override def collect(doc: Int): Unit = {
                docIds.add(doc)
              }
            }

          override def scoreMode(): ScoreMode = ScoreMode.COMPLETE_NO_SCORES
        })
      scoreDocs = docIds.iterator()

      val leafReader = indexReader.leaves.get(0).reader
      for (field <- requiredSchema.fields) {
        if (field.dataType eq DataTypes.StringType) {
          docValueIterators.put(
            field.name,
            DocValues.unwrapSingleton(DocValues.getSortedSet(leafReader, field.name)))
        } else {
          docValueIterators.put(
            field.name,
            DocValues.unwrapSingleton(DocValues.getSortedNumeric(leafReader, field.name)))
        }
      }

      endTime = System.currentTimeMillis()
      logInfo(
        s"TID-$taskId, Shard-${snapshotInputPartition.shardId} Time taken to " +
          s"search: ${endTime -
              startTime} ms")
    }
    scoreDocs.hasNext
  }

  var firstCall = false
  var startTime = System.currentTimeMillis()

  override def get(): InternalRow = {
    if (firstCall) {
      startTime = System.currentTimeMillis()
    }

    try {
      val docId = scoreDocs.next()
      val internalRow = new GenericInternalRow(requiredSchema.fields.length)
      var i = 0
      docValueIterators.forEach((name, iterator) => {
        iterator.advance(docId)
        if (iterator.isInstanceOf[SortedDocValues]) {
          val stringIterator = iterator.asInstanceOf[SortedDocValues]
          val bytesRef = stringIterator.lookupOrd(stringIterator.ordValue)
          internalRow.update(i, UTF8String.fromBytes(bytesRef.bytes))
        } else {
          internalRow.update(i, iterator.asInstanceOf[NumericDocValues].longValue)
        }
        i += 1
      })
      recordCnt += 1
      if (recordCnt == 1000000) {
        logInfo(
          s"TID-$taskId, Shard-${snapshotInputPartition.shardId} Time taken to process 1M " +
            s"record: ${System
                .currentTimeMillis() - startTime} ms")
        recordCnt = 0
        startTime = System.currentTimeMillis()
      }
      internalRow
    } catch {
      case e: IOException =>
        throw new NoSuchElementException(s"Failed to retrieve next document: ${e.getMessage}")
    }
  }

//  private def convertFieldToInternalRow(fields: util.Map[String, String]): InternalRow = {
//    val values = new Array[Any](requiredSchema.fields.length)
//    var i = 0
//    for (field <- requiredSchema.fields) {
//      values(i) = convertField(field.name, field.dataType, fields)
//      i += 1
//    }
//    InternalRow.fromSeq(values)
//  }

//  private def convertField(
//      dataType: DataType,
//      fields: util.Map[String, String]): Any = {
//    val value = fields.get(name)
//    dataType match {
//      case StringType => UTF8String.fromString(value)
//      case LongType => value.toLong
//      case IntegerType => value.toInt
//      case BooleanType => value.toBoolean
//      case TimestampType => value.toLong
//      case _ =>
//        throw new RuntimeException(s"unsupported field: ${name}, type: ${dataType}")
//    }
//  }

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

  private def parseIP(value: BytesRef) = {
    val bytes = util.Arrays.copyOfRange(value.bytes, value.offset, value.offset + value.length)
    val inet = InetAddressPoint.decode(bytes)
    InetAddresses.toAddrString(inet)
  }
}
