/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.snapshot

import java.io.IOException

import org.apache.lucene.index._
import org.apache.lucene.search._
import org.opensearch.snapshot.utils.{SnapshotParams, SnapshotUtil}

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.snapshot.SnapshotPartitionReader.{DocValueData, LongDocValueData, StringDocValueData}
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

  private val docValues: Array[DocValueData] = new Array[DocValueData](requiredSchema.size)

  private var taskId = -1L

  private var init = false

  var docId = 0
  var maxDoc: Int = -1

  override def next(): Boolean = {
    if (indexSearcher == null) {
      init = true

      taskId = TaskContext.get.taskAttemptId()

      var startTime = System.currentTimeMillis()
      indexReader = DirectoryReader.open(
        SnapshotUtil.getRemoteSnapShotDirectory(
          snapshotParams,
          snapshotInputPartition.snapshotUUID,
          snapshotInputPartition.indexId,
          snapshotInputPartition.shardId))
      indexSearcher = new IndexSearcher(indexReader)
      logInfo(
        s"TID-$taskId, Shard-${snapshotInputPartition.shardId} Time taken to init: ${System.currentTimeMillis() -
            startTime} ms")

      val leafReader = indexReader.leaves.get(0).reader
      var i = 0
      for (field <- requiredSchema.fields) {
        if (field.dataType eq DataTypes.StringType) {
          docValues.update(
            i,
            StringDocValueData(
              DocValues.unwrapSingleton(DocValues.getSortedSet(leafReader, field.name))))
        } else {
          docValues.update(
            i,
            LongDocValueData(
              DocValues.unwrapSingleton(DocValues.getSortedNumeric(leafReader, field.name))))
        }
        i += 1
      }

      maxDoc = indexReader.maxDoc()
      logInfo(s"TID-$taskId, Shard-${snapshotInputPartition.shardId} max doc: $maxDoc")
    }

    docId < maxDoc
  }

  override def get(): InternalRow = {
    try {
      val internalRow = new GenericInternalRow(requiredSchema.fields.length)
      var i = 0
      for (docValue <- docValues) {
        internalRow.update(i, docValue.row(docId))
        i += 1
      }
      docId += 1
      internalRow
    } catch {
      case e: IOException =>
        throw new NoSuchElementException(s"Failed to retrieve next document: ${e.getMessage}")
    }
  }

  override def close(): Unit = {
    if (indexReader != null) {
      logInfo(s"Close ${snapshotInputPartition.shardId}")
      indexReader.close()
    }
  }
}

object SnapshotPartitionReader {
  trait DocValueData {
    def row(docId: Int): Any
  }

  case class LongDocValueData(values: NumericDocValues) extends DocValueData {
    def row(docId: Int): Any = {
      values.advance(docId)
      values.longValue()
    }
  }

  case class StringDocValueData(values: SortedDocValues) extends DocValueData {
    def row(docId: Int): Any = {
      values.advance(docId)
      UTF8String.fromBytes(values.lookupOrd(values.ordValue()).bytes)
    }
  }
}
