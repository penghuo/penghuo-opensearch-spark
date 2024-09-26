/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.snapshot

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{BigIntVector, ValueVector, VarCharVector}
import org.apache.lucene.index._
import org.apache.lucene.search.{Collector, IndexSearcher, LeafCollector, MatchAllDocsQuery, Query, Scorable, ScoreMode}
import org.apache.lucene.util.BytesRef
import org.opensearch.snapshot.utils.{SnapshotParams, SnapshotUtil}

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.snapshot.SnapshotPartitionColumnReader.{DocValueData, LongDocValueData, MatchAllIterator, SearchIterator, StringDocValueData}
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch, ColumnVector}

class SnapshotPartitionColumnReader(
    snapshotParams: SnapshotParams,
    schema: StructType,
    snapshotInputPartition: SnapshotInputPartition,
    query: Query,
    pushedSort: String,
    pushedLimit: Int,
    requiredSchema: StructType,
    batchSize: Int = 1024 * 100 // Specify batch size
) extends PartitionReader[ColumnarBatch]
    with Logging {

  private var indexReader: IndexReader = null
  private var indexSearcher: IndexSearcher = null

  private val docValues: Array[DocValueData] = new Array[DocValueData](requiredSchema.size)

  private var taskId = -1L

  private val allocator = new RootAllocator(Long.MaxValue)
//  private val allocator2 = new RootAllocator(Long.MaxValue)
  private val arrowVectors = new Array[ArrowColumnVector](requiredSchema.size)

  private var currentBatchSize = 0
  private var docIds: Iterator[Int] = null

  override def next(): Boolean = {
    if (indexSearcher == null) {
      taskId = TaskContext.get.taskAttemptId()

      val startTime = System.currentTimeMillis()
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
        if (field.dataType == DataTypes.StringType) {
          val vector = new VarCharVector(field.name, allocator)
          vector.allocateNew(batchSize * 5, batchSize)
          vector.allocateNew()
          docValues.update(
            i,
            StringDocValueData(
              DocValues.unwrapSingleton(DocValues.getSortedSet(leafReader, field.name)),
              vector))
          arrowVectors(i) = new ArrowColumnVector(vector)
        } else {
          val vector = new BigIntVector(field.name, allocator)
          vector.allocateNew(batchSize)
          docValues.update(
            i,
            LongDocValueData(
              DocValues.unwrapSingleton(DocValues.getSortedNumeric(leafReader, field.name)),
              vector))
          arrowVectors(i) = new ArrowColumnVector(vector)
        }
        i += 1
      }

      if (query.equals(new MatchAllDocsQuery)) {
        val maxDoc = indexReader.maxDoc()
        logInfo(s"TID-$taskId, Shard-${snapshotInputPartition.shardId} max doc: $maxDoc")
        docIds = MatchAllIterator(maxDoc)
      } else {
        val internalDocIds = SearchIterator()
        indexSearcher.search(
          query,
          new Collector {
            override def getLeafCollector(context: LeafReaderContext): LeafCollector =
              new LeafCollector {
                override def setScorer(scorer: Scorable): Unit = {}

                override def collect(doc: Int): Unit = {
                  internalDocIds.add(doc)
                }
              }
            override def scoreMode(): ScoreMode = ScoreMode.COMPLETE_NO_SCORES
          })
        docIds = internalDocIds
        logInfo(
          s"TID-$taskId, Shard-${snapshotInputPartition.shardId} total hits: ${internalDocIds.total()}")
      }

    }
    docIds.hasNext
  }

  override def get(): ColumnarBatch = {
    resetVectors()

    currentBatchSize = 0
    while (currentBatchSize < batchSize && docIds.hasNext) {
      val docId = docIds.next()
      for (docValue <- docValues) {
        docValue.row(docId, currentBatchSize)
      }
      currentBatchSize += 1
    }
    logInfo(s"TID-$taskId, Shard-${snapshotInputPartition.shardId} batch size: $currentBatchSize")
    for (vector <- arrowVectors) {
      vector.getValueVector.setValueCount(currentBatchSize)
    }
    new ColumnarBatch(arrowVectors.toArray[ColumnVector], currentBatchSize)
  }

  // Reset Arrow vectors before each batch
  private def resetVectors(): Unit = {
    logInfo(s"TID-$taskId, Shard-${snapshotInputPartition.shardId} resetVectors")
    for (vector <- arrowVectors) {
      vector.getValueVector.reset()
    }
  }

  override def close(): Unit = {
    if (indexReader != null) {
      logInfo(s"Closing ${snapshotInputPartition.shardId}")
      indexReader.close()
    }
    arrowVectors.foreach { vector =>
      if (vector != null) vector.close()
    }
    allocator.close()
  }
}

object SnapshotPartitionColumnReader extends Logging {
  trait DocValueData {
    def row(docId: Int, index: Int): Any

    def vector(): ValueVector
  }

  case class LongDocValueData(values: NumericDocValues, vector: BigIntVector)
      extends DocValueData {
    def row(docId: Int, index: Int): Any = {
      values.advance(docId)
      vector.set(index, values.longValue() * 1000)
    }
  }

  case class StringDocValueData(values: SortedDocValues, vector: VarCharVector)
      extends DocValueData {

    private val cache: mutable.Map[Int, BytesRef] = mutable.Map()

    def row(docId: Int, index: Int): Any = {
      values.advance(docId)

      vector.set(index, lookupOrd(values.ordValue()).bytes)
    }

    def lookupOrd(ord: Int): BytesRef = {
      cache.get(ord) match {
        case Some(cachedTerm) => cachedTerm // Return the cached term
        case None =>
          val term = values.lookupOrd(ord)
          val termCopy = BytesRef.deepCopyOf(term)
          cache(ord) = termCopy
          termCopy
      }
    }
  }

  case class MatchAllIterator(maxDoc: Int) extends Iterator[Int] {
    var docId = 0

    override def hasNext: Boolean = {
      docId < maxDoc
    }

    override def next(): Int = {
      val temp = docId
      docId += 1
      temp
    }
  }

  case class SearchIterator() extends Iterator[Int] {
    var buffer = ArrayBuffer.empty[Int]
    var iter: Iterator[Int] = null

    def total(): Int = buffer.length

    def add(docId: Int): Unit = {
      buffer.append(docId)
    }

    override def hasNext: Boolean = {
      if (iter == null) {
        iter = buffer.iterator
      }
      iter.hasNext
    }

    override def next(): Int = {
      iter.next()
    }
  }
}
