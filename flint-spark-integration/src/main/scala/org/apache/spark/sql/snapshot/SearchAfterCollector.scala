/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.snapshot

import scala.collection.mutable.ArrayBuffer

import org.apache.lucene.index.{DocValues, LeafReaderContext, SortedSetDocValues}
import org.apache.lucene.search.{Collector, LeafCollector, Scorable, ScoreMode}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types.{LongType, StringType, StructType, TimestampType}

case class SearchAfterCollector(requiredSchema: StructType, rows: ArrayBuffer[InternalRow])
    extends Collector {

  override def getLeafCollector(context: LeafReaderContext): LeafCollector = {
    new LeafCollector {
      override def setScorer(scorer: Scorable): Unit = {}

      override def collect(doc: Int): Unit = {
        var i = 0
        val internalRow = new GenericInternalRow(requiredSchema.fields.length)
        for (field <- requiredSchema.fields) {
          field.dataType match {
            case StringType =>
              val dvStr = DocValues.getSortedSet(context.reader(), field.name)
              var ord = SortedSetDocValues.NO_MORE_ORDS
              if (dvStr.advanceExact(doc) || (ord =
                  dvStr.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
                internalRow.update(i, dvStr.lookupOrd(ord))
              }
            case LongType | TimestampType =>
              val dvLong = context.reader().getSortedNumericDocValues(field.name)
              if (dvLong.advanceExact(doc)) {
                internalRow.update(i, dvLong.nextValue())
              }
          }
          i += 1
        }
        rows.append(internalRow)
      }
    }
  }
  override def scoreMode(): ScoreMode = ScoreMode.COMPLETE_NO_SCORES
}
