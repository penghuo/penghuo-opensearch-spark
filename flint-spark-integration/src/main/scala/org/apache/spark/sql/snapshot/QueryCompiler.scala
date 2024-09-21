/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.snapshot

import scala.io.Source

import org.apache.lucene.index.Term
import org.apache.lucene.queryparser.xml.builders.BooleanQueryBuilder
import org.apache.lucene.search.{BooleanClause, BooleanQuery, FieldExistsQuery, MatchAllDocsQuery, Query, TermQuery}
import org.opensearch.index.mapper.FieldNamesFieldMapper

import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.util.{DateTimeUtils, TimestampFormatter}
import org.apache.spark.sql.connector.expressions.{Expression, FieldReference, LiteralValue}
import org.apache.spark.sql.connector.expressions.filter.{And, Predicate}
import org.apache.spark.sql.flint.datatype.FlintDataType.STRICT_DATE_OPTIONAL_TIME_FORMATTER_WITH_NANOS
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

/**
 * Todo. find the right package.
 */
case class QueryCompiler(schema: StructType) {

  /**
   * Using AND to concat predicates. Todo. If spark spark.sql.ansi.enabled = true, more expression
   * defined in V2ExpressionBuilder could be pushed down.
   */
  def compile(predicates: Array[Predicate]): Query = {
    if (predicates.isEmpty) {
      return new MatchAllDocsQuery
    }
    visitPredicate(predicates.reduce(new And(_, _)))
  }

  /**
   * Predicate is defined in SPARK filters.scala. Todo.
   *   1. currently, we map spark contains to OpenSearch match query. Can we leverage more full
   *      text queries for text field. 2. configuration of expensive query.
   */
  def visitPredicate(p: Predicate): Query = {
    val name = p.name()
    name match {
      case "IS_NOT_NULL" => new MatchAllDocsQuery
//        new TermQuery(new Term(FieldNamesFieldMapper.NAME, visitFieldValue(p.children()(0))))
      case "AND" =>
        new BooleanQuery.Builder()
          .add(compile(p.children()(0)), BooleanClause.Occur.MUST)
          .add(compile(p.children()(1)), BooleanClause.Occur.MUST)
          .build()
      case "OR" =>
        new BooleanQuery.Builder()
          .add(compile(p.children()(0)), BooleanClause.Occur.SHOULD)
          .add(compile(p.children()(1)), BooleanClause.Occur.SHOULD)
          .build()
      case "=" =>
        new TermQuery(
          new Term(visitFieldValue(p.children()(0)), visitFieldValue(p.children()(1))))
      case _ => new MatchAllDocsQuery()
    }
  }

  def compile(expr: Expression): Query = {
    expr match {
      case p: Predicate => visitPredicate(p)
      case _ => throw new UnsupportedOperationException(s"Must be predicated, but got $expr")
    }
  }

  def extract(value: Any, dataType: DataType): String = dataType match {
    case TimestampType =>
      TimestampFormatter(
        STRICT_DATE_OPTIONAL_TIME_FORMATTER_WITH_NANOS,
        DateTimeUtils
          .getZoneId(SQLConf.get.sessionLocalTimeZone),
        false)
        .format(value.asInstanceOf[Long])
    case _ => Literal(value, dataType).toString()
  }

  def quote(f: ((Any, DataType) => String), quoteString: Boolean = false)(
      value: Any,
      dataType: DataType): String =
    dataType match {
      case DateType | TimestampType | StringType if quoteString =>
        s""""${f(value, dataType)}""""
      case _ => f(value, dataType)
    }

  def visitFieldValue(expr: Expression, quoteString: Boolean = false): String = {
    expr match {
      case LiteralValue(value, dataType) =>
        quote(extract, quoteString)(value, dataType)
      case f: FieldReference => f.toString()
      case _ => ""
    }
  }
}
