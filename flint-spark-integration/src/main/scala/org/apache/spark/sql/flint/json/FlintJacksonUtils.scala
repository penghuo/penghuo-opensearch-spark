/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.flint.json

import com.fasterxml.jackson.core.{JsonParser, JsonToken}

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{DataTypeMismatch, TypeCheckSuccess}
import org.apache.spark.sql.errors.QueryErrorsBase
import org.apache.spark.sql.types._

object FlintJacksonUtils extends QueryErrorsBase {

  /**
   * Advance the parser until a null or a specific token is found
   */
  def nextUntil(parser: JsonParser, stopOn: JsonToken): Boolean = {
    parser.nextToken() match {
      case null => false
      case x => x != stopOn
    }
  }

  def verifyType(name: String, dataType: DataType): TypeCheckResult = {
    dataType match {
      case NullType | _: AtomicType | CalendarIntervalType => TypeCheckSuccess

      case st: StructType =>
        st.foldLeft(TypeCheckSuccess: TypeCheckResult) { case (currResult, field) =>
          if (currResult.isFailure) currResult else verifyType(field.name, field.dataType)
        }

      case at: ArrayType => verifyType(name, at.elementType)

      // For MapType, its keys are treated as a string (i.e. calling `toString`) basically when
      // generating JSON, so we only care if the values are valid for JSON.
      case mt: MapType => verifyType(name, mt.valueType)

      case udt: UserDefinedType[_] => verifyType(name, udt.sqlType)

      case _ =>
        DataTypeMismatch(
          errorSubClass = "CANNOT_CONVERT_TO_JSON",
          messageParameters = Map("name" -> toSQLId(name), "type" -> toSQLType(dataType)))
    }
  }
}
