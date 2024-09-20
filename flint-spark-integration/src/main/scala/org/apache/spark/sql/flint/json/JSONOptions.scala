/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.flint.json

import java.nio.charset.{Charset, StandardCharsets}

import org.apache.spark.sql.catalyst.util._

private[sql] class JSONOptionsInRead(
    @transient override val parameters: CaseInsensitiveMap[String],
    defaultTimeZoneId: String,
    defaultColumnNameOfCorruptRecord: String)
    extends JSONOptions(parameters, defaultTimeZoneId, defaultColumnNameOfCorruptRecord) {

  def this(
      parameters: Map[String, String],
      defaultTimeZoneId: String,
      defaultColumnNameOfCorruptRecord: String = "") = {
    this(CaseInsensitiveMap(parameters), defaultTimeZoneId, defaultColumnNameOfCorruptRecord)
  }

  protected override def checkedEncoding(enc: String): String = {
    val isDenied = JSONOptionsInRead.denyList.contains(Charset.forName(enc))
    require(
      multiLine || !isDenied,
      s"""The $enc encoding must not be included in the denyList when multiLine is disabled:
         |denylist: ${JSONOptionsInRead.denyList.mkString(", ")}""".stripMargin)

    val isLineSepRequired =
      multiLine || Charset.forName(enc) == StandardCharsets.UTF_8 || lineSeparator.nonEmpty
    require(isLineSepRequired, s"The lineSep option must be specified for the $enc encoding")

    enc
  }
}



private[sql] object JSONOptionsInRead {
  // The following encodings are not supported in per-line mode (multiline is false)
  // because they cause some problems in reading files with BOM which is supposed to
  // present in the files with such encodings. After splitting input files by lines,
  // only the first lines will have the BOM which leads to impossibility for reading
  // the rest lines. Besides of that, the lineSep option must have the BOM in such
  // encodings which can never present between lines.
  val denyList = Seq(Charset.forName("UTF-16"), Charset.forName("UTF-32"))
}
