/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.app

import org.json4s.{Formats, NoTypeHints}
import org.json4s.native.JsonMethods.parse
import org.json4s.native.Serialization

class FlintCommand(var state: String, val query: String, val queryId: String) {
  def running(): Unit = {
    state = "RUNNING"
  }

  def complete(): Unit = {
    state = "SUCCESS"
  }

  def fail(): Unit = {
    state = "FAILED"
  }
}

object FlintCommand {

  implicit val formats: Formats = Serialization.formats(NoTypeHints)

  def deserialize(command: String): FlintCommand = {
    val meta = parse(command)
    val state = (meta \ "state").extract[String]
    val query = (meta \ "query").extract[String]
    val queryId = (meta \ "queryId").extract[String]

    new FlintCommand(state, query, queryId)
  }

  def serialize(flintCommand: FlintCommand): String = {
    Serialization.write(
      Map(
        "state" -> flintCommand.state,
        "query" -> flintCommand.query,
        "queryId" -> flintCommand.queryId))
  }
}
