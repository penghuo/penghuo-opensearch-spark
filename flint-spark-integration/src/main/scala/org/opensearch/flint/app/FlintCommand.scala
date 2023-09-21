/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.app

import org.json4s.{Formats, NoTypeHints}
import org.json4s.native.JsonMethods.parse
import org.json4s.native.Serialization

class FlintCommand(var status: String, val query: String, val id: String) {
  def running(): Unit = {
    status = "running"
  }

  def complete(): Unit = {
    status = "complete"
  }

  def fail(): Unit = {
    status = "fail"
  }
}

object FlintCommand {

  implicit val formats: Formats = Serialization.formats(NoTypeHints)

  def deserialize(command: String): FlintCommand = {
    val meta = parse(command)
    val status = (meta \ "status").extract[String]
    val query = (meta \ "query").extract[String]
    val id = (meta \ "id").extract[String]

    new FlintCommand(status, query, id)
  }

  def serialize(flintCommand: FlintCommand): String = {
    Serialization.write(
      Map(
        "status" -> flintCommand.status,
        "query" -> flintCommand.query,
        "id" -> flintCommand.id))
  }
}
