/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.app

import org.json4s.{Formats, NoTypeHints}
import org.json4s.native.JsonMethods.parse
import org.json4s.native.Serialization

class FlintInstance(val jobId: String, val sessionId: String, val state: String) {}

object FlintInstance {

  implicit val formats: Formats = Serialization.formats(NoTypeHints)

  def deserialize(job: String): FlintInstance = {
    val meta = parse(job)
    val state = (meta \ "state").extract[String]
    val jobId = (meta \ "jobId").extract[String]
    val sessionId = (meta \ "sessionId").extract[String]

    new FlintInstance(jobId, sessionId, state)
  }

  def serialize(job: FlintInstance): String = {
    Serialization.write(
      Map("jobId" -> job.jobId, "sessionId" -> job.sessionId, "state" -> job.state))
  }
}
