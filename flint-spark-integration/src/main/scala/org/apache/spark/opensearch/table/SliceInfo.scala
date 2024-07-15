/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.opensearch.table

case class SliceInfo(shardId: Int, beginId: Int, iter: Int, pageSize: Int, pit: String)
