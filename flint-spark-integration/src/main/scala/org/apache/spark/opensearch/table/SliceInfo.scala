/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.opensearch.table

case class SliceInfo(sliceId: Int, maxSlice: Int, pageSize: Int)
