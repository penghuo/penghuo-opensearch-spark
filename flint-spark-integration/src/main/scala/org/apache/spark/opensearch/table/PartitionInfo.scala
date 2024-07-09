/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.opensearch.table

import org.json4s.{Formats, NoTypeHints}
import org.json4s.jackson.JsonMethods
import org.json4s.native.Serialization
import org.opensearch.flint.core.{FlintClientBuilder, FlintOptions}
import org.opensearch.flint.core.storage.stats.IndexStatsInfo

import org.apache.spark.internal.Logging

/**
 * Represents information about a partition in OpenSearch. Partition is backed by OpenSearch
 * Index. Each partition contain a list of Shards
 *
 * @param partitionName
 *   partition name.
 * @param shards
 *   shards.
 */
case class PartitionInfo(partitionName: String, pit: String, sliceInfo: Array[SliceInfo]) {}

object PartitionInfo extends Logging {
  implicit val formats: Formats = Serialization.formats(NoTypeHints)

  /**
   * Creates a PartitionInfo instance.
   *
   * @param partitionName
   *   The name of the partition.
   * @param settings
   *   The settings of the partition.
   * @return
   *   An instance of PartitionInfo.
   */
//  def apply(partitionName: String, settings: String): PartitionInfo = {
//    val shards =
//      Range.apply(0, numberOfShards(settings)).map(id => ShardInfo(partitionName, id)).toArray
//    PartitionInfo(partitionName, shards)
//  }

//  def apply(
//      partitionName: String,
//      settings: String,
//      stats: IndexStatsInfo,
//      options: FlintOptions): PartitionInfo = {
//    val totalSizeBytes = stats.sizeInBytes
//    val docSize = Math.ceil(totalSizeBytes / stats.docCount).toLong
//    val maxSplitSizeBytes = 10 * 1024 * 1024
//    val shardCount = numberOfShards(settings)
//    var splitCount = shardCount
//    val maxResult = maxResultWindow(settings)
//
//    // Adjust split count to ensure split_size < 10MB and search_size < 10000
//    while ((totalSizeBytes / splitCount > maxSplitSizeBytes) || ((totalSizeBytes / splitCount) / docSize > maxResult)) {
//      splitCount += shardCount
//    }
//
//    val maxSlice = splitCount
//    val splitSize = totalSizeBytes / splitCount
//    val pageSize = Math.ceil(splitSize / docSize).toInt
//
//    val sliceInfo =
//      Range.apply(0, maxSlice).map(sliceId => SliceInfo(sliceId, maxSlice, pageSize)).toArray
//
//    val pit = FlintClientBuilder.build(options).createPit(partitionName)
//    logInfo(s"index $partitionName create pit:$pit")
//    PartitionInfo(partitionName, pit, sliceInfo)
//  }

  // PIT with search_after
  def apply(
      partitionName: String,
      settings: String,
      stats: IndexStatsInfo,
      options: FlintOptions): PartitionInfo = {
    val totalSizeBytes = stats.sizeInBytes
    val docSize = Math.ceil(totalSizeBytes / stats.docCount).toLong
    val maxSplitSizeBytes = 10 * 1024 * 1024
    val maxResult = maxResultWindow(settings)
    val pageSize = Math.min(maxSplitSizeBytes / docSize, maxResult).toInt

    val pit = FlintClientBuilder.build(options).createPit(partitionName)
    logInfo(s"docSize: $docSize, pageSize: $pageSize")
    logInfo(s"index $partitionName create pit:$pit")
    PartitionInfo(partitionName, pit, Array(SliceInfo(-1, -1, pageSize)))
  }

  /**
   * Extracts the number of shards from the settings string.
   *
   * @param settingStr
   *   The settings string.
   * @return
   *   The number of shards.
   */
  def numberOfShards(settingStr: String): Int = {
    val setting = JsonMethods.parse(settingStr)
    (setting \ "index.number_of_shards").extract[String].toInt
  }

  def maxResultWindow(settingStr: String): Int = {
    val setting = JsonMethods.parse(settingStr)
    (setting \ "index.max_result_window").extractOrElse[String]("10000").toInt
  }

}
