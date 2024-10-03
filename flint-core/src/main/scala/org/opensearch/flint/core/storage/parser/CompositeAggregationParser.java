/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage.parser;

import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CompositeAggregationParser {

  public List<Map<String, Object>> parse(Aggregations aggregations) {
    return ((CompositeAggregation) aggregations.asList().get(0))
        .getBuckets().stream().map(this::parse).collect(Collectors.toList());
  }

  private Map<String, Object> parse(CompositeAggregation.Bucket bucket) {
    Map<String, Object> resultMap = new HashMap<>();
    resultMap.putAll(bucket.getKey());
    resultMap.put("count(*)", bucket.getDocCount());
    return resultMap;
  }
}
