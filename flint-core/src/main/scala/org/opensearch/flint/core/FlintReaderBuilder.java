/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.search.builder.PointInTimeBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.slice.SliceBuilder;

public interface FlintReaderBuilder {
  SearchSourceBuilder enrich(SearchSourceBuilder searchSourceBuilder);

  class FlintSliceReaderBuilder implements FlintReaderBuilder {
    private final int sliceId;

    private final int maxSlice;

    private final String pit;

    private final int scrollSize;

    public FlintSliceReaderBuilder(int sliceId, int maxSlice, String pit, int scrollSize) {
      this.sliceId = sliceId;
      this.maxSlice = maxSlice;
      this.pit = pit;
      this.scrollSize = scrollSize;
    }

    public SearchSourceBuilder enrich(SearchSourceBuilder searchSourceBuilder) {
      return searchSourceBuilder.slice(new SliceBuilder(sliceId, maxSlice))
          .pointInTimeBuilder(new PointInTimeBuilder(pit).setKeepAlive(TimeValue.timeValueMinutes(5)))
          .size(scrollSize);
    }
  }

  class FlintPITReaderBuilder implements FlintReaderBuilder {

    private final String pit;

    private final int scrollSize;

    public FlintPITReaderBuilder( String pit, int scrollSize) {
      this.pit = pit;
      this.scrollSize = scrollSize;
    }

    public SearchSourceBuilder enrich(SearchSourceBuilder searchSourceBuilder) {
      return searchSourceBuilder
          .pointInTimeBuilder(new PointInTimeBuilder(pit).setKeepAlive(TimeValue.timeValueMinutes(5)))
          .size(scrollSize);
    }
  }

  class FlintNoOpReaderBuilder implements FlintReaderBuilder {
    @Override public SearchSourceBuilder enrich(SearchSourceBuilder searchSourceBuilder) {
      return searchSourceBuilder;
    }
  }

}
