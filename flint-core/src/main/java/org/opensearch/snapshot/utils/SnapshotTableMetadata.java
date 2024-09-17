/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.snapshot.utils;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.repositories.IndexId;
import org.opensearch.snapshots.SnapshotInfo;

public class SnapshotTableMetadata {
  private final IndexMetadata indexMetadata;
  private final SnapshotInfo snapshotInfo;
  private final IndexId indexId;

  // Constructor
  public SnapshotTableMetadata(IndexMetadata indexMetadata, SnapshotInfo snapshotInfo, IndexId indexId) {
    this.indexMetadata = indexMetadata;
    this.snapshotInfo = snapshotInfo;
    this.indexId = indexId;
  }

  // Getters
  public IndexMetadata getIndexMetadata() {
    return indexMetadata;
  }

  public SnapshotInfo getSnapshotInfo() {
    return snapshotInfo;
  }

  public IndexId getIndexId() {
    return indexId;
  }
}

