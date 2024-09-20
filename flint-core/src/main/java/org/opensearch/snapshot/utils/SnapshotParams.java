/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.snapshot.utils;

import java.io.Serializable;

public class SnapshotParams implements Serializable {
  private final String snapshotName;
  private final String s3Bucket;
  private final String basePath;
  private final String s3Region;
  private final String tableName;

  private SnapshotParams(Builder builder) {
    this.snapshotName = builder.snapshotName;
    this.s3Bucket = builder.s3Bucket;
    this.basePath = builder.basePath;
    this.s3Region = builder.s3Region;
    this.tableName = builder.tableName;
  }

  public String getSnapshotName() { return snapshotName; }
  public String getS3Bucket() { return s3Bucket; }
  public String getBasePath() { return basePath; }
  public String getS3Region() { return s3Region; }
  public String getTableName() { return tableName; }

  public static class Builder {
    private String snapshotName;
    private String s3Bucket;
    private String basePath;
    private String s3Region;
    private String s3AccessKey;
    private String s3SecretKey;
    private String tableName;

    public Builder snapshotName(String snapshotName) {
      this.snapshotName = snapshotName;
      return this;
    }

    public Builder s3Bucket(String s3Bucket) {
      this.s3Bucket = s3Bucket;
      return this;
    }

    public Builder basePath(String basePath) {
      this.basePath = basePath;
      return this;
    }

    public Builder s3Region(String s3Region) {
      this.s3Region = s3Region;
      return this;
    }

    public Builder tableName(String tableName) {
      this.tableName = tableName;
      return this;
    }

    public SnapshotParams build() {
      return new SnapshotParams(this);
    }
  }
}
