package org.opensearch.snapshot.utils;

import org.apache.lucene.store.FSDirectory;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.opensearch.cluster.ClusterModule;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.io.PathUtils;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.NoopCircuitBreaker;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.opensearch.index.store.remote.directory.RemoteSnapshotDirectory;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.filecache.FileCacheFactory;
import org.opensearch.index.store.remote.utils.TransferManager;
import org.opensearch.monitor.jvm.JvmInfo;
import org.opensearch.repositories.IndexId;
import org.opensearch.repositories.RepositoryData;
import org.opensearch.repositories.blobstore.ChecksumBlobStoreFormat;
import org.opensearch.repositories.s3.GenericStatsMetricPublisher;
import org.opensearch.repositories.s3.S3BlobStore;
import org.opensearch.repositories.s3.S3ClientSettings;
import org.opensearch.repositories.s3.S3Service;
import org.opensearch.repositories.s3.SocketAccess;
import org.opensearch.repositories.s3.async.AsyncExecutorContainer;
import org.opensearch.repositories.s3.async.AsyncTransferEventLoopGroup;
import org.opensearch.repositories.s3.async.AsyncTransferManager;
import org.opensearch.repositories.s3.async.SizeBasedBlockingQ;
import org.opensearch.repositories.s3.async.TransferSemaphoresHolder;
import org.opensearch.snapshots.SnapshotId;
import org.opensearch.snapshots.SnapshotInfo;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.opensearch.repositories.blobstore.BlobStoreRepository.INDEX_FILE_PREFIX;
import static org.opensearch.repositories.blobstore.BlobStoreRepository.METADATA_NAME_FORMAT;
import static org.opensearch.repositories.blobstore.BlobStoreRepository.SNAPSHOT_NAME_FORMAT;

public class SnapshotUtil {

  public static final ChecksumBlobStoreFormat<IndexMetadata> INDEX_METADATA_FORMAT =
      new ChecksumBlobStoreFormat<>(
          "index-metadata",
          METADATA_NAME_FORMAT,
          IndexMetadata::fromXContent
      );

  public static final ChecksumBlobStoreFormat<SnapshotInfo> SNAPSHOT_FORMAT =
      new ChecksumBlobStoreFormat<>(
          "snapshot",
          SNAPSHOT_NAME_FORMAT,
          SnapshotInfo::fromXContentInternal
      );

  public static final ChecksumBlobStoreFormat<org.opensearch.cluster.metadata.Metadata>
      GLOBAL_METADATA_FORMAT = new ChecksumBlobStoreFormat<>(
      "metadata",
      METADATA_NAME_FORMAT,
      org.opensearch.cluster.metadata.Metadata::fromXContent
  );

  public static final ChecksumBlobStoreFormat<BlobStoreIndexShardSnapshot>
      INDEX_SHARD_SNAPSHOT_FORMAT = new ChecksumBlobStoreFormat<>(
      "snapshot",
      SNAPSHOT_NAME_FORMAT,
      BlobStoreIndexShardSnapshot::fromXContent
  );

  public static S3BlobStore createS3BlobStore(SnapshotParams snapshotParams) {
    Path configPath = PathUtils.get("config");
    S3Service s3Service = new S3Service(configPath);
    final Settings.Builder clientSettings = Settings.builder();
    clientSettings.put("s3.client.default.region", snapshotParams.getS3Region());
    final MockSecureSettings secureSettings = new MockSecureSettings();
//    secureSettings.setString(
//        "s3.client.default.access_key", snapshotParams.getS3AccessKey());
//    secureSettings.setString(
//        "s3.client.default.secret_key", snapshotParams.getS3SecretKey());
    clientSettings.setSecureSettings(secureSettings);
    s3Service.refreshAndClearCache(S3ClientSettings.load(clientSettings.build(), configPath));
    SocketAccess.doPrivileged(
        () -> System.setProperty("opensearch.path.conf", configPath.toString()));
    ExecutorService futureCompletionService = null;
    ExecutorService streamReaderService = null;
    ExecutorService transferQueueConsumerService = Executors.newFixedThreadPool(2);
    AsyncTransferEventLoopGroup transferNIOGroup = new AsyncTransferEventLoopGroup(1);
    GenericStatsMetricPublisher
        genericStatsMetricPublisher = new GenericStatsMetricPublisher(10000L, 10, 10000L, 10);
    SizeBasedBlockingQ normalPrioritySizeBasedBlockingQ = new SizeBasedBlockingQ(
        new ByteSizeValue(Runtime.getRuntime().availableProcessors() * 10L, ByteSizeUnit.GB),
        transferQueueConsumerService,
        10,
        genericStatsMetricPublisher,
        SizeBasedBlockingQ.QueueEventType.NORMAL
    );
    SizeBasedBlockingQ lowPrioritySizeBasedBlockingQ = new SizeBasedBlockingQ(
        new ByteSizeValue(Runtime.getRuntime().availableProcessors() * 20L, ByteSizeUnit.GB),
        transferQueueConsumerService,
        5,
        genericStatsMetricPublisher,
        SizeBasedBlockingQ.QueueEventType.NORMAL
    );
    normalPrioritySizeBasedBlockingQ.start();
    lowPrioritySizeBasedBlockingQ.start();
    final RepositoryMetadata repositoryMetadata = new RepositoryMetadata(
        "repository",
        "s3",
        Settings.builder().put("client", "default").put("bucket", snapshotParams.getS3Bucket())
            .build()
    );
    AsyncExecutorContainer asyncExecutorContainer = new AsyncExecutorContainer(
        futureCompletionService,
        streamReaderService,
        transferNIOGroup
    );
    return new S3BlobStore(
        s3Service,
        null,
        true,
        snapshotParams.getS3Bucket(),
        false,
        new ByteSizeValue(
            Math.max(
                ByteSizeUnit.MB.toBytes(5), // minimum value
                Math.min(ByteSizeUnit.MB.toBytes(100),
                    JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() / 20)
            ),
            ByteSizeUnit.BYTES
        ),
        "",
        "",
        1000,
        repositoryMetadata,
        new AsyncTransferManager(
            new ByteSizeValue(
                ByteSizeUnit.MB.toBytes(16),
                ByteSizeUnit.BYTES
            ).getBytes(),
            asyncExecutorContainer.getStreamReader(),
            asyncExecutorContainer.getStreamReader(),
            asyncExecutorContainer.getStreamReader(),
            new TransferSemaphoresHolder(
                3,
                Math.max(Runtime.getRuntime().availableProcessors() * 5, 10),
                5,
                TimeUnit.MINUTES,
                genericStatsMetricPublisher
            )
        ),
        asyncExecutorContainer,
        asyncExecutorContainer,
        asyncExecutorContainer,
        normalPrioritySizeBasedBlockingQ,
        lowPrioritySizeBasedBlockingQ,
        genericStatsMetricPublisher
    );
  }

  public static SnapshotTableMetadata getSnapshotTableMetadata(S3BlobStore s3BlobStore,
                                                               SnapshotParams snapshotParams) {
    try {
      final BlobPath snapshotRootPath = new BlobPath().add(snapshotParams.getBasePath());
      BlobContainer snapshotRootContainer = s3BlobStore.blobContainer(snapshotRootPath);
      NamedXContentRegistry namedXContentRegistry =
          new NamedXContentRegistry(ClusterModule.getNamedXWriteables());
      long indexGen = 0;
      RepositoryData repositoryData = null;
      indexGen = listBlobsToGetLatestIndexId(snapshotRootContainer);
      repositoryData = getRepositoryData(snapshotRootContainer, indexGen);
      Optional<SnapshotId> matchingSnapshotId = repositoryData.getSnapshotIds()
          .stream()
          .filter(s -> snapshotParams.getSnapshotName().equals(s.getName()))
          .findFirst();

      if (matchingSnapshotId.isEmpty()) {
        throw new RuntimeException("Snapshot not found: " + snapshotParams.getSnapshotName());
      }
      SnapshotId snapshotId = matchingSnapshotId.get();
      SnapshotInfo snapshotInfo =
          SNAPSHOT_FORMAT.read(snapshotRootContainer, snapshotId.getUUID(), namedXContentRegistry);
      //handle alias
      boolean isIndexPresent = snapshotInfo.indices().stream()
          .anyMatch(index -> index.equals(snapshotParams.getTableName()));
      if (!isIndexPresent) {
        throw new RuntimeException("Table not found: " + snapshotParams.getTableName());
      }
      IndexId indexId = repositoryData.resolveIndexId(snapshotParams.getTableName());
      IndexMetadata indexMetadata = INDEX_METADATA_FORMAT.read(
          s3BlobStore.blobContainer(snapshotRootPath.add("indices").add(indexId.getId())),
          repositoryData.indexMetaDataGenerations().indexMetaBlobId(snapshotId, indexId),
          namedXContentRegistry
      );
      return new SnapshotTableMetadata(indexMetadata, snapshotInfo, indexId);
    } catch (IOException e) {
      throw new RuntimeException(e.getMessage());
    }
  }


  public static RemoteSnapshotDirectory getRemoteSnapShotDirectory(SnapshotParams snapshotParams,
                                                                   String snapshotUUID,
                                                                   String indexId, String shardId) {
    try (S3BlobStore s3BlobStore = createS3BlobStore(snapshotParams)) {
      NamedXContentRegistry namedXContentRegistry =
          new NamedXContentRegistry(ClusterModule.getNamedXWriteables());
      BlobContainer shardContainer = s3BlobStore.blobContainer(
          new BlobPath().add(snapshotParams.getBasePath())
              .add("indices")
              .add(indexId)
              .add(shardId));
      BlobStoreIndexShardSnapshot blobStoreIndexShardSnapshot =
          INDEX_SHARD_SNAPSHOT_FORMAT.read(shardContainer,
              snapshotUUID,
              namedXContentRegistry);
      Path localStorePath =
          Files.createDirectories(Paths.get("RemoteLocalStore").resolve(shardId));
      FSDirectory localStoreDir = FSDirectory.open(localStorePath);
      return new RemoteSnapshotDirectory(blobStoreIndexShardSnapshot, localStoreDir,
          createTransferManager(shardContainer));
    } catch (Exception e) {
      throw new RuntimeException("Error while creating Remote Snapshot Directory");
    }
  }

  public static StructType createSparkSchema(MappingMetadata mapping) {
    Map<String, Object> properties =
        (Map<String, Object>) mapping.getSourceAsMap().get("properties");
    List<StructField> fields = new ArrayList<>();

    for (Map.Entry<String, Object> entry : properties.entrySet()) {
      String fieldName = entry.getKey();
      Map<String, Object> fieldProperties = (Map<String, Object>) entry.getValue();
      fields.add(convertField(fieldName, fieldProperties));
    }

    return new StructType(fields.toArray(new StructField[0]));
  }

  private static StructField convertField(String name, Map<String, Object> props) {
    DataType dataType;
    String type = "object";
    if (props.containsKey("type")) {
      type = props.get("type").toString();
    }

    switch (type) {
      case "long":
        dataType = DataTypes.LongType;
        break;
      case "integer":
        dataType = DataTypes.IntegerType;
        break;
      case "short":
        dataType = DataTypes.ShortType;
        break;
      case "byte":
        dataType = DataTypes.ByteType;
        break;
      case "double":
        dataType = DataTypes.DoubleType;
        break;
      case "float":
        dataType = DataTypes.FloatType;
        break;
      case "boolean":
        dataType = DataTypes.BooleanType;
        break;
      case "date":
        dataType = DataTypes.TimestampType;
        break;
      case "binary":
        dataType = DataTypes.BinaryType;
        break;
      case "object":
        dataType = createSparkSchema(new MappingMetadata(name, props));
        break;
      case "nested":
        dataType = DataTypes.createArrayType(createSparkSchema(new MappingMetadata(name, props)));
        break;
      default:
        dataType = DataTypes.StringType; // Default to StringType for unknown types
    }
    return new StructField(name, dataType, true, Metadata.empty());
  }


  private static long listBlobsToGetLatestIndexId(BlobContainer blobContainer) throws IOException {
    Collection<String> rootBlobs = blobContainer.listBlobsByPrefix(INDEX_FILE_PREFIX).keySet();
    long latest = RepositoryData.EMPTY_REPO_GEN;
    for (String blobName : rootBlobs) {
      if (blobName.startsWith(INDEX_FILE_PREFIX) == false) {
        continue;
      }
      try {
        final long curr = Long.parseLong(blobName.substring(INDEX_FILE_PREFIX.length()));
        latest = Math.max(latest, curr);
      } catch (NumberFormatException nfe) {
        // the index- blob wasn't of the format index-N where N is a number,
        // no idea what this blob is but it doesn't belong in the repository!
      }
    }
    return latest;
  }

  private static RepositoryData getRepositoryData(BlobContainer blobContainer, long indexGen)
      throws IOException {
    final String snapshotsIndexBlobName = INDEX_FILE_PREFIX + indexGen;
    // EMPTY is safe here because RepositoryData#fromXContent calls namedObject
    try (
        InputStream blob = blobContainer.readBlob(snapshotsIndexBlobName);
        XContentParser parser = MediaTypeRegistry.JSON.xContent()
            .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, blob)
    ) {
      return RepositoryData.snapshotsFromXContent(parser, indexGen, true);
    }
  }


  private static TransferManager createTransferManager(BlobContainer shardContainer) {
    final FileCache remoteStoreFileCache = FileCacheFactory.createConcurrentLRUFileCache(
        10*1024,
        1,
        new NoopCircuitBreaker(CircuitBreaker.REQUEST)
    );
    return new TransferManager(shardContainer, remoteStoreFileCache);
  }


}
