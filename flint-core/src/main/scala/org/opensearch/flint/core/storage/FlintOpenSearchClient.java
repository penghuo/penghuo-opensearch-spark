/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage;

import static org.opensearch.common.xcontent.DeprecationHandler.IGNORE_DEPRECATIONS;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.admin.indices.get.GetIndexAction;
import org.opensearch.action.search.CreatePitRequest;
import org.opensearch.action.search.CreatePitResponse;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.client.indices.GetIndexResponse;
import org.opensearch.client.indices.PutMappingRequest;
import org.opensearch.client.opensearch.indices.IndicesStatsRequest;
import org.opensearch.client.opensearch.indices.IndicesStatsResponse;
import org.opensearch.client.opensearch.indices.stats.IndicesStats;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.common.Strings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.flint.core.FlintClient;
import org.opensearch.flint.core.FlintOptions;
import org.opensearch.flint.core.FlintReaderBuilder;
import org.opensearch.flint.core.IRestHighLevelClient;
import org.opensearch.flint.core.metadata.FlintMetadata;
import org.opensearch.flint.core.storage.stats.IndexStatsInfo;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.SearchModule;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.SortOrder;
import scala.Option;

/**
 * Flint client implementation for OpenSearch storage.
 */
public class FlintOpenSearchClient implements FlintClient {

  private static final Logger LOG = Logger.getLogger(FlintOpenSearchClient.class.getName());

  /**
   * {@link NamedXContentRegistry} from {@link SearchModule} used for construct {@link QueryBuilder} from DSL query string.
   */
  private final static NamedXContentRegistry
      xContentRegistry =
      new NamedXContentRegistry(new SearchModule(Settings.builder().build(),
          new ArrayList<>()).getNamedXContents());

  /**
   * Invalid index name characters to percent-encode,
   * excluding '*' because it's reserved for pattern matching.
   */
  private final static Set<Character>
      INVALID_INDEX_NAME_CHARS =
      Set.of(' ', ',', ':', '"', '+', '/', '\\', '|', '?', '#', '>', '<');

  private final static Function<String, String>
      SHARD_ID_PREFERENCE =
      shardId -> shardId == null ? shardId : "_shards:" + shardId;

  private final FlintOptions options;

  public FlintOpenSearchClient(FlintOptions options) {
    this.options = options;
  }

  @Override public void createIndex(String indexName, FlintMetadata metadata) {
    LOG.info("Creating Flint index " + indexName + " with metadata " + metadata);
    createIndex(indexName, metadata.getContent(), metadata.indexSettings());
  }

  protected void createIndex(String indexName, String mapping, Option<String> settings) {
    LOG.info("Creating Flint index " + indexName);
    String osIndexName = sanitizeIndexName(indexName);
    try (IRestHighLevelClient client = createClient()) {
      CreateIndexRequest request = new CreateIndexRequest(osIndexName);
      request.mapping(mapping, XContentType.JSON);
      if (settings.isDefined()) {
        request.settings(settings.get(), XContentType.JSON);
      }
      client.createIndex(request, RequestOptions.DEFAULT);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to create Flint index " + osIndexName, e);
    }
  }

  @Override public boolean exists(String indexName) {
    LOG.info("Checking if Flint index exists " + indexName);
    String osIndexName = sanitizeIndexName(indexName);
    try (IRestHighLevelClient client = createClient()) {
      return client.doesIndexExist(new GetIndexRequest(osIndexName), RequestOptions.DEFAULT);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to check if Flint index exists " + osIndexName, e);
    }
  }

  @Override public Map<String, FlintMetadata> getAllIndexMetadata(String... indexNamePattern) {
    LOG.info("Fetching all Flint index metadata for pattern " + String.join(",", indexNamePattern));
    String[]
        indexNames =
        Arrays.stream(indexNamePattern).map(this::sanitizeIndexName).toArray(String[]::new);
    try (IRestHighLevelClient client = createClient()) {
      GetIndexRequest request = new GetIndexRequest(indexNames);
      GetIndexResponse response = client.getIndex(request, RequestOptions.DEFAULT);

      return Arrays.stream(response.getIndices())
          .collect(Collectors.toMap(index -> index,
              index -> FlintMetadata.apply(response.getMappings().get(index).source().toString(),
                  response.getSettings().get(index).toString())));
    } catch (Exception e) {
      throw new IllegalStateException("Failed to get Flint index metadata for " + String.join(
          ",",
          indexNames), e);
    }
  }

  @Override public FlintMetadata getIndexMetadata(String indexName) {
    LOG.info("Fetching Flint index metadata for " + indexName);
    String osIndexName = sanitizeIndexName(indexName);
    try (IRestHighLevelClient client = createClient()) {
      GetIndexRequest request = new GetIndexRequest(osIndexName);
      GetIndexResponse response = client.getIndex(request, RequestOptions.DEFAULT);

      MappingMetadata mapping = response.getMappings().get(osIndexName);
      Settings settings = response.getSettings().get(osIndexName);
      return FlintMetadata.apply(mapping.source().string(), settings.toString());
    } catch (Exception e) {
      throw new IllegalStateException("Failed to get Flint index metadata for " + osIndexName, e);
    }
  }

  @Override
  public Map<String, IndexStatsInfo> getIndexStats(String... indexNamePattern) {
    List<String>
        indexNames =
        Arrays.stream(indexNamePattern).map(this::sanitizeIndexName).collect(Collectors.toList());

    try (IRestHighLevelClient client = createClient()) {
      IndicesStatsRequest request =
          new IndicesStatsRequest.Builder().index(indexNames).metric("docs", "store").build();
      IndicesStatsResponse response = client.stats(request);
      return response.indices().entrySet().stream()
          .collect(Collectors.toMap(
              Map.Entry::getKey,
              entry -> {
                IndicesStats indicesStats = entry.getValue();
                long docCount = indicesStats.total().docs().count();
                long sizeInBytes = indicesStats.total().store().sizeInBytes();
                return new IndexStatsInfo(docCount, sizeInBytes);
              }
          ));
    } catch (Exception e) {
      throw new IllegalStateException("Failed to get Flint index metadata for " + String.join(
          ",",
          indexNames), e);
    }
  }

  @Override public void updateIndex(String indexName, FlintMetadata metadata) {
    LOG.info("Updating Flint index " + indexName + " with metadata " + metadata);
    String osIndexName = sanitizeIndexName(indexName);
    try (IRestHighLevelClient client = createClient()) {
      PutMappingRequest request = new PutMappingRequest(osIndexName);
      request.source(metadata.getContent(), XContentType.JSON);
      client.updateIndexMapping(request, RequestOptions.DEFAULT);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to update Flint index " + osIndexName, e);
    }
  }

  @Override public void deleteIndex(String indexName) {
    LOG.info("Deleting Flint index " + indexName);
    String osIndexName = sanitizeIndexName(indexName);
    try (IRestHighLevelClient client = createClient()) {
      DeleteIndexRequest request = new DeleteIndexRequest(osIndexName);
      client.deleteIndex(request, RequestOptions.DEFAULT);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to delete Flint index " + osIndexName, e);
    }
  }

  /**
   * Create {@link FlintReader}.
   *
   * @param indexName index name.
   * @param query     DSL query. DSL query is null means match_all.
   * @return {@link FlintReader}.
   */
  @Override public FlintReader createReader(String indexName, String query) {
    return createReader(indexName, query, new FlintReaderBuilder.FlintNoOpReaderBuilder());
  }

  /**
   * Create {@link FlintReader}.
   *
   * @param indexName index name.
   * @param query DSL query. DSL query is null means match_all
   * @return
   */
  @Override public FlintReader createReader(String indexName, String query, FlintReaderBuilder builder) {
    LOG.info("Creating Flint index reader for " + indexName + " with query " + query);
    try {
      QueryBuilder queryBuilder = new MatchAllQueryBuilder();
      if (!Strings.isNullOrEmpty(query)) {
        XContentParser
            parser =
            XContentType.JSON.xContent().createParser(xContentRegistry, IGNORE_DEPRECATIONS, query);
        queryBuilder = AbstractQueryBuilder.parseInnerQueryBuilder(parser);
      }
      return new OpenSearchPITSearchAfterQueryReader(createClient(),
          sanitizeIndexName(indexName),
          builder.enrich(new SearchSourceBuilder().query(queryBuilder)
                  .sort("_doc", SortOrder.ASC).sort("_id", SortOrder.ASC)
          ));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override public String createPit(String indexName) {
    String osIndexName = sanitizeIndexName(indexName);
    try (IRestHighLevelClient client = createClient()) {
      CreatePitRequest request = new CreatePitRequest(TimeValue.timeValueMinutes(5), true, indexName);
      CreatePitResponse response = client.createPit(request, RequestOptions.DEFAULT);
      return response.getId();
    } catch (Exception e) {
      throw new IllegalStateException("Failed to delete Flint index " + osIndexName, e);
    }
  }

  public FlintWriter createWriter(String indexName) {
    LOG.info(String.format(
        "Creating Flint index writer for %s, refresh_policy:%s, " + "batch_bytes:%d",
        indexName,
        options.getRefreshPolicy(),
        options.getBatchBytes()));
    return new OpenSearchWriter(createClient(),
        sanitizeIndexName(indexName),
        options.getRefreshPolicy(),
        options.getBatchBytes());
  }

  @Override public IRestHighLevelClient createClient() {
    return OpenSearchClientUtils.createClient(options);
  }

  /*
   * Because OpenSearch requires all lowercase letters in index name, we have to
   * lowercase all letters in the given Flint index name.
   */
  private String toLowercase(String indexName) {
    Objects.requireNonNull(indexName);

    return indexName.toLowerCase(Locale.ROOT);
  }

  /*
   * Percent-encode invalid OpenSearch index name characters.
   */
  private String percentEncode(String indexName) {
    Objects.requireNonNull(indexName);

    StringBuilder builder = new StringBuilder(indexName.length());
    for (char ch : indexName.toCharArray()) {
      if (INVALID_INDEX_NAME_CHARS.contains(ch)) {
        builder.append(String.format("%%%02X", (int) ch));
      } else {
        builder.append(ch);
      }
    }
    return builder.toString();
  }

  /*
   * Sanitize index name to comply with OpenSearch index name restrictions.
   */
  private String sanitizeIndexName(String indexName) {
    Objects.requireNonNull(indexName);

    String encoded = percentEncode(indexName);
    return toLowercase(encoded);
  }
}
