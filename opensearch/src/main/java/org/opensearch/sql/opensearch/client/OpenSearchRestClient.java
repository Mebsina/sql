/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.client;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.RequiredArgsConstructor;
import org.opensearch.action.admin.cluster.settings.ClusterGetSettingsRequest;
import org.opensearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.opensearch.action.search.*;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.client.indices.GetIndexResponse;
import org.opensearch.client.indices.GetMappingsRequest;
import org.opensearch.client.indices.GetMappingsResponse;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.sql.opensearch.mapping.IndexMapping;
import org.opensearch.sql.opensearch.request.OpenSearchQueryRequest;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;
import org.opensearch.sql.opensearch.request.OpenSearchScrollRequest;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;
import org.opensearch.transport.client.node.NodeClient;

/**
 * OpenSearch REST client to support standalone mode that runs entire engine from remote.
 *
 * <p>TODO: Support for authN and authZ with AWS Sigv4 or security plugin.
 */
@RequiredArgsConstructor
public class OpenSearchRestClient implements OpenSearchClient {

  /** OpenSearch high level REST client. */
  private final RestHighLevelClient client;
  

  @Override
  public boolean exists(String indexName) {
    try {
      GetIndexRequest request = new GetIndexRequest(indexName);
      System.out.println("OpenSearchRestClient.exists()");
      System.out.println("Index name: " + indexName);
      System.out.println("Request type: " + request.getClass().getSimpleName());
      // Removed hardcoded endpoint
      System.out.println("Request information: Checking if index exists");
      boolean result = client.indices().exists(request, RequestOptions.DEFAULT);
      System.out.println("Result: " + result);
      return result;
    } catch (IOException e) {
      System.out.println("Error in exists(): " + e.getMessage());
      throw new IllegalStateException("Failed to check if index [" + indexName + "] exist", e);
    }
  }

  @Override
  public void createIndex(String indexName, Map<String, Object> mappings) {
    try {
      CreateIndexRequest request = new CreateIndexRequest(indexName).mapping(mappings);
      System.out.println("OpenSearchRestClient.createIndex()");
      System.out.println("Index name: " + indexName);
      System.out.println("Request type: " + request.getClass().getSimpleName());
      // Removed hardcoded endpoint
      System.out.println("Request information: Creating index with mappings");
      System.out.println("Mappings: " + mappings);
      client.indices().create(request, RequestOptions.DEFAULT);
      System.out.println("Index created successfully");
    } catch (IOException e) {
      System.out.println("Error in createIndex(): " + e.getMessage());
      throw new IllegalStateException("Failed to create index [" + indexName + "]", e);
    }
  }

  @Override
  public Map<String, IndexMapping> getIndexMappings(String... indexExpression) {
    GetMappingsRequest request = new GetMappingsRequest().indices(indexExpression);
    try {
      System.out.println("OpenSearchRestClient.getIndexMappings()");
      System.out.println("Index names: " + Arrays.toString(indexExpression));
      System.out.println("Request type: " + request.getClass().getSimpleName());
      // Removed hardcoded endpoint
      System.out.println("Request information: Getting index mappings");
      GetMappingsResponse response = client.indices().getMapping(request, RequestOptions.DEFAULT);
      Map<String, IndexMapping> result = response.mappings().entrySet().stream()
          .collect(Collectors.toMap(Map.Entry::getKey, e -> new IndexMapping(e.getValue())));
      System.out.println("Retrieved mappings for " + result.size() + " indices");
      return result;
    } catch (IOException e) {
      System.out.println("Error in getIndexMappings(): " + e.getMessage());
      throw new IllegalStateException("Failed to get index mappings for " + Arrays.toString(indexExpression), e);
    }
  }

  @Override
  public Map<String, Integer> getIndexMaxResultWindows(String... indexExpression) {
    GetSettingsRequest request =
        new GetSettingsRequest().indices(indexExpression).includeDefaults(true);
    try {
      System.out.println("OpenSearchRestClient.getIndexMaxResultWindows()");
      System.out.println("Index names: " + Arrays.toString(indexExpression));
      System.out.println("Request type: " + request.getClass().getSimpleName());
      System.out.println("Request information: Getting max result window settings");
      GetSettingsResponse response = client.indices().getSettings(request, RequestOptions.DEFAULT);
      Map<String, Settings> settings = response.getIndexToSettings();
      Map<String, Settings> defaultSettings = response.getIndexToDefaultSettings();
      Map<String, Integer> result = new HashMap<>();

      defaultSettings.forEach(
          (key, value) -> {
            Integer maxResultWindow = value.getAsInt("index.max_result_window", null);
            if (maxResultWindow != null) {
              result.put(key, maxResultWindow);
            }
          });

      settings.forEach(
          (key, value) -> {
            Integer maxResultWindow = value.getAsInt("index.max_result_window", null);
            if (maxResultWindow != null) {
              result.put(key, maxResultWindow);
            }
          });

      System.out.println("Retrieved max result window settings for " + result.size() + " indices");
      return result;
    } catch (IOException e) {
      System.out.println("Error in getIndexMaxResultWindows(): " + e.getMessage());
      throw new IllegalStateException("Failed to get max result window for " + Arrays.toString(indexExpression), e);
    }
  }

  @Override
  public OpenSearchResponse search(OpenSearchRequest request) {
    System.out.println("OpenSearchRestClient.search()");
    System.out.println("Request type: " + request.getClass().getSimpleName());
    System.out.println("Request information: Performing search operation");
    
    if (request instanceof OpenSearchScrollRequest) {
        OpenSearchScrollRequest scrollRequest = (OpenSearchScrollRequest) request;
        System.out.println("Scroll request - Index names: " + Arrays.toString(scrollRequest.getIndexName().getIndexNames()));
        System.out.println("Scroll request - Scroll ID: " + scrollRequest.getScrollId());
        System.out.println("Scroll request - Scroll timeout: " + scrollRequest.getScrollTimeout());
        System.out.println("Scroll request - Is scroll: " + scrollRequest.isScroll());
    } else if (request instanceof OpenSearchQueryRequest) {
        OpenSearchQueryRequest queryRequest = (OpenSearchQueryRequest) request;
        System.out.println("Query request - Index names: " + Arrays.toString(queryRequest.getIndexName().getIndexNames()));
        System.out.println("Query request - Source builder: " + queryRequest.getSourceBuilder());
        System.out.println("Query request - PIT ID: " + queryRequest.getPitId());
    }
    
    return request.search(
        req -> {
          try {
            System.out.println("Executing search request: " + req.getClass().getSimpleName());
            System.out.println("Search indices: " + Arrays.toString(req.indices()));
            
            // Get the DSL query from the request source
            String dslQuery = req.source() != null ? req.source().toString() : null;
            System.out.println("Search source: " + dslQuery);
            
            // Add logging to see the request details before it's sent
            System.out.println("=== OPENSEARCH REST CLIENT - BEFORE SENDING ===");
            System.out.println("Request class: " + req.getClass().getName());
            System.out.println("Request toString: " + req.toString());
            
            // Use reflection to get more details about the request
            try {
                java.lang.reflect.Field[] fields = req.getClass().getDeclaredFields();
                for (java.lang.reflect.Field field : fields) {
                    field.setAccessible(true);
                    System.out.println("Field " + field.getName() + ": " + field.get(req));
                }
            } catch (Exception e) {
                System.out.println("Error getting request fields: " + e.getMessage());
            }
            
            SearchResponse response = client.search(req, RequestOptions.DEFAULT);
            return response;
          } catch (IOException e) {
            System.out.println("Error in search(): " + e.getMessage());
            throw new IllegalStateException(
                "Failed to perform search operation with request " + req, e);
          }
        },
        req -> {
          try {
            System.out.println("Executing scroll request: " + req.getClass().getSimpleName());
            System.out.println("Scroll ID: " + req.scrollId());
            // Removed hardcoded endpoint
            SearchResponse response = client.scroll(req, RequestOptions.DEFAULT);
            return response;
          } catch (IOException e) {
            System.out.println("Error in scroll(): " + e.getMessage());
            throw new IllegalStateException(
                "Failed to perform scroll operation with request " + req, e);
          }
        });
  }

  /**
   * Get the combination of the indices and the alias.
   *
   * @return the combination of the indices and the alias
   */
  @Override
  public List<String> indices() {
    try {
      GetIndexRequest request = new GetIndexRequest();
      System.out.println("OpenSearchRestClient.indices()");
      System.out.println("Request type: " + request.getClass().getSimpleName());
      System.out.println("Request information: Getting all indices and aliases");
      
      GetIndexResponse indexResponse = client.indices().get(request, RequestOptions.DEFAULT);
      final Stream<String> aliasStream =
          ImmutableList.copyOf(indexResponse.getAliases().values()).stream()
              .flatMap(Collection::stream)
              .map(AliasMetadata::alias);
      List<String> result = Stream.concat(Arrays.stream(indexResponse.getIndices()), aliasStream)
          .collect(Collectors.toList());
      
      System.out.println("Retrieved " + result.size() + " indices and aliases");
      return result;
    } catch (IOException e) {
      System.out.println("Error in indices(): " + e.getMessage());
      throw new IllegalStateException("Failed to get indices", e);
    }
  }

  /**
   * Get meta info of the cluster.
   *
   * @return meta info of the cluster.
   */
  @Override
  public Map<String, String> meta() {
    try {
      ClusterGetSettingsRequest request = new ClusterGetSettingsRequest();
      request.includeDefaults(true);
      request.local(true);
      
      System.out.println("OpenSearchRestClient.meta()");
      System.out.println("Request type: " + request.getClass().getSimpleName());
      System.out.println("Request information: Getting cluster metadata");
      
      final ImmutableMap.Builder<String, String> builder = new ImmutableMap.Builder<>();
      final Settings defaultSettings =
          client.cluster().getSettings(request, RequestOptions.DEFAULT).getDefaultSettings();
      builder.put(META_CLUSTER_NAME, defaultSettings.get("cluster.name", "opensearch"));
      builder.put(
          "plugins.sql.pagination.api", defaultSettings.get("plugins.sql.pagination.api", "true"));
      
      Map<String, String> result = builder.build();
      System.out.println("Retrieved cluster metadata: " + result);
      return result;
    } catch (IOException e) {
      System.out.println("Error in meta(): " + e.getMessage());
      throw new IllegalStateException("Failed to get cluster meta info", e);
    }
  }

  @Override
  public void cleanup(OpenSearchRequest request) {
    System.out.println("OpenSearchRestClient.cleanup()");
    System.out.println("Request type: " + request.getClass().getSimpleName());
    System.out.println("Request information: Cleaning up resources");
    
    if (request instanceof OpenSearchScrollRequest) {
      System.out.println("Cleaning up scroll resources");
      request.clean(
          scrollId -> {
            try {
              ClearScrollRequest clearRequest = new ClearScrollRequest();
              clearRequest.addScrollId(scrollId);
              System.out.println("Clearing scroll ID: " + scrollId);
              client.clearScroll(clearRequest, RequestOptions.DEFAULT);
              System.out.println("Scroll resources cleaned up successfully");
            } catch (IOException e) {
              System.out.println("Error in cleanup() for scroll: " + e.getMessage());
              throw new IllegalStateException(
                  "Failed to clean up resources for search request " + request, e);
            }
          });
    } else {
      System.out.println("Cleaning up PIT resources");
      request.clean(
          pitId -> {
            System.out.println("Deleting PIT ID: " + pitId);
            DeletePitRequest deletePitRequest = new DeletePitRequest(pitId);
            deletePit(deletePitRequest);
            System.out.println("PIT resources cleaned up successfully");
          });
    }
  }

  @Override
  public void schedule(Runnable task) {
    task.run();
  }

  @Override
  public NodeClient getNodeClient() {
    throw new UnsupportedOperationException("Unsupported method.");
  }

  @Override
  public String createPit(CreatePitRequest createPitRequest) {
    System.out.println("OpenSearchRestClient.createPit()");
    System.out.println("Request type: " + createPitRequest.getClass().getSimpleName());
    System.out.println("Request information: Creating Point in Time");
    System.out.println("Index names: " + Arrays.toString(createPitRequest.indices()));
    
    try {
      CreatePitResponse createPitResponse =
          client.createPit(createPitRequest, RequestOptions.DEFAULT);
      String pitId = createPitResponse.getId();
      System.out.println("PIT created successfully with ID: " + pitId);
      return pitId;
    } catch (IOException e) {
      System.out.println("Error in createPit(): " + e.getMessage());
      
      // Enhanced exception handling with more details
      System.out.println("Exception details in createPit():");
      System.out.println("Exception class: " + e.getClass().getName());
      System.out.println("Exception message: " + e.getMessage());
      
      if (e.getCause() != null) {
        System.out.println("Cause class: " + e.getCause().getClass().getName());
        System.out.println("Cause message: " + e.getCause().getMessage());
      }
      
      // Print the full stack trace for detailed debugging
      System.out.println("Stack trace:");
      e.printStackTrace();
      
      throw new RuntimeException("Error occurred while creating PIT for new engine SQL query", e);
    } 
  }

  @Override
  public void deletePit(DeletePitRequest deletePitRequest) {
    System.out.println("OpenSearchRestClient.deletePit()");
    System.out.println("Request type: " + deletePitRequest.getClass().getSimpleName());
    System.out.println("Request information: Deleting Point in Time");
    
    try {
      DeletePitResponse deletePitResponse =
          client.deletePit(deletePitRequest, RequestOptions.DEFAULT);
      System.out.println("PIT deleted successfully. Response: " + deletePitResponse);
    } catch (IOException e) {
      System.out.println("Error in deletePit(): " + e.getMessage());
      
      // Enhanced exception handling with more details
      System.out.println("Exception details in deletePit():");
      System.out.println("Exception class: " + e.getClass().getName());
      System.out.println("Exception message: " + e.getMessage());
      
      if (e.getCause() != null) {
        System.out.println("Cause class: " + e.getCause().getClass().getName());
        System.out.println("Cause message: " + e.getCause().getMessage());
      }
      
      // Print the full stack trace for detailed debugging
      System.out.println("Stack trace:");
      e.printStackTrace();
      
      throw new RuntimeException("Error occurred while deleting PIT for new engine SQL query", e);
    } 
  }
}
