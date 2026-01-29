package com.scylladb.alternator;

import static org.junit.Assert.*;
import static org.junit.Assume.*;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.*;

/**
 * Integration tests for AlternatorDynamoDbAsyncClient. These tests require a running ScyllaDB
 * cluster with Alternator enabled.
 *
 * <p>Set environment variables to configure: - ALTERNATOR_HOST: Host address (default: 172.39.0.2)
 * - ALTERNATOR_PORT: Port number (default: 8000) - ALTERNATOR_HTTPS: Use HTTPS (default: false) -
 * ALTERNATOR_DATACENTER: Datacenter name (default: datacenter1) - ALTERNATOR_RACK: Rack name
 * (default: rack1)
 */
public class AlternatorDynamoDbAsyncClientTest {

  private static String host;
  private static int port;
  private static boolean useHttps;
  private static String datacenter;
  private static String rack;
  private static URI seedUri;
  private static boolean integrationTestsEnabled;
  private static StaticCredentialsProvider credentialsProvider;

  @BeforeClass
  public static void setUpClass() {
    host = System.getenv().getOrDefault("ALTERNATOR_HOST", "172.39.0.2");
    port = Integer.parseInt(System.getenv().getOrDefault("ALTERNATOR_PORT", "9998"));
    useHttps = Boolean.parseBoolean(System.getenv().getOrDefault("ALTERNATOR_HTTPS", "false"));
    datacenter = System.getenv().getOrDefault("ALTERNATOR_DATACENTER", "datacenter1");
    rack = System.getenv().getOrDefault("ALTERNATOR_RACK", "rack1");

    String scheme = useHttps ? "https" : "http";
    try {
      seedUri = new URI(scheme + "://" + host + ":" + port);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }

    credentialsProvider =
        StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"));

    // Check if integration tests should run
    integrationTestsEnabled =
        Boolean.parseBoolean(System.getenv().getOrDefault("INTEGRATION_TESTS", "false"));
  }

  @Before
  public void setUp() {
    assumeTrue(
        "Integration tests disabled. Set INTEGRATION_TESTS=true to enable.",
        integrationTestsEnabled);
  }

  private AlternatorDynamoDbAsyncClientWrapper buildClient(String dc, String rackName) {
    return AlternatorDynamoDbAsyncClient.builder()
        .endpointOverride(seedUri)
        .credentialsProvider(credentialsProvider)
        .withDatacenter(dc)
        .withRack(rackName)
        .buildWithAlternatorAPI();
  }

  private AlternatorDynamoDbAsyncClientWrapper buildClient() {
    return AlternatorDynamoDbAsyncClient.builder()
        .endpointOverride(seedUri)
        .credentialsProvider(credentialsProvider)
        .buildWithAlternatorAPI();
  }

  @Test
  public void testCheckIfRackAndDatacenterSetCorrectly_WrongDC() throws Exception {
    // Wrong datacenter should gracefully fall back to no datacenter filtering
    AlternatorDynamoDbAsyncClientWrapper client = buildClient("wrongDC", "");

    // Should have discovered nodes (fallback should work)
    List<URI> nodes = client.getLiveNodes();
    assertFalse("Should have at least one node after fallback", nodes.isEmpty());

    client.close();
  }

  @Test
  public void testCheckIfRackAndDatacenterSetCorrectly_CorrectDC() throws Exception {
    AlternatorDynamoDbAsyncClientWrapper client = buildClient(datacenter, "");

    // Should have discovered nodes
    List<URI> nodes = client.getLiveNodes();
    assertFalse("Should have at least one node", nodes.isEmpty());

    client.close();
  }

  @Test
  public void testCheckIfRackAndDatacenterSetCorrectly_WrongRack() throws Exception {
    // With wrong rack, pickSupportedDatacenterRack should fallback gracefully
    AlternatorDynamoDbAsyncClientWrapper client = buildClient(datacenter, "wrongRack");

    // Should have fallen back and discovered nodes
    List<URI> nodes = client.getLiveNodes();
    assertFalse("Should have at least one node after fallback", nodes.isEmpty());

    client.close();
  }

  @Test
  public void testCheckIfRackAndDatacenterSetCorrectly_CorrectRack() throws Exception {
    AlternatorDynamoDbAsyncClientWrapper client = buildClient(datacenter, rack);

    // Should have discovered nodes
    List<URI> nodes = client.getLiveNodes();
    assertFalse("Should have at least one node", nodes.isEmpty());

    client.close();
  }

  @Test
  public void testCheckIfRackDatacenterFeatureIsSupported() throws Exception {
    AlternatorDynamoDbAsyncClientWrapper client = buildClient(datacenter, "");

    boolean supported = client.checkIfRackDatacenterFeatureIsSupported();

    assertTrue("ScyllaDB should support rack/datacenter filtering", supported);

    client.close();
  }

  @Test
  public void testRoutingFallback() throws Exception {
    // Create with wrong datacenter - should fallback
    AlternatorDynamoDbAsyncClientWrapper client = buildClient("wrongDC", "");

    // Should have discovered nodes (fallback should work)
    List<URI> nodes = client.getLiveNodes();
    assertFalse("Should have at least one node after fallback", nodes.isEmpty());

    client.close();
  }

  @Test
  public void testUpdateLiveNodes() throws Exception {
    AlternatorDynamoDbAsyncClientWrapper client = buildClient();

    // The background thread is already started by AlternatorEndpointProvider
    // Wait for node list to update
    Thread.sleep(1000);

    List<URI> updatedNodes = client.getLiveNodes();

    // In a multi-node cluster, we should have more nodes
    // In a single-node cluster, we should still have at least one
    assertFalse("Should have at least one node after update", updatedNodes.isEmpty());

    client.close();
  }

  @Test
  public void testNodeDiscoveryWithRoundRobin() throws Exception {
    AlternatorDynamoDbAsyncClientWrapper client = buildClient(datacenter, "");

    // Wait for node discovery
    Thread.sleep(1000);

    Set<URI> meetNodes = new HashSet<>();
    List<URI> allNodes = client.getLiveNodes();

    // Call nextAsURI more times than there are nodes to verify round-robin
    for (int i = 0; i < allNodes.size() * 2; i++) {
      meetNodes.add(client.nextAsURI());
    }

    assertEquals("Should visit all nodes via round-robin", allNodes.size(), meetNodes.size());

    client.close();
  }

  @Test
  public void testDynamoDBOperations() throws Exception {
    String tableName = "java_async_client_test_table";

    AlternatorDynamoDbAsyncClientWrapper wrapper = buildClient();
    DynamoDbAsyncClient client = wrapper.getClient();

    // Clean up if table exists
    try {
      client.deleteTable(DeleteTableRequest.builder().tableName(tableName).build()).get();
      Thread.sleep(500);
    } catch (Exception e) {
      // Table doesn't exist, that's fine
    }

    // Create table
    client
        .createTable(
            CreateTableRequest.builder()
                .tableName(tableName)
                .keySchema(
                    KeySchemaElement.builder().attributeName("ID").keyType(KeyType.HASH).build())
                .attributeDefinitions(
                    AttributeDefinition.builder()
                        .attributeName("ID")
                        .attributeType(ScalarAttributeType.S)
                        .build())
                .provisionedThroughput(
                    ProvisionedThroughput.builder()
                        .readCapacityUnits(1L)
                        .writeCapacityUnits(1L)
                        .build())
                .build())
        .get();

    // Put item
    client
        .putItem(
            PutItemRequest.builder()
                .tableName(tableName)
                .item(
                    java.util.Map.of(
                        "ID", AttributeValue.builder().s("123").build(),
                        "Name", AttributeValue.builder().s("test-value").build()))
                .build())
        .get();

    // Get item
    GetItemResponse getResult =
        client
            .getItem(
                GetItemRequest.builder()
                    .tableName(tableName)
                    .key(java.util.Map.of("ID", AttributeValue.builder().s("123").build()))
                    .build())
            .get();

    assertNotNull("Should get item back", getResult.item());
    assertEquals("123", getResult.item().get("ID").s());
    assertEquals("test-value", getResult.item().get("Name").s());

    // Delete item
    client
        .deleteItem(
            DeleteItemRequest.builder()
                .tableName(tableName)
                .key(java.util.Map.of("ID", AttributeValue.builder().s("123").build()))
                .build())
        .get();

    // Clean up
    client.deleteTable(DeleteTableRequest.builder().tableName(tableName).build()).get();

    wrapper.close();
  }

  @Test
  public void testAccessToAlternatorEndpointProvider() throws Exception {
    AlternatorDynamoDbAsyncClientWrapper client = buildClient();

    AlternatorEndpointProvider provider = client.getAlternatorEndpointProvider();
    assertNotNull("Should be able to access endpoint provider", provider);

    // Test that wrapper exposes live nodes directly
    List<URI> nodes = client.getLiveNodes();
    assertNotNull("Should be able to access live nodes from wrapper", nodes);

    client.close();
  }

  @Test
  public void testClientWithCompressionEnabled() throws Exception {
    AlternatorDynamoDbAsyncClientWrapper wrapper =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(seedUri)
            .credentialsProvider(credentialsProvider)
            .withDatacenter(datacenter)
            .withCompressionAlgorithm(RequestCompressionAlgorithm.GZIP)
            .withMinCompressionSizeBytes(512)
            .buildWithAlternatorAPI();

    // Verify client is functional with compression enabled
    List<URI> nodes = wrapper.getLiveNodes();
    assertFalse("Should have at least one node", nodes.isEmpty());

    // Perform a simple async operation to verify compression doesn't break requests
    try {
      wrapper.getClient().listTables(ListTablesRequest.builder().build()).get();
    } catch (Exception e) {
      // ListTables should work with compression enabled
      fail("Async ListTables should succeed with compression enabled: " + e.getMessage());
    }

    wrapper.close();
  }

  @Test
  public void testCompressionWithLargePayload() throws Exception {
    // Track if Content-Encoding: gzip header was seen
    AtomicBoolean compressionHeaderSeen = new AtomicBoolean(false);

    ExecutionInterceptor compressionVerifier =
        new ExecutionInterceptor() {
          @Override
          public void beforeTransmission(
              Context.BeforeTransmission context, ExecutionAttributes executionAttributes) {
            context
                .httpRequest()
                .firstMatchingHeader("Content-Encoding")
                .ifPresent(
                    value -> {
                      if (value.contains("gzip")) {
                        compressionHeaderSeen.set(true);
                      }
                    });
          }
        };

    // Build client with the compression verifier interceptor
    ClientOverrideConfiguration overrideConfig =
        ClientOverrideConfiguration.builder().addExecutionInterceptor(compressionVerifier).build();

    DynamoDbAsyncClient client =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(seedUri)
            .credentialsProvider(credentialsProvider)
            .overrideConfiguration(overrideConfig)
            .withCompressionAlgorithm(RequestCompressionAlgorithm.GZIP)
            .withMinCompressionSizeBytes(100) // Low threshold to ensure compression kicks in
            .build();

    // Create a large payload that should trigger compression (> 100 bytes threshold)
    StringBuilder largeValue = new StringBuilder();
    for (int i = 0; i < 100; i++) {
      largeValue.append("This is a test value that should be compressed. ");
    }

    // Perform an async operation with the large payload
    try {
      client
          .putItem(
              PutItemRequest.builder()
                  .tableName("nonexistent_table_for_compression_test")
                  .item(
                      java.util.Map.of(
                          "ID", AttributeValue.builder().s("compression-test").build(),
                          "LargeData", AttributeValue.builder().s(largeValue.toString()).build()))
                  .build())
          .get();
    } catch (Exception e) {
      // Table doesn't exist - that's expected, we just want to verify the request was compressed
    }

    assertTrue(
        "Content-Encoding: gzip header should be present for large payloads",
        compressionHeaderSeen.get());

    client.close();
  }

  @Test
  public void testNoCompressionForSmallPayload() throws Exception {
    // Track if Content-Encoding: gzip header was seen
    AtomicBoolean compressionHeaderSeen = new AtomicBoolean(false);

    ExecutionInterceptor compressionVerifier =
        new ExecutionInterceptor() {
          @Override
          public void beforeTransmission(
              Context.BeforeTransmission context, ExecutionAttributes executionAttributes) {
            context
                .httpRequest()
                .firstMatchingHeader("Content-Encoding")
                .ifPresent(
                    value -> {
                      if (value.contains("gzip")) {
                        compressionHeaderSeen.set(true);
                      }
                    });
          }
        };

    ClientOverrideConfiguration overrideConfig =
        ClientOverrideConfiguration.builder().addExecutionInterceptor(compressionVerifier).build();

    DynamoDbAsyncClient client =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(seedUri)
            .credentialsProvider(credentialsProvider)
            .overrideConfiguration(overrideConfig)
            .withCompressionAlgorithm(RequestCompressionAlgorithm.GZIP)
            .withMinCompressionSizeBytes(10000) // High threshold - small requests won't compress
            .build();

    // Perform a small async operation (listTables request is small)
    try {
      client.listTables(ListTablesRequest.builder().build()).get();
    } catch (Exception e) {
      // Ignore errors - we just want to check the header
    }

    assertFalse(
        "Content-Encoding: gzip header should NOT be present for small payloads",
        compressionHeaderSeen.get());

    client.close();
  }

  @Test
  public void testHeadersOptimizationFiltersHeaders() throws Exception {
    // Track which headers are seen in the request
    Set<String> seenHeaders = new HashSet<>();

    ExecutionInterceptor headerTracker =
        new ExecutionInterceptor() {
          @Override
          public void beforeTransmission(
              Context.BeforeTransmission context, ExecutionAttributes executionAttributes) {
            seenHeaders.addAll(context.httpRequest().headers().keySet());
          }
        };

    ClientOverrideConfiguration overrideConfig =
        ClientOverrideConfiguration.builder().addExecutionInterceptor(headerTracker).build();

    DynamoDbAsyncClient client =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(seedUri)
            .credentialsProvider(credentialsProvider)
            .overrideConfiguration(overrideConfig)
            .withOptimizeHeaders(true)
            .build();

    // Perform an async operation
    try {
      client.listTables(ListTablesRequest.builder().build()).get();
    } catch (Exception e) {
      // Ignore errors - we just want to check the headers
    }

    // Required headers should be present
    assertTrue("Host header should be present", seenHeaders.contains("Host"));
    assertTrue("X-Amz-Target header should be present", seenHeaders.contains("X-Amz-Target"));
    assertTrue("Content-Type header should be present", seenHeaders.contains("Content-Type"));
    assertTrue("Authorization header should be present", seenHeaders.contains("Authorization"));
    assertTrue("X-Amz-Date header should be present", seenHeaders.contains("X-Amz-Date"));

    // Note: Header filtering happens at HTTP client level (after SDK interceptors run).
    // SDK interceptors see unfiltered headers; actual wire traffic is filtered.
    // The filtering is verified in HeadersFilteringSdkAsyncHttpClientTest.

    client.close();
  }

  @Test
  public void testHeadersOptimizationWithCustomWhitelist() throws Exception {
    // Track which headers are seen in the request
    Set<String> seenHeaders = new HashSet<>();

    ExecutionInterceptor headerTracker =
        new ExecutionInterceptor() {
          @Override
          public void beforeTransmission(
              Context.BeforeTransmission context, ExecutionAttributes executionAttributes) {
            seenHeaders.addAll(context.httpRequest().headers().keySet());
          }
        };

    // Custom whitelist that includes User-Agent but excludes some default headers
    Set<String> customWhitelist =
        new HashSet<>(
            java.util.Arrays.asList(
                "Host",
                "X-Amz-Target",
                "Content-Type",
                "Content-Length",
                "Accept-Encoding", // Required header
                "Authorization",
                "X-Amz-Date",
                "User-Agent" // Include User-Agent in custom whitelist
                ));

    ClientOverrideConfiguration overrideConfig =
        ClientOverrideConfiguration.builder().addExecutionInterceptor(headerTracker).build();

    DynamoDbAsyncClient client =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(seedUri)
            .credentialsProvider(credentialsProvider)
            .overrideConfiguration(overrideConfig)
            .withOptimizeHeaders(true)
            .withHeadersWhitelist(customWhitelist)
            .build();

    // Perform an async operation
    try {
      client.listTables(ListTablesRequest.builder().build()).get();
    } catch (Exception e) {
      // Ignore errors - we just want to check the headers
    }

    // Custom whitelist headers should be present
    assertTrue(
        "User-Agent should be present (custom whitelist)", seenHeaders.contains("User-Agent"));
    assertTrue("Host header should be present", seenHeaders.contains("Host"));

    // Headers not in custom whitelist should be filtered
    assertFalse(
        "X-Amz-Sdk-Invocation-Id should be filtered",
        seenHeaders.contains("X-Amz-Sdk-Invocation-Id"));

    client.close();
  }

  @Test
  public void testHeadersOptimizationDisabledByDefault() throws Exception {
    // Track which headers are seen in the request
    Set<String> seenHeaders = new HashSet<>();

    ExecutionInterceptor headerTracker =
        new ExecutionInterceptor() {
          @Override
          public void beforeTransmission(
              Context.BeforeTransmission context, ExecutionAttributes executionAttributes) {
            seenHeaders.addAll(context.httpRequest().headers().keySet());
          }
        };

    // Don't enable headers optimization (use default settings)
    ClientOverrideConfiguration overrideConfig =
        ClientOverrideConfiguration.builder().addExecutionInterceptor(headerTracker).build();

    DynamoDbAsyncClient client =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(seedUri)
            .credentialsProvider(credentialsProvider)
            .overrideConfiguration(overrideConfig)
            .build();

    // Perform an async operation
    try {
      client.listTables(ListTablesRequest.builder().build()).get();
    } catch (Exception e) {
      // Ignore errors - we just want to check the headers
    }

    // When disabled, SDK metadata headers should still be present
    assertTrue(
        "User-Agent should be present when optimization disabled",
        seenHeaders.contains("User-Agent"));

    client.close();
  }

  @Test
  public void testHeadersOptimizationWithCompression() throws Exception {
    // Track headers and compression
    Set<String> seenHeaders = new HashSet<>();
    AtomicBoolean compressionHeaderSeen = new AtomicBoolean(false);

    ExecutionInterceptor tracker =
        new ExecutionInterceptor() {
          @Override
          public void beforeTransmission(
              Context.BeforeTransmission context, ExecutionAttributes executionAttributes) {
            seenHeaders.addAll(context.httpRequest().headers().keySet());
            context
                .httpRequest()
                .firstMatchingHeader("Content-Encoding")
                .ifPresent(
                    value -> {
                      if (value.contains("gzip")) {
                        compressionHeaderSeen.set(true);
                      }
                    });
          }
        };

    // Enable both headers optimization and compression
    ClientOverrideConfiguration overrideConfig =
        ClientOverrideConfiguration.builder().addExecutionInterceptor(tracker).build();

    DynamoDbAsyncClient client =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(seedUri)
            .credentialsProvider(credentialsProvider)
            .overrideConfiguration(overrideConfig)
            .withOptimizeHeaders(true)
            .withCompressionAlgorithm(RequestCompressionAlgorithm.GZIP)
            .withMinCompressionSizeBytes(100)
            .build();

    // Create a large payload that should trigger compression
    StringBuilder largeValue = new StringBuilder();
    for (int i = 0; i < 100; i++) {
      largeValue.append("This is a test value that should be compressed. ");
    }

    // Perform an async operation with the large payload
    try {
      client
          .putItem(
              PutItemRequest.builder()
                  .tableName("nonexistent_table_for_test")
                  .item(
                      java.util.Map.of(
                          "ID", AttributeValue.builder().s("test").build(),
                          "LargeData", AttributeValue.builder().s(largeValue.toString()).build()))
                  .build())
          .get();
    } catch (Exception e) {
      // Expected - table doesn't exist
    }

    // Verify both features work together
    assertTrue(
        "Content-Encoding should be present for compression",
        seenHeaders.contains("Content-Encoding"));
    assertTrue("Compression should have been applied", compressionHeaderSeen.get());

    // Note: Header filtering happens at HTTP client level (after SDK interceptors run).
    // SDK interceptors see unfiltered headers; actual wire traffic is filtered.

    client.close();
  }
}
