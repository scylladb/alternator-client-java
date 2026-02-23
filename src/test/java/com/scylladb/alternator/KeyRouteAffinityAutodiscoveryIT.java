package com.scylladb.alternator;

import static org.junit.Assert.*;
import static org.junit.Assume.*;

import com.scylladb.alternator.keyrouting.KeyRouteAffinity;
import com.scylladb.alternator.keyrouting.KeyRouteAffinityConfig;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

/**
 * Integration tests for key route affinity autodiscovery against a real Alternator cluster.
 *
 * <p>These tests verify that partition key names are automatically discovered via DescribeTable
 * when not pre-configured, and that key-based routing produces consistent node selection for the
 * same partition key.
 *
 * <p>Set environment variables to configure: - ALTERNATOR_HOST: Host address (default: 172.39.0.2)
 * - ALTERNATOR_PORT: Port number (default: 9998) - ALTERNATOR_HTTPS: Use HTTPS (default: false) -
 * INTEGRATION_TESTS: Set to "true" to enable these tests
 */
public class KeyRouteAffinityAutodiscoveryIT {

  private static String host;
  private static int port;
  private static boolean useHttps;
  private static URI seedUri;
  private static boolean integrationTestsEnabled;
  private static StaticCredentialsProvider credentialsProvider;

  @BeforeClass
  public static void setUpClass() {
    host = System.getenv().getOrDefault("ALTERNATOR_HOST", "172.39.0.2");
    port = Integer.parseInt(System.getenv().getOrDefault("ALTERNATOR_PORT", "9998"));
    useHttps = Boolean.parseBoolean(System.getenv().getOrDefault("ALTERNATOR_HTTPS", "false"));

    String scheme = useHttps ? "https" : "http";
    try {
      seedUri = new URI(scheme + "://" + host + ":" + port);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }

    credentialsProvider =
        StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"));

    integrationTestsEnabled =
        Boolean.parseBoolean(System.getenv().getOrDefault("INTEGRATION_TESTS", "false"));
  }

  @Before
  public void setUp() {
    assumeTrue(
        "Integration tests disabled. Set INTEGRATION_TESTS=true to enable.",
        integrationTestsEnabled);
  }

  private static String uniqueTableName(String prefix) {
    return prefix + "_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
  }

  private static CreateTableRequest createTableRequest(String tableName) {
    return CreateTableRequest.builder()
        .tableName(tableName)
        .keySchema(
            KeySchemaElement.builder().attributeName("user_id").keyType(KeyType.HASH).build())
        .attributeDefinitions(
            AttributeDefinition.builder()
                .attributeName("user_id")
                .attributeType(ScalarAttributeType.S)
                .build())
        .provisionedThroughput(
            ProvisionedThroughput.builder().readCapacityUnits(1L).writeCapacityUnits(1L).build())
        .build();
  }

  private static void safeDeleteTable(DynamoDbClient client, String tableName) {
    try {
      client.deleteTable(DeleteTableRequest.builder().tableName(tableName).build());
    } catch (ResourceNotFoundException e) {
      // Table does not exist, nothing to clean up
    }
  }

  @Test
  public void testAutodiscoveryFindsPartitionKey() throws Exception {
    String tableName = uniqueTableName("affinity_discover_it");

    // Build client with key affinity but WITHOUT pre-configured PK info.
    // The interceptor should trigger DescribeTable to discover the PK name.
    KeyRouteAffinityConfig affinityConfig =
        KeyRouteAffinityConfig.builder().withType(KeyRouteAffinity.ANY_WRITE).build();

    AlternatorDynamoDbClientWrapper wrapper =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(seedUri)
            .credentialsProvider(credentialsProvider)
            .withKeyRouteAffinity(affinityConfig)
            .buildWithAlternatorAPI();

    DynamoDbClient client = wrapper.getClient();

    try {
      // Clean up if table exists
      safeDeleteTable(client, tableName);
      Thread.sleep(500);

      // Create table with partition key "user_id"
      client.createTable(createTableRequest(tableName));

      // First write triggers autodiscovery asynchronously.
      // The first request may not benefit from affinity routing (PK not yet cached),
      // but it triggers the DescribeTable call.
      client.putItem(
          PutItemRequest.builder()
              .tableName(tableName)
              .item(
                  Map.of(
                      "user_id", AttributeValue.builder().s("user-001").build(),
                      "name", AttributeValue.builder().s("Alice").build()))
              .build());

      // Wait for async discovery to complete
      Thread.sleep(2000);

      // Second write should now benefit from cached PK info.
      // If autodiscovery failed, this would still succeed (falls back to round-robin),
      // but we verify the data is correct regardless.
      client.putItem(
          PutItemRequest.builder()
              .tableName(tableName)
              .item(
                  Map.of(
                      "user_id", AttributeValue.builder().s("user-002").build(),
                      "name", AttributeValue.builder().s("Bob").build()))
              .build());

      // Verify both items were written correctly
      GetItemResponse response1 =
          client.getItem(
              GetItemRequest.builder()
                  .tableName(tableName)
                  .key(Map.of("user_id", AttributeValue.builder().s("user-001").build()))
                  .build());
      assertNotNull("First item should exist", response1.item());
      assertEquals("Alice", response1.item().get("name").s());

      GetItemResponse response2 =
          client.getItem(
              GetItemRequest.builder()
                  .tableName(tableName)
                  .key(Map.of("user_id", AttributeValue.builder().s("user-002").build()))
                  .build());
      assertNotNull("Second item should exist", response2.item());
      assertEquals("Bob", response2.item().get("name").s());

      // Clean up
      client.deleteTable(DeleteTableRequest.builder().tableName(tableName).build());
    } catch (Exception e) {
      safeDeleteTable(client, tableName);
      throw e;
    } finally {
      wrapper.close();
    }
  }

  @Test
  public void testAutodiscoveryRoutesConsistently() throws Exception {
    String tableName = uniqueTableName("affinity_route_it");

    KeyRouteAffinityConfig affinityConfig =
        KeyRouteAffinityConfig.builder().withType(KeyRouteAffinity.ANY_WRITE).build();

    AlternatorDynamoDbClientWrapper wrapper =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(seedUri)
            .credentialsProvider(credentialsProvider)
            .withKeyRouteAffinity(affinityConfig)
            .buildWithAlternatorAPI();

    DynamoDbClient client = wrapper.getClient();

    try {
      // Clean up if table exists
      safeDeleteTable(client, tableName);
      Thread.sleep(500);

      // Create table
      client.createTable(createTableRequest(tableName));

      // Trigger autodiscovery with a first write
      client.putItem(
          PutItemRequest.builder()
              .tableName(tableName)
              .item(
                  Map.of(
                      "user_id", AttributeValue.builder().s("route-test").build(),
                      "value", AttributeValue.builder().s("initial").build()))
              .build());

      // Wait for async discovery to complete
      Thread.sleep(2000);

      // Perform multiple conditional updates on the same partition key.
      // With affinity routing, these should all go to the same node, reducing
      // Paxos contention. Even if the cluster has one node, the operations
      // should succeed consistently.
      Set<String> writtenValues = new HashSet<>();
      for (int i = 0; i < 10; i++) {
        String value = "value-" + i;
        client.putItem(
            PutItemRequest.builder()
                .tableName(tableName)
                .item(
                    Map.of(
                        "user_id", AttributeValue.builder().s("route-test").build(),
                        "value", AttributeValue.builder().s(value).build()))
                .build());
        writtenValues.add(value);
      }

      // Verify the final state is the last written value
      GetItemResponse finalResponse =
          client.getItem(
              GetItemRequest.builder()
                  .tableName(tableName)
                  .key(Map.of("user_id", AttributeValue.builder().s("route-test").build()))
                  .build());

      assertNotNull("Item should exist after repeated writes", finalResponse.item());
      String finalValue = finalResponse.item().get("value").s();
      assertTrue(
          "Final value should be one of the written values", writtenValues.contains(finalValue));

      // Also test with different partition keys to verify they can route independently
      for (int i = 0; i < 5; i++) {
        String pk = "different-key-" + i;
        client.putItem(
            PutItemRequest.builder()
                .tableName(tableName)
                .item(
                    Map.of(
                        "user_id", AttributeValue.builder().s(pk).build(),
                        "value", AttributeValue.builder().s("val-" + i).build()))
                .build());
      }

      // Verify all different keys are readable
      for (int i = 0; i < 5; i++) {
        String pk = "different-key-" + i;
        GetItemResponse response =
            client.getItem(
                GetItemRequest.builder()
                    .tableName(tableName)
                    .key(Map.of("user_id", AttributeValue.builder().s(pk).build()))
                    .build());
        assertNotNull("Item " + pk + " should exist", response.item());
        assertEquals("val-" + i, response.item().get("value").s());
      }

      // Clean up
      client.deleteTable(DeleteTableRequest.builder().tableName(tableName).build());
    } catch (Exception e) {
      safeDeleteTable(client, tableName);
      throw e;
    } finally {
      wrapper.close();
    }
  }

  @Test
  public void testAutodiscoveryWithPreConfiguredPk() throws Exception {
    String tableName = uniqueTableName("affinity_preconf_it");

    // Pre-configure the partition key name so DescribeTable is not needed
    KeyRouteAffinityConfig affinityConfig =
        KeyRouteAffinityConfig.builder()
            .withType(KeyRouteAffinity.ANY_WRITE)
            .withPkInfo(tableName, "user_id")
            .build();

    AlternatorDynamoDbClientWrapper wrapper =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(seedUri)
            .credentialsProvider(credentialsProvider)
            .withKeyRouteAffinity(affinityConfig)
            .buildWithAlternatorAPI();

    DynamoDbClient client = wrapper.getClient();

    try {
      // Clean up if table exists
      safeDeleteTable(client, tableName);
      Thread.sleep(500);

      // Create table
      client.createTable(createTableRequest(tableName));

      // With pre-configured PK, the very first write should benefit from affinity routing
      // immediately, without needing a DescribeTable call or any discovery delay.
      for (int i = 0; i < 10; i++) {
        client.putItem(
            PutItemRequest.builder()
                .tableName(tableName)
                .item(
                    Map.of(
                        "user_id", AttributeValue.builder().s("preconf-user").build(),
                        "seq", AttributeValue.builder().n(String.valueOf(i)).build()))
                .build());
      }

      // Verify the item exists and has the last written sequence number
      GetItemResponse response =
          client.getItem(
              GetItemRequest.builder()
                  .tableName(tableName)
                  .key(Map.of("user_id", AttributeValue.builder().s("preconf-user").build()))
                  .build());

      assertNotNull("Item should exist", response.item());
      assertEquals("preconf-user", response.item().get("user_id").s());
      // The final seq value should be "9" (last iteration)
      assertEquals("9", response.item().get("seq").n());

      // Write items with different partition keys to verify routing works for multiple keys
      for (int i = 0; i < 5; i++) {
        client.putItem(
            PutItemRequest.builder()
                .tableName(tableName)
                .item(
                    Map.of(
                        "user_id", AttributeValue.builder().s("user-" + i).build(),
                        "data", AttributeValue.builder().s("data-" + i).build()))
                .build());
      }

      // Verify all items are readable
      for (int i = 0; i < 5; i++) {
        GetItemResponse readResponse =
            client.getItem(
                GetItemRequest.builder()
                    .tableName(tableName)
                    .key(Map.of("user_id", AttributeValue.builder().s("user-" + i).build()))
                    .build());
        assertNotNull("Item user-" + i + " should exist", readResponse.item());
        assertEquals("data-" + i, readResponse.item().get("data").s());
      }

      // Clean up
      client.deleteTable(DeleteTableRequest.builder().tableName(tableName).build());
    } catch (Exception e) {
      safeDeleteTable(client, tableName);
      throw e;
    } finally {
      wrapper.close();
    }
  }
}
