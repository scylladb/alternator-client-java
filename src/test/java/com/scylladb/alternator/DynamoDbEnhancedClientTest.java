package com.scylladb.alternator;

import static org.junit.Assert.*;

import com.scylladb.alternator.internal.AlternatorLiveNodes;
import com.scylladb.alternator.queryplan.BasicQueryPlanInterceptor;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedAsyncClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/**
 * Unit tests verifying that DynamoDB Enhanced Client works correctly with Alternator load
 * balancing.
 *
 * <p>The enhanced client wraps the standard DynamoDbClient, so load balancing should work
 * transparently when the underlying client is created with AlternatorDynamoDbClient.builder().
 */
public class DynamoDbEnhancedClientTest {

  /** Simple model class for testing. */
  @DynamoDbBean
  public static class TestItem {
    private String id;
    private String data;

    public TestItem() {}

    public TestItem(String id, String data) {
      this.id = id;
      this.data = data;
    }

    @DynamoDbPartitionKey
    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }

    public String getData() {
      return data;
    }

    public void setData(String data) {
      this.data = data;
    }
  }

  private static final StaticCredentialsProvider TEST_CREDENTIALS =
      StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"));

  @Test
  public void testEnhancedClientCanBeBuiltFromAlternatorClient() throws URISyntaxException {
    List<URI> nodes =
        Arrays.asList(
            new URI("http://node1.example.com:8000"),
            new URI("http://node2.example.com:8000"),
            new URI("http://node3.example.com:8000"));

    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(nodes, "http", 8000, "", "");
    BasicQueryPlanInterceptor interceptor = new BasicQueryPlanInterceptor(liveNodes);

    ClientOverrideConfiguration overrideConfig =
        ClientOverrideConfiguration.builder().addExecutionInterceptor(interceptor).build();

    DynamoDbClient client =
        DynamoDbClient.builder()
            .endpointOverride(nodes.get(0))
            .overrideConfiguration(overrideConfig)
            .credentialsProvider(TEST_CREDENTIALS)
            .region(Region.US_EAST_1)
            .build();

    // Build enhanced client from load-balanced client
    DynamoDbEnhancedClient enhancedClient =
        DynamoDbEnhancedClient.builder().dynamoDbClient(client).build();

    assertNotNull("Enhanced client should be created successfully", enhancedClient);

    // Verify we can get a table reference
    DynamoDbTable<TestItem> table =
        enhancedClient.table("test_table", TableSchema.fromBean(TestItem.class));
    assertNotNull("Table reference should be created", table);

    client.close();
  }

  @Test
  public void testEnhancedAsyncClientCanBeBuiltFromAlternatorAsyncClient()
      throws URISyntaxException {
    List<URI> nodes =
        Arrays.asList(
            new URI("http://node1.example.com:8000"),
            new URI("http://node2.example.com:8000"),
            new URI("http://node3.example.com:8000"));

    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(nodes, "http", 8000, "", "");
    BasicQueryPlanInterceptor interceptor = new BasicQueryPlanInterceptor(liveNodes);

    ClientOverrideConfiguration overrideConfig =
        ClientOverrideConfiguration.builder().addExecutionInterceptor(interceptor).build();

    DynamoDbAsyncClient asyncClient =
        DynamoDbAsyncClient.builder()
            .endpointOverride(nodes.get(0))
            .overrideConfiguration(overrideConfig)
            .credentialsProvider(TEST_CREDENTIALS)
            .region(Region.US_EAST_1)
            .build();

    // Build enhanced async client from load-balanced async client
    DynamoDbEnhancedAsyncClient enhancedAsyncClient =
        DynamoDbEnhancedAsyncClient.builder().dynamoDbClient(asyncClient).build();

    assertNotNull("Enhanced async client should be created successfully", enhancedAsyncClient);

    asyncClient.close();
  }

  @Test
  public void testLoadBalancingWorksWithEnhancedClient() throws URISyntaxException {
    List<URI> nodes =
        Arrays.asList(
            new URI("http://node1.example.com:8000"),
            new URI("http://node2.example.com:8000"),
            new URI("http://node3.example.com:8000"));

    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(nodes, "http", 8000, "", "");
    BasicQueryPlanInterceptor interceptor = new BasicQueryPlanInterceptor(liveNodes);

    // Track which hosts are being targeted
    Set<String> targetedHosts = Collections.synchronizedSet(new HashSet<>());
    AtomicInteger requestCount = new AtomicInteger(0);

    ExecutionInterceptor hostTracker =
        new ExecutionInterceptor() {
          @Override
          public void beforeTransmission(
              Context.BeforeTransmission context, ExecutionAttributes executionAttributes) {
            String host = context.httpRequest().host();
            targetedHosts.add(host);
            requestCount.incrementAndGet();
          }
        };

    ClientOverrideConfiguration overrideConfig =
        ClientOverrideConfiguration.builder()
            .addExecutionInterceptor(interceptor)
            .addExecutionInterceptor(hostTracker)
            .build();

    DynamoDbClient client =
        DynamoDbClient.builder()
            .endpointOverride(nodes.get(0))
            .credentialsProvider(TEST_CREDENTIALS)
            .overrideConfiguration(overrideConfig)
            .region(Region.US_EAST_1)
            .build();

    DynamoDbEnhancedClient enhancedClient =
        DynamoDbEnhancedClient.builder().dynamoDbClient(client).build();

    DynamoDbTable<TestItem> table =
        enhancedClient.table("test_table", TableSchema.fromBean(TestItem.class));

    // Make multiple requests - with random distribution across 3 nodes
    for (int i = 0; i < 30; i++) {
      final int index = i;
      try {
        table.getItem(r -> r.key(k -> k.partitionValue("item-" + index)));
      } catch (Exception e) {
        // Expected - no actual server, but the request was sent
      }
    }

    // Verify that multiple hosts were targeted (random distribution)
    // Note: SDK may retry failed requests, so we verify distribution rather than exact count
    assertTrue("Should have made at least 30 requests", requestCount.get() >= 30);
    // With random distribution over 30 requests across 3 nodes, we should hit all nodes
    assertEquals(
        "Random distribution should hit all 3 nodes over 30+ requests", 3, targetedHosts.size());
    assertTrue("Should have targeted node1", targetedHosts.contains("node1.example.com"));
    assertTrue("Should have targeted node2", targetedHosts.contains("node2.example.com"));
    assertTrue("Should have targeted node3", targetedHosts.contains("node3.example.com"));

    client.close();
  }

  @Test
  public void testLoadBalancingWorksWithEnhancedAsyncClient() throws URISyntaxException {
    List<URI> nodes =
        Arrays.asList(
            new URI("http://node1.example.com:8000"),
            new URI("http://node2.example.com:8000"),
            new URI("http://node3.example.com:8000"));

    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(nodes, "http", 8000, "", "");
    BasicQueryPlanInterceptor interceptor = new BasicQueryPlanInterceptor(liveNodes);

    // Track which hosts are being targeted
    Set<String> targetedHosts = Collections.synchronizedSet(new HashSet<>());
    AtomicInteger requestCount = new AtomicInteger(0);

    ExecutionInterceptor hostTracker =
        new ExecutionInterceptor() {
          @Override
          public void beforeTransmission(
              Context.BeforeTransmission context, ExecutionAttributes executionAttributes) {
            String host = context.httpRequest().host();
            targetedHosts.add(host);
            requestCount.incrementAndGet();
          }
        };

    ClientOverrideConfiguration overrideConfig =
        ClientOverrideConfiguration.builder()
            .addExecutionInterceptor(interceptor)
            .addExecutionInterceptor(hostTracker)
            .build();

    DynamoDbAsyncClient asyncClient =
        DynamoDbAsyncClient.builder()
            .endpointOverride(nodes.get(0))
            .credentialsProvider(TEST_CREDENTIALS)
            .overrideConfiguration(overrideConfig)
            .region(Region.US_EAST_1)
            .build();

    DynamoDbEnhancedAsyncClient enhancedAsyncClient =
        DynamoDbEnhancedAsyncClient.builder().dynamoDbClient(asyncClient).build();

    software.amazon.awssdk.enhanced.dynamodb.DynamoDbAsyncTable<TestItem> table =
        enhancedAsyncClient.table("test_table", TableSchema.fromBean(TestItem.class));

    // Make multiple async requests - with random distribution across 3 nodes
    List<java.util.concurrent.CompletableFuture<?>> futures = new ArrayList<>();
    for (int i = 0; i < 30; i++) {
      final int index = i;
      futures.add(
          table
              .getItem(r -> r.key(k -> k.partitionValue("item-" + index)))
              .exceptionally(e -> null)); // Ignore errors - no actual server
    }

    // Wait for all requests to complete (or fail)
    java.util.concurrent.CompletableFuture.allOf(
            futures.toArray(new java.util.concurrent.CompletableFuture[0]))
        .join();

    // Verify that multiple hosts were targeted (random distribution)
    // Note: SDK may retry failed requests, so we verify distribution rather than exact count
    assertTrue("Should have made at least 30 requests", requestCount.get() >= 30);
    // With random distribution over 30 requests across 3 nodes, we should hit all nodes
    assertEquals(
        "Random distribution should hit all 3 nodes over 30+ requests", 3, targetedHosts.size());
    assertTrue("Should have targeted node1", targetedHosts.contains("node1.example.com"));
    assertTrue("Should have targeted node2", targetedHosts.contains("node2.example.com"));
    assertTrue("Should have targeted node3", targetedHosts.contains("node3.example.com"));

    asyncClient.close();
  }

  @Test
  public void testEnhancedClientWithQueryPlanInterceptor() throws URISyntaxException {
    List<URI> nodes =
        Arrays.asList(
            new URI("http://node1.example.com:8000"),
            new URI("http://node2.example.com:8000"),
            new URI("http://node3.example.com:8000"));

    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(nodes, "http", 8000, "", "");
    BasicQueryPlanInterceptor interceptor = new BasicQueryPlanInterceptor(liveNodes);

    // Track hosts
    Set<String> targetedHosts = Collections.synchronizedSet(new HashSet<>());

    ExecutionInterceptor hostTracker =
        new ExecutionInterceptor() {
          @Override
          public void beforeTransmission(
              Context.BeforeTransmission context, ExecutionAttributes executionAttributes) {
            targetedHosts.add(context.httpRequest().host());
          }
        };

    ClientOverrideConfiguration overrideConfig =
        ClientOverrideConfiguration.builder()
            .addExecutionInterceptor(interceptor)
            .addExecutionInterceptor(hostTracker)
            .build();

    // Using BasicQueryPlanInterceptor for load balancing
    DynamoDbClient client =
        DynamoDbClient.builder()
            .endpointOverride(nodes.get(0))
            .credentialsProvider(TEST_CREDENTIALS)
            .overrideConfiguration(overrideConfig)
            .region(Region.US_EAST_1)
            .build();

    // Build enhanced client - this is the pattern users should follow
    DynamoDbEnhancedClient enhancedClient =
        DynamoDbEnhancedClient.builder().dynamoDbClient(client).build();

    DynamoDbTable<TestItem> table =
        enhancedClient.table("test_table", TableSchema.fromBean(TestItem.class));

    // Multiple operations should hit different nodes
    for (int i = 0; i < 9; i++) {
      try {
        table.putItem(new TestItem("id-" + i, "data-" + i));
      } catch (Exception e) {
        // Expected - no actual server
      }
    }

    // All three nodes should have been targeted
    assertEquals("Should target all 3 nodes", 3, targetedHosts.size());

    client.close();
  }

  @Test
  public void testTableSchemaFromBeanWorks() {
    // Verify that the @DynamoDbBean annotation works correctly
    TableSchema<TestItem> schema = TableSchema.fromBean(TestItem.class);

    assertNotNull("Schema should be created", schema);
    assertEquals(
        "Should have partition key 'id'", "id", schema.tableMetadata().primaryPartitionKey());
  }
}
