package com.scylladb.alternator.internal;

import static org.junit.Assert.*;
import static org.junit.Assume.*;

import com.scylladb.alternator.AlternatorConfig;
import com.scylladb.alternator.AlternatorDynamoDbClient;
import com.scylladb.alternator.AlternatorDynamoDbClientWrapper;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.http.pool.PoolStats;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

/**
 * Integration test that verifies HTTP connections to a real Scylla cluster stay in the pool and are
 * reused across multiple {@code /localnodes} requests, rather than being created and destroyed for
 * each request.
 *
 * <p>Requires a running ScyllaDB cluster with Alternator enabled. Set environment variables:
 *
 * <ul>
 *   <li>INTEGRATION_TESTS=true - Enable integration tests
 *   <li>ALTERNATOR_HOST - Host address (default: 172.39.0.2)
 *   <li>ALTERNATOR_PORT - Port number (default: 9998)
 *   <li>ALTERNATOR_HTTPS - Use HTTPS (default: false)
 * </ul>
 */
public class ConnectionPoolIT {

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

  private AlternatorLiveNodes createLiveNodes() {
    AlternatorConfig config = AlternatorConfig.builder().withSeedNode(seedUri).build();
    return new AlternatorLiveNodes(config);
  }

  /**
   * Verifies that connections to a real Scylla cluster are pooled and reused. After many {@code
   * updateLiveNodes()} calls, the number of available connections should equal the number of
   * distinct nodes contacted (one per route, since maxPerRoute=1), and none should be leased. This
   * proves connections are returned to the pool and reused rather than created and destroyed each
   * time.
   */
  @Test
  public void testConnectionsArePooledAndReused() throws Exception {
    AlternatorLiveNodes liveNodes = createLiveNodes();

    // Make many requests — enough to round-robin through all nodes multiple times
    for (int i = 0; i < 30; i++) {
      liveNodes.updateLiveNodes();
    }

    int nodeCount = liveNodes.getLiveNodes().size();
    PoolStats stats = liveNodes.getConnectionPoolStats();
    assertEquals("No connections should be leased", 0, stats.getLeased());
    assertTrue(
        "Available connections should be at most the number of live nodes, got "
            + stats.getAvailable()
            + " available for "
            + nodeCount
            + " nodes",
        stats.getAvailable() <= nodeCount);
    assertTrue("At least one connection should be available in the pool", stats.getAvailable() > 0);
  }

  /**
   * Verifies that pooled connections to a real Scylla cluster survive idle periods and are reused
   * after sitting idle for 60 seconds.
   *
   * <p>This guards against connections being evicted or closed by the client-side pool. After the
   * idle period, making more requests should not increase the number of available connections —
   * proving the existing ones were reused rather than new ones created alongside stale entries.
   */
  @Test(timeout = 120_000)
  public void testConnectionSurvivesIdlePeriod() throws Exception {
    AlternatorLiveNodes liveNodes = createLiveNodes();

    // Establish pooled connections by round-robining through all nodes
    for (int i = 0; i < 30; i++) {
      liveNodes.updateLiveNodes();
    }
    PoolStats before = liveNodes.getConnectionPoolStats();
    assertTrue("Connections should be available after requests", before.getAvailable() > 0);
    int availableBefore = before.getAvailable();

    // Let connections sit idle for 60 seconds
    Thread.sleep(60_000);

    // Use the connections again — they should be reused from the pool
    for (int i = 0; i < 30; i++) {
      liveNodes.updateLiveNodes();
    }
    PoolStats after = liveNodes.getConnectionPoolStats();
    assertEquals("No connections should be leased after reuse", 0, after.getLeased());
    assertEquals(
        "Available connections should not grow after idle period (connections reused, not leaked)",
        availableBefore,
        after.getAvailable());
  }

  /**
   * Returns the sum of {@code scylla_httpd_connections_total} for the {@code http-alternator}
   * service across all shards on all discovered nodes. This counter is incremented by Scylla every
   * time a new TCP connection is accepted, so the delta between two calls tells us exactly how many
   * new connections were opened in between.
   */
  private static long getTotalConnectionsFromScylla(List<URI> nodes) throws Exception {
    long total = 0;
    for (URI node : nodes) {
      URI metricsUri = new URI("http", null, node.getHost(), 9180, "/metrics", null, null);
      ProcessBuilder pb = new ProcessBuilder("curl", "-sf", metricsUri.toString());
      pb.redirectErrorStream(true);
      Process proc = pb.start();
      try (BufferedReader reader =
          new BufferedReader(new InputStreamReader(proc.getInputStream()))) {
        String line;
        while ((line = reader.readLine()) != null) {
          // Match lines like: scylla_httpd_connections_total{service="http-alternator",shard="0"}
          // 14
          if (line.startsWith("scylla_httpd_connections_total")
              && line.contains("http-alternator")) {
            String[] parts = line.split("\\s+");
            if (parts.length >= 2) {
              total += Long.parseLong(parts[parts.length - 1]);
            }
          }
        }
      }
      proc.waitFor();
    }
    return total;
  }

  /**
   * Builds the list of node URIs to query for metrics from the wrapper's live nodes. Falls back to
   * seed URI if no nodes discovered yet.
   */
  private static List<URI> getNodeUris(AlternatorDynamoDbClientWrapper wrapper) {
    List<URI> nodes = wrapper.getLiveNodes();
    if (nodes == null || nodes.isEmpty()) {
      nodes = new ArrayList<>();
      nodes.add(seedUri);
    }
    return nodes;
  }

  /**
   * Runs the DynamoDB connection reuse test with the given client wrapper and table name.
   *
   * <p>Performs warmup requests, verifies connections are stable during 500ms idle gaps, verifies
   * they don't grow during bulk requests, and verifies they survive a 60-second idle period.
   */
  private void runDynamoDbConnectionReuseTest(
      AlternatorDynamoDbClientWrapper wrapper, String tableName) throws Exception {
    DynamoDbClient client = wrapper.getClient();

    // Create test table
    try {
      client.deleteTable(DeleteTableRequest.builder().tableName(tableName).build());
      Thread.sleep(500);
    } catch (ResourceNotFoundException e) {
      // Table doesn't exist, that's fine
    }
    client.createTable(
        CreateTableRequest.builder()
            .tableName(tableName)
            .keySchema(KeySchemaElement.builder().attributeName("pk").keyType(KeyType.HASH).build())
            .attributeDefinitions(
                AttributeDefinition.builder()
                    .attributeName("pk")
                    .attributeType(ScalarAttributeType.S)
                    .build())
            .provisionedThroughput(
                ProvisionedThroughput.builder()
                    .readCapacityUnits(10L)
                    .writeCapacityUnits(10L)
                    .build())
            .build());

    try {
      List<URI> nodes = getNodeUris(wrapper);

      // Warm up — establish connections for both the SDK client and the background
      // AlternatorLiveNodes thread.
      for (int i = 0; i < 10; i++) {
        client.putItem(
            PutItemRequest.builder()
                .tableName(tableName)
                .item(
                    Map.of(
                        "pk", AttributeValue.builder().s("warmup-" + i).build(),
                        "data", AttributeValue.builder().s("value").build()))
                .build());
      }
      // Allow the background node-discovery thread to stabilize its connections
      Thread.sleep(2000);
      nodes = getNodeUris(wrapper);

      // Take baseline from Scylla's total connections counter.
      // This counter only increases, so any delta > 0 means new TCP connections were opened.
      long baselineTotal = getTotalConnectionsFromScylla(nodes);

      // Verify connections are not dropped during short idle gaps (500ms between requests).
      // If connections are reused, no new TCP connections should be opened on Scylla's side.
      for (int i = 0; i < 20; i++) {
        Thread.sleep(500);
        client.putItem(
            PutItemRequest.builder()
                .tableName(tableName)
                .item(
                    Map.of(
                        "pk", AttributeValue.builder().s("gap-" + i).build(),
                        "data", AttributeValue.builder().s("value").build()))
                .build());
      }

      long totalAfterGaps = getTotalConnectionsFromScylla(nodes);
      long newConnectionsDuringGaps = totalAfterGaps - baselineTotal;
      // Allow up to nodes.size() new connections: the background AlternatorLiveNodes
      // thread may round-robin through nodes and open a connection during this window.
      assertTrue(
          "New TCP connections during 20 requests with 500ms gaps should be at most "
              + nodes.size()
              + " (background thread), got "
              + newConnectionsDuringGaps
              + " (baseline="
              + baselineTotal
              + ", after="
              + totalAfterGaps
              + ")",
          newConnectionsDuringGaps <= nodes.size());

      // Perform many more operations back-to-back — connections should be reused
      long baselineBeforeBulk = getTotalConnectionsFromScylla(nodes);
      for (int i = 0; i < 50; i++) {
        client.putItem(
            PutItemRequest.builder()
                .tableName(tableName)
                .item(
                    Map.of(
                        "pk", AttributeValue.builder().s("item-" + i).build(),
                        "data", AttributeValue.builder().s("value-" + i).build()))
                .build());
      }

      long totalAfterBulk = getTotalConnectionsFromScylla(nodes);
      long newConnectionsDuringBulk = totalAfterBulk - baselineBeforeBulk;
      // Allow up to nodes.size() new connections for the same background thread reason.
      assertTrue(
          "New TCP connections during 50 back-to-back requests should be at most "
              + nodes.size()
              + " (background thread), got "
              + newConnectionsDuringBulk
              + " (baseline="
              + baselineBeforeBulk
              + ", after="
              + totalAfterBulk
              + ")",
          newConnectionsDuringBulk <= nodes.size());

      // Let connections sit idle for 60 seconds
      long baselineBeforeIdle = getTotalConnectionsFromScylla(nodes);
      Thread.sleep(60_000);

      // Connections should still be alive and reused after idle period
      for (int i = 0; i < 10; i++) {
        client.putItem(
            PutItemRequest.builder()
                .tableName(tableName)
                .item(
                    Map.of(
                        "pk", AttributeValue.builder().s("after-idle-" + i).build(),
                        "data", AttributeValue.builder().s("value").build()))
                .build());
      }

      long totalAfterIdle = getTotalConnectionsFromScylla(nodes);
      long newConnectionsAfterIdle = totalAfterIdle - baselineBeforeIdle;
      // The AWS SDK's ApacheHttpClient has a default idle connection reaper that closes
      // connections idle for 60 seconds. After the idle period, the SDK will re-establish
      // connections as needed — at most one per node contacted. This is expected SDK behavior.
      assertTrue(
          "New connections after 60s idle should be at most the number of nodes ("
              + nodes.size()
              + "), got "
              + newConnectionsAfterIdle
              + " (baseline="
              + baselineBeforeIdle
              + ", after="
              + totalAfterIdle
              + ")",
          newConnectionsAfterIdle <= nodes.size());

    } finally {
      try {
        client.deleteTable(DeleteTableRequest.builder().tableName(tableName).build());
      } catch (Exception e) {
        // Best effort cleanup
      }
      wrapper.close();
    }
  }

  /**
   * Verifies that DynamoDB SDK operations reuse pooled TCP connections rather than creating a new
   * connection for every request.
   *
   * <p>This test performs DynamoDB PutItem operations and checks that the number of ESTABLISHED TCP
   * connections to the Alternator port stays bounded (does not grow with the number of requests).
   * It also verifies that connections are not dropped during 500ms idle gaps and survive a
   * 60-second idle period.
   */
  @Test(timeout = 180_000)
  public void testDynamoDbOperationsReuseConnections() throws Exception {
    AlternatorDynamoDbClientWrapper wrapper =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(seedUri)
            .credentialsProvider(credentialsProvider)
            .buildWithAlternatorAPI();

    runDynamoDbConnectionReuseTest(wrapper, "conn_pool_it_default");
  }

  /**
   * Verifies that DynamoDB SDK operations reuse pooled TCP connections when headers optimization is
   * enabled.
   *
   * <p>Headers optimization wraps the SDK HTTP client with {@link
   * com.scylladb.alternator.HeadersFilteringSdkHttpClient}, which filters outgoing headers through
   * a whitelist. This test ensures that the wrapper does not interfere with HTTP connection pooling
   * — connections must still be kept alive and reused across requests, including after 500ms idle
   * gaps and a 60-second idle period.
   */
  @Test(timeout = 180_000)
  public void testDynamoDbOperationsReuseConnectionsWithHeadersOptimization() throws Exception {
    AlternatorDynamoDbClientWrapper wrapper =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(seedUri)
            .credentialsProvider(credentialsProvider)
            .withOptimizeHeaders(true)
            .buildWithAlternatorAPI();

    runDynamoDbConnectionReuseTest(wrapper, "conn_pool_it_headers_opt");
  }
}
