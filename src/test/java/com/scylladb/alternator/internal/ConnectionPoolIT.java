package com.scylladb.alternator.internal;

import static org.junit.Assert.*;
import static org.junit.Assume.*;

import com.scylladb.alternator.AlternatorConfig;
import com.scylladb.alternator.AlternatorDynamoDbClient;
import com.scylladb.alternator.AlternatorDynamoDbClientWrapper;
import com.scylladb.alternator.IntegrationTestConfig;
import com.scylladb.alternator.TlsConfig;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.http.pool.PoolStats;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

/**
 * Integration test that verifies HTTP connections to a real Scylla cluster stay in the pool and are
 * reused across multiple {@code /localnodes} requests, rather than being created and destroyed for
 * each request.
 *
 * <p>Tests run against both HTTP and HTTPS endpoints. Requires a running ScyllaDB cluster with
 * Alternator enabled. Set environment variables:
 *
 * <ul>
 *   <li>INTEGRATION_TESTS=true - Enable integration tests
 *   <li>ALTERNATOR_HOST - Host address (default: 172.39.0.2)
 *   <li>ALTERNATOR_PORT - HTTP port number (default: 9998)
 *   <li>ALTERNATOR_HTTPS_PORT - HTTPS port number (default: 9999)
 * </ul>
 */
@RunWith(Parameterized.class)
public class ConnectionPoolIT {

  private final URI seedUri;

  public ConnectionPoolIT(String scheme, URI seedUri) {
    this.seedUri = seedUri;
  }

  @Parameters(name = "{0}")
  public static Collection<Object[]> data() {
    return IntegrationTestConfig.httpAndHttpsEndpoints();
  }

  @Before
  public void setUp() {
    assumeTrue(
        "Integration tests disabled. Set INTEGRATION_TESTS=true to enable.",
        IntegrationTestConfig.ENABLED);
  }

  private AlternatorLiveNodes createLiveNodes() {
    AlternatorConfig.Builder builder = AlternatorConfig.builder().withSeedNode(seedUri);
    if ("https".equals(seedUri.getScheme())) {
      builder.withTlsConfig(TlsConfig.trustAll());
    }
    return new AlternatorLiveNodes(builder.build());
  }

  /**
   * Verifies that connections are pooled and reused. After many {@code updateLiveNodes()} calls,
   * the number of available connections should equal the number of distinct nodes contacted (one
   * per route, since maxPerRoute=1), and none should be leased.
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
   * after sitting idle.
   *
   * <p>This guards against connections being evicted or closed by the client-side pool. After the
   * idle period, making more requests should not increase the number of available connections —
   * proving the existing ones were reused rather than new ones created alongside stale entries.
   *
   * <p>Uses a 10-second idle period which is sufficient to verify connection survival without
   * triggering the default 60-second idle reaper.
   */
  @Test(timeout = 30_000)
  public void testConnectionSurvivesIdlePeriod() throws Exception {
    AlternatorLiveNodes liveNodes = createLiveNodes();

    // Establish pooled connections by round-robining through all nodes
    for (int i = 0; i < 30; i++) {
      liveNodes.updateLiveNodes();
    }
    PoolStats before = liveNodes.getConnectionPoolStats();
    assertTrue("Connections should be available after requests", before.getAvailable() > 0);
    int availableBefore = before.getAvailable();

    // Let connections sit idle for 10 seconds — enough to verify survival
    // without hitting the default 60-second idle reaper
    Thread.sleep(10_000);

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
  private List<URI> getNodeUris(AlternatorDynamoDbClientWrapper wrapper) {
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

      // Let connections sit idle for 10 seconds — enough to verify survival
      // without hitting the default 60-second idle reaper
      long baselineBeforeIdle = getTotalConnectionsFromScylla(nodes);
      Thread.sleep(10_000);

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
      // After a short idle period (well under the 60s idle reaper threshold),
      // connections should be reused. Allow up to nodes.size() new connections
      // for the background AlternatorLiveNodes thread.
      assertTrue(
          "New connections after 10s idle should be at most the number of nodes ("
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

  private AlternatorDynamoDbClientWrapper buildDynamoWrapper() {
    AlternatorDynamoDbClient.AlternatorDynamoDbClientBuilder builder =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(seedUri)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS);
    if ("https".equals(seedUri.getScheme())) {
      builder.withTlsConfig(TlsConfig.trustAll());
    }
    return builder.buildWithAlternatorAPI();
  }

  private AlternatorDynamoDbClientWrapper buildDynamoWrapperWithHeaders() {
    AlternatorDynamoDbClient.AlternatorDynamoDbClientBuilder builder =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(seedUri)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .withOptimizeHeaders(true);
    if ("https".equals(seedUri.getScheme())) {
      builder.withTlsConfig(TlsConfig.trustAll());
    }
    return builder.buildWithAlternatorAPI();
  }

  /**
   * Verifies that DynamoDB SDK operations reuse pooled TCP connections rather than creating a new
   * connection for every request.
   *
   * <p>This test performs DynamoDB PutItem operations and checks that the number of ESTABLISHED TCP
   * connections to the Alternator port stays bounded (does not grow with the number of requests).
   * It also verifies that connections are not dropped during 500ms idle gaps and survive a
   * 10-second idle period.
   */
  @Test(timeout = 60_000)
  public void testDynamoDbOperationsReuseConnections() throws Exception {
    runDynamoDbConnectionReuseTest(buildDynamoWrapper(), "conn_pool_it_default");
  }

  /**
   * Verifies that DynamoDB SDK operations reuse pooled TCP connections when headers optimization is
   * enabled.
   */
  @Test(timeout = 180_000)
  public void testDynamoDbOperationsReuseConnectionsWithHeadersOptimization() throws Exception {
    runDynamoDbConnectionReuseTest(buildDynamoWrapperWithHeaders(), "conn_pool_it_headers_opt");
  }
}
