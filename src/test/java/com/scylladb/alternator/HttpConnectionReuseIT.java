package com.scylladb.alternator;

import static org.junit.Assert.*;
import static org.junit.Assume.*;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.ListTablesRequest;
import software.amazon.awssdk.services.dynamodb.model.ListTablesResponse;

/**
 * Integration tests for HTTP connection reuse and pooling through the load-balanced client.
 *
 * <p>These tests verify that rapid sequential and parallel requests succeed without connection
 * errors, which implicitly validates that HTTP connection reuse and pooling work correctly.
 *
 * <p>Set environment variables to configure: - ALTERNATOR_HOST: Host address (default: 172.39.0.2)
 * - ALTERNATOR_PORT: Port number (default: 9998) - ALTERNATOR_HTTPS: Use HTTPS (default: false) -
 * INTEGRATION_TESTS: Set to "true" to enable these tests
 */
public class HttpConnectionReuseIT {

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

  @Test
  public void testSerialRequestsSucceed() throws Exception {
    AlternatorDynamoDbClientWrapper wrapper =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(seedUri)
            .credentialsProvider(credentialsProvider)
            .buildWithAlternatorAPI();

    DynamoDbClient client = wrapper.getClient();

    try {
      int requestCount = 20;
      int successCount = 0;

      for (int i = 0; i < requestCount; i++) {
        ListTablesResponse response = client.listTables(ListTablesRequest.builder().build());
        assertNotNull("Response should not be null for request " + i, response);
        successCount++;
      }

      assertEquals(
          "All serial requests should succeed without connection errors",
          requestCount,
          successCount);
    } finally {
      wrapper.close();
    }
  }

  @Test
  public void testParallelRequestsSucceed() throws Exception {
    AlternatorDynamoDbClientWrapper wrapper =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(seedUri)
            .credentialsProvider(credentialsProvider)
            .buildWithAlternatorAPI();

    DynamoDbClient client = wrapper.getClient();
    ExecutorService executor = Executors.newFixedThreadPool(10);

    try {
      int requestCount = 50;
      List<Future<ListTablesResponse>> futures = new ArrayList<>();

      for (int i = 0; i < requestCount; i++) {
        futures.add(executor.submit(() -> client.listTables(ListTablesRequest.builder().build())));
      }

      int successCount = 0;
      for (int i = 0; i < futures.size(); i++) {
        ListTablesResponse response = futures.get(i).get(30, TimeUnit.SECONDS);
        assertNotNull("Response should not be null for request " + i, response);
        successCount++;
      }

      assertEquals(
          "All parallel requests should succeed without connection errors",
          requestCount,
          successCount);
    } finally {
      executor.shutdown();
      executor.awaitTermination(10, TimeUnit.SECONDS);
      wrapper.close();
    }
  }

  @Test
  public void testAsyncSerialRequestsSucceed() throws Exception {
    AlternatorDynamoDbAsyncClientWrapper wrapper =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(seedUri)
            .credentialsProvider(credentialsProvider)
            .buildWithAlternatorAPI();

    DynamoDbAsyncClient client = wrapper.getClient();

    try {
      int requestCount = 20;
      int successCount = 0;

      for (int i = 0; i < requestCount; i++) {
        ListTablesResponse response =
            client.listTables(ListTablesRequest.builder().build()).get(10, TimeUnit.SECONDS);
        assertNotNull("Response should not be null for request " + i, response);
        successCount++;
      }

      assertEquals(
          "All serial async requests should succeed without connection errors",
          requestCount,
          successCount);
    } finally {
      wrapper.close();
    }
  }

  @Test
  public void testAsyncParallelRequestsSucceed() throws Exception {
    AlternatorDynamoDbAsyncClientWrapper wrapper =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(seedUri)
            .credentialsProvider(credentialsProvider)
            .buildWithAlternatorAPI();

    DynamoDbAsyncClient client = wrapper.getClient();

    try {
      int requestCount = 50;
      @SuppressWarnings("unchecked")
      CompletableFuture<ListTablesResponse>[] futures = new CompletableFuture[requestCount];

      for (int i = 0; i < requestCount; i++) {
        futures[i] = client.listTables(ListTablesRequest.builder().build());
      }

      CompletableFuture.allOf(futures).get(30, TimeUnit.SECONDS);

      int successCount = 0;
      for (int i = 0; i < requestCount; i++) {
        ListTablesResponse response = futures[i].get();
        assertNotNull("Response should not be null for request " + i, response);
        successCount++;
      }

      assertEquals(
          "All parallel async requests should succeed without connection errors",
          requestCount,
          successCount);
    } finally {
      wrapper.close();
    }
  }
}
