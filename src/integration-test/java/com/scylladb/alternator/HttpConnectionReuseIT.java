package com.scylladb.alternator;

import static org.junit.Assert.*;
import static org.junit.Assume.*;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
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
 * <p>Tests run against both HTTP and HTTPS endpoints. Set environment variables to configure:
 *
 * <ul>
 *   <li>ALTERNATOR_HOST: Host address (default: 172.39.0.2)
 *   <li>ALTERNATOR_PORT: HTTP port number (default: 9998)
 *   <li>ALTERNATOR_HTTPS_PORT: HTTPS port number (default: 9999)
 *   <li>INTEGRATION_TESTS: Set to "true" to enable these tests
 * </ul>
 */
@RunWith(Parameterized.class)
public class HttpConnectionReuseIT {

  private final URI seedUri;

  public HttpConnectionReuseIT(String scheme, URI seedUri) {
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

  private AlternatorDynamoDbClientWrapper buildSyncWrapper() {
    AlternatorDynamoDbClient.AlternatorDynamoDbClientBuilder builder =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(seedUri)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS);
    if ("https".equals(seedUri.getScheme())) {
      builder.withTlsConfig(TlsConfig.trustAll());
    }
    return builder.buildWithAlternatorAPI();
  }

  private AlternatorDynamoDbAsyncClientWrapper buildAsyncWrapper() {
    AlternatorDynamoDbAsyncClient.AlternatorDynamoDbAsyncClientBuilder builder =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(seedUri)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS);
    if ("https".equals(seedUri.getScheme())) {
      builder.withTlsConfig(TlsConfig.trustAll());
    }
    return builder.buildWithAlternatorAPI();
  }

  @Test
  public void testSerialRequestsSucceed() throws Exception {
    AlternatorDynamoDbClientWrapper wrapper = buildSyncWrapper();
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
    AlternatorDynamoDbClientWrapper wrapper = buildSyncWrapper();
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
    AlternatorDynamoDbAsyncClientWrapper wrapper = buildAsyncWrapper();
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
    AlternatorDynamoDbAsyncClientWrapper wrapper = buildAsyncWrapper();
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
