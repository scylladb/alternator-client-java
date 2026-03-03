package com.scylladb.alternator.internal;

import static org.junit.Assert.*;

import com.scylladb.alternator.AlternatorConfig;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;

/**
 * Tests that {@code connectionAcquisitionTimeoutMs} triggers a timeout when the Apache sync
 * connection pool is exhausted.
 */
public class ApacheConnectionPoolExhaustionTest {

  private HttpServer server;
  private int port;
  private final AtomicInteger requestCount = new AtomicInteger(0);

  @Before
  public void setUp() throws IOException {
    requestCount.set(0);
    server = HttpServer.create(new InetSocketAddress(0), 0);
    port = server.getAddress().getPort();

    server.createContext(
        "/test",
        exchange -> {
          requestCount.incrementAndGet();
          try {
            Thread.sleep(2000);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          byte[] body = "OK".getBytes();
          exchange.sendResponseHeaders(200, body.length);
          try (OutputStream os = exchange.getResponseBody()) {
            os.write(body);
          }
        });

    server.start();
  }

  @After
  public void tearDown() {
    if (server != null) {
      server.stop(0);
    }
  }

  @Test(timeout = 30000)
  public void testAcquisitionTimeoutWhenPoolExhausted() throws Exception {
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withMaxConnections(1)
            .withConnectionAcquisitionTimeoutMs(500)
            .build();

    SdkHttpClient client = ApacheSyncClientFactory.create(null, config, null);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      // First request holds the only connection for 2s
      Future<?> firstRequest = executor.submit(() -> executeRequestIgnoreError(client));
      Thread.sleep(200);

      // Second request should fail with acquisition timeout
      try {
        executeRequest(client);
        fail("Expected an exception due to connection pool exhaustion");
      } catch (Exception e) {
        // Expected: acquisition timeout
        assertTimeoutException(e);
      }

      firstRequest.get();
    } finally {
      executor.shutdownNow();
      client.close();
    }
  }

  @Test(timeout = 30000)
  public void testNoTimeoutWithSufficientConnections() throws Exception {
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withMaxConnections(2)
            .withConnectionAcquisitionTimeoutMs(10000)
            .build();

    SdkHttpClient client = ApacheSyncClientFactory.create(null, config, null);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<?> first = executor.submit(() -> executeRequestIgnoreError(client));
      Future<?> second = executor.submit(() -> executeRequestIgnoreError(client));

      first.get();
      second.get();
      assertEquals("Both requests should have reached the server", 2, requestCount.get());
    } finally {
      executor.shutdownNow();
      client.close();
    }
  }

  private void executeRequest(SdkHttpClient client) throws Exception {
    SdkHttpRequest request =
        SdkHttpRequest.builder()
            .uri(URI.create("http://127.0.0.1:" + port + "/test"))
            .method(SdkHttpMethod.GET)
            .build();

    HttpExecuteRequest executeRequest = HttpExecuteRequest.builder().request(request).build();
    HttpExecuteResponse response = client.prepareRequest(executeRequest).call();

    if (response.responseBody().isPresent()) {
      response.responseBody().get().close();
    }
  }

  private void executeRequestIgnoreError(SdkHttpClient client) {
    try {
      executeRequest(client);
    } catch (Exception e) {
      // Ignore — used for background requests
    }
  }

  private void assertTimeoutException(Throwable e) {
    String message = getFullExceptionChain(e).toLowerCase();
    assertTrue(
        "Exception should indicate timeout or pool exhaustion, but was: " + message,
        message.contains("timeout")
            || message.contains("pool")
            || message.contains("acquire")
            || message.contains("connection"));
  }

  private String getFullExceptionChain(Throwable e) {
    StringBuilder sb = new StringBuilder();
    Throwable current = e;
    while (current != null) {
      if (current.getMessage() != null) {
        sb.append(current.getMessage()).append(" ");
      }
      sb.append(current.getClass().getSimpleName()).append(" ");
      current = current.getCause();
    }
    return sb.toString();
  }
}
