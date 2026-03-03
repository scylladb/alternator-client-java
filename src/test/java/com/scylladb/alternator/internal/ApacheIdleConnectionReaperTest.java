package com.scylladb.alternator.internal;

import static org.junit.Assert.*;

import com.scylladb.alternator.AlternatorConfig;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
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
 * Tests that Apache's idle connection reaper is properly disabled when {@code
 * connectionMaxIdleTimeMs=0} (the default).
 *
 * <p>Uses a real HTTP server and makes actual requests through SDK clients created by {@link
 * ApacheSyncClientFactory}.
 */
public class ApacheIdleConnectionReaperTest {

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

  /**
   * Verifies that connectionMaxIdleTimeMs=0 disables idle reaping and connections remain usable
   * after idle periods.
   */
  @Test(timeout = 30000)
  public void testZeroIdleTimeDisablesReaping() throws Exception {
    AlternatorConfig config = AlternatorConfig.builder().withConnectionMaxIdleTimeMs(0).build();
    assertEquals("connectionMaxIdleTimeMs should be 0", 0, config.getConnectionMaxIdleTimeMs());

    SdkHttpClient client = ApacheSyncClientFactory.create(null, config, null);
    try {
      // Make multiple requests with brief idle periods — all should succeed
      for (int i = 0; i < 5; i++) {
        executeRequest(client);
        Thread.sleep(50);
      }
      assertEquals("All 5 requests should have reached the server", 5, requestCount.get());
    } finally {
      client.close();
    }
  }

  /**
   * Control test: with a non-zero idle time, the reaper is enabled but requests still succeed
   * because the SDK creates new connections after idle ones are reaped.
   */
  @Test(timeout = 30000)
  public void testIdleReaperEnabledWithNonZeroIdleTime() throws Exception {
    AlternatorConfig config = AlternatorConfig.builder().withConnectionMaxIdleTimeMs(1).build();

    SdkHttpClient client = ApacheSyncClientFactory.create(null, config, null);
    try {
      executeRequest(client);
      Thread.sleep(200);

      // Subsequent requests succeed — SDK creates new connections after idle ones are reaped
      for (int i = 0; i < 4; i++) {
        executeRequest(client);
      }
      assertEquals("All 5 requests should have reached the server", 5, requestCount.get());
    } finally {
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
    assertEquals("Request should succeed with 200", 200, response.httpResponse().statusCode());

    // Consume response to release the connection back to the pool
    if (response.responseBody().isPresent()) {
      response.responseBody().get().close();
    }
  }
}
