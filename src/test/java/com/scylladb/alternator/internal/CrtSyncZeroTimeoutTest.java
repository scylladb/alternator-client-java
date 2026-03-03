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
 * Tests that CRT sync client handles zero timeout values gracefully. The CRT SDK requires positive
 * durations for {@code connectionAcquisitionTimeout} and {@code connectionTimeout}, so the factory
 * skips these when set to 0 (falling back to SDK defaults).
 *
 * <p>Uses a real HTTP server and makes actual requests through SDK clients created by {@link
 * CrtSyncClientFactory}.
 */
public class CrtSyncZeroTimeoutTest {

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
   * Verifies that {@code connectionAcquisitionTimeoutMs=0} is handled gracefully — the factory
   * skips the setting (using SDK default) instead of passing {@code Duration.ZERO} which the CRT
   * SDK rejects.
   */
  @Test(timeout = 30000)
  public void testZeroAcquisitionTimeoutUsesSDKDefault() throws Exception {
    AlternatorConfig config =
        AlternatorConfig.builder().withConnectionAcquisitionTimeoutMs(0).build();

    SdkHttpClient client = CrtSyncClientFactory.create(null, config, null);
    try {
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
   * Verifies that {@code connectionTimeoutMs=0} is handled gracefully — the factory skips the
   * setting (using SDK default) instead of passing {@code Duration.ZERO} which the CRT SDK rejects.
   */
  @Test(timeout = 30000)
  public void testZeroConnectionTimeoutUsesSDKDefault() throws Exception {
    AlternatorConfig config = AlternatorConfig.builder().withConnectionTimeoutMs(0).build();

    SdkHttpClient client = CrtSyncClientFactory.create(null, config, null);
    try {
      for (int i = 0; i < 5; i++) {
        executeRequest(client);
        Thread.sleep(50);
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

    if (response.responseBody().isPresent()) {
      response.responseBody().get().close();
    }
  }
}
