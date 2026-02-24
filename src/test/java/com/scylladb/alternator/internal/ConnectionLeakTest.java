package com.scylladb.alternator.internal;

import static org.junit.Assert.*;

import com.scylladb.alternator.AlternatorConfig;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.http.pool.PoolStats;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests that HTTP connections are properly managed and not leaked in AlternatorLiveNodes.
 *
 * <p>These tests call {@link AlternatorLiveNodes#updateLiveNodes()} synchronously (no background
 * thread) and directly inspect the connection pool stats to verify that connections are returned to
 * the pool in all code paths.
 */
public class ConnectionLeakTest {

  private HttpServer server;
  private int port;
  private volatile int responseCode = 200;
  private volatile String responseBody = "[\"127.0.0.1\"]";

  @Before
  public void setUp() throws IOException {
    responseCode = 200;
    responseBody = "[\"127.0.0.1\"]";
    server = HttpServer.create(new InetSocketAddress(0), 0);
    port = server.getAddress().getPort();

    server.createContext(
        "/localnodes",
        exchange -> {
          byte[] body = responseBody.getBytes();
          exchange.sendResponseHeaders(responseCode, body.length);
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

  private AlternatorLiveNodes createLiveNodes() throws Exception {
    URI seedUri = new URI("http://127.0.0.1:" + port);
    AlternatorConfig config = AlternatorConfig.builder().withSeedNode(seedUri).build();
    return new AlternatorLiveNodes(config);
  }

  /**
   * Verifies that connections are not leaked when /localnodes returns non-200 responses.
   *
   * <p>Before the fix, each non-200 response would leak a connection because the response entity
   * was not consumed. With maxPerRoute=1 in the pool, a single leaked connection causes all
   * subsequent requests to the same route to block indefinitely.
   */
  @Test(timeout = 5000)
  public void testNoConnectionLeakOnNon200Responses() throws Exception {
    responseCode = 500;
    AlternatorLiveNodes liveNodes = createLiveNodes();

    for (int i = 0; i < 10; i++) {
      liveNodes.updateLiveNodes();
    }

    PoolStats stats = liveNodes.getConnectionPoolStats();
    assertEquals("Leased connections after non-200 responses", 0, stats.getLeased());
  }

  /** Verifies that connections are properly returned after successful responses. */
  @Test(timeout = 5000)
  public void testNoConnectionLeakOnSuccessfulResponses() throws Exception {
    responseCode = 200;
    responseBody = "[\"127.0.0.1\",\"127.0.0.2\"]";
    AlternatorLiveNodes liveNodes = createLiveNodes();

    for (int i = 0; i < 20; i++) {
      liveNodes.updateLiveNodes();
    }

    PoolStats stats = liveNodes.getConnectionPoolStats();
    assertEquals("Leased connections after successful responses", 0, stats.getLeased());
    assertEquals(2, liveNodes.getLiveNodes().size());
  }

  /** Verifies no leaks when alternating between error and success responses. */
  @Test(timeout = 5000)
  public void testNoConnectionLeakOnAlternatingResponses() throws Exception {
    AtomicInteger callCount = new AtomicInteger(0);
    server.removeContext("/localnodes");
    server.createContext(
        "/localnodes",
        exchange -> {
          int count = callCount.incrementAndGet();
          byte[] body;
          int code;
          if (count % 2 == 0) {
            code = 200;
            body = "[\"127.0.0.1\"]".getBytes();
          } else {
            code = 503;
            body = "Service Unavailable".getBytes();
          }
          exchange.sendResponseHeaders(code, body.length);
          try (OutputStream os = exchange.getResponseBody()) {
            os.write(body);
          }
        });

    AlternatorLiveNodes liveNodes = createLiveNodes();

    for (int i = 0; i < 30; i++) {
      liveNodes.updateLiveNodes();
    }

    PoolStats stats = liveNodes.getConnectionPoolStats();
    assertEquals("Leased connections after alternating responses", 0, stats.getLeased());
  }
}
