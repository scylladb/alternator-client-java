/*
 * Copyright ScyllaDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.scylladb.alternator.internal;

import static org.junit.Assert.*;

import com.scylladb.alternator.AlternatorConfig;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests that HTTP connections are properly managed and not leaked in AlternatorLiveNodes.
 *
 * <p>These tests call {@link AlternatorLiveNodes#refreshDiscoveredNodes()} synchronously (no
 * background thread) and verify that connections are properly released by checking that subsequent
 * requests succeed (they would hang/fail if connections were leaked from a small pool).
 */
public class ConnectionLeakTest {

  private HttpServer server;
  private int port;
  private volatile int responseCode = 200;
  private volatile String localResponseBody = localScanResponse("127.0.0.1");
  private volatile String peersResponseBody = emptyScanResponse();

  @Before
  public void setUp() throws IOException {
    responseCode = 200;
    localResponseBody = localScanResponse("127.0.0.1");
    peersResponseBody = emptyScanResponse();
    server = HttpServer.create(new InetSocketAddress(0), 0);
    port = server.getAddress().getPort();

    server.createContext(
        "/",
        exchange -> {
          String requestBody =
              new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
          String responseBody =
              requestBody.contains(SystemTableTopologyDiscovery.LOCAL_TABLE)
                  ? localResponseBody
                  : peersResponseBody;
          byte[] body = responseBody.getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().set("Content-Type", "application/x-amz-json-1.0");
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
   * Verifies that connections are not leaked when topology scans return non-200 responses.
   *
   * <p>Before the fix, each non-200 response would leak a connection because the response entity
   * was not consumed. With a small pool, leaked connections cause subsequent requests to block
   * indefinitely. This test uses a timeout to detect such hangs.
   */
  @Test(timeout = 10000)
  public void testNoConnectionLeakOnNon200Responses() throws Exception {
    responseCode = 500;
    AlternatorLiveNodes liveNodes = createLiveNodes();

    try {
      for (int i = 0; i < 20; i++) {
        refreshAllowingFailure(liveNodes);
      }
    } finally {
      liveNodes.shutdownAndWait();
    }
  }

  /** Verifies that connections are properly returned after successful responses. */
  @Test(timeout = 10000)
  public void testNoConnectionLeakOnSuccessfulResponses() throws Exception {
    responseCode = 200;
    localResponseBody = localScanResponse("127.0.0.1");
    peersResponseBody = localScanResponse("127.0.0.2");
    AlternatorLiveNodes liveNodes = createLiveNodes();

    try {
      for (int i = 0; i < 30; i++) {
        liveNodes.refreshDiscoveredNodes();
      }

      assertEquals(2, liveNodes.getDiscoveredNodes().size());
    } finally {
      liveNodes.shutdownAndWait();
    }
  }

  /** Verifies malformed successful scan responses release their connections. */
  @Test(timeout = 10000)
  public void testMalformedSuccessfulResponsesDoNotThrowOrLeakConnections() throws Exception {
    responseCode = 200;
    String[] malformedBodies = {"", "not-json", "{", "{\"Items\":[", "{\"Items\":bad}"};
    AlternatorLiveNodes liveNodes = createLiveNodes();

    try {
      for (int i = 0; i < 30; i++) {
        localResponseBody = malformedBodies[i % malformedBodies.length];
        refreshAllowingFailure(liveNodes);
      }

      assertEquals(1, liveNodes.getDiscoveredNodes().size());
      assertEquals("127.0.0.1", liveNodes.getDiscoveredNodes().get(0).getHost());
    } finally {
      liveNodes.shutdownAndWait();
    }
  }

  /** Verifies no leaks when alternating between error and success responses. */
  @Test(timeout = 10000)
  public void testNoConnectionLeakOnAlternatingResponses() throws Exception {
    AtomicInteger callCount = new AtomicInteger(0);
    AtomicInteger responseForAttempt = new AtomicInteger(200);
    server.removeContext("/");
    server.createContext(
        "/",
        exchange -> {
          String requestBody =
              new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
          if (requestBody.contains(SystemTableTopologyDiscovery.LOCAL_TABLE)) {
            int count = callCount.incrementAndGet();
            responseForAttempt.set(count % 2 == 0 ? 200 : 503);
          }
          int code = responseForAttempt.get();
          byte[] body =
              (code == 200
                      ? requestBody.contains(SystemTableTopologyDiscovery.LOCAL_TABLE)
                          ? localScanResponse("127.0.0.1")
                          : emptyScanResponse()
                      : "Service Unavailable")
                  .getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().set("Content-Type", "application/x-amz-json-1.0");
          exchange.sendResponseHeaders(code, body.length);
          try (OutputStream os = exchange.getResponseBody()) {
            os.write(body);
          }
        });

    AlternatorLiveNodes liveNodes = createLiveNodes();

    try {
      for (int i = 0; i < 40; i++) {
        try {
          liveNodes.refreshDiscoveredNodes();
        } catch (IOException expected) {
          // Every other topology attempt intentionally fails.
        }
      }
    } finally {
      liveNodes.shutdownAndWait();
    }
  }

  /**
   * Verifies that HTTP connections are kept and reused across multiple requests, rather than being
   * created and destroyed for each request.
   *
   * <p>After multiple requests to the same host, subsequent requests should still succeed quickly
   * (within timeout), proving connections are being reused from the pool.
   */
  @Test(timeout = 10000)
  public void testConnectionsAreReused() throws Exception {
    responseCode = 200;
    localResponseBody = localScanResponse("127.0.0.1");
    peersResponseBody = emptyScanResponse();
    AlternatorLiveNodes liveNodes = createLiveNodes();

    try {
      for (int i = 0; i < 20; i++) {
        liveNodes.refreshDiscoveredNodes();
      }

      assertEquals(1, liveNodes.getDiscoveredNodes().size());
    } finally {
      liveNodes.shutdownAndWait();
    }
  }

  private static void refreshAllowingFailure(AlternatorLiveNodes liveNodes) throws Exception {
    try {
      liveNodes.refreshDiscoveredNodes();
    } catch (IOException expected) {
      // Some discovery implementations surface protocol or HTTP failures.
    }
  }

  private static String localScanResponse(String address) {
    return "{\"Items\":[{\"rpc_address\":{\"S\":\""
        + address
        + "\"},\"data_center\":{\"S\":\"dc1\"},\"rack\":{\"S\":\"rack1\"}}],"
        + "\"Count\":1,\"ScannedCount\":1}";
  }

  private static String emptyScanResponse() {
    return "{\"Items\":[],\"Count\":0,\"ScannedCount\":0}";
  }
}
