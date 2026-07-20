package com.scylladb.alternator.internal;

import static org.junit.Assert.assertEquals;

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

/** Unit tests for DNS-backed live-node discovery. */
public class AlternatorLiveNodesDnsDiscoveryTest {

  private HttpServer server;
  private int port;
  private final AtomicInteger requestCount = new AtomicInteger(0);

  @Before
  public void setUp() throws IOException {
    requestCount.set(0);
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    port = server.getAddress().getPort();

    server.createContext(
        "/localnodes",
        exchange -> {
          requestCount.incrementAndGet();
          byte[] body = "[\"localhost\",\"node-a.internal\"]".getBytes();
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

  /** Verifies discovery works when the configured entrypoint is a DNS name. */
  @Test(timeout = 10000)
  public void testDnsEntrypointDiscoversDnsNodeRecords() throws Exception {
    URI seedUri = new URI("http://localhost:" + port);
    AlternatorConfig config = AlternatorConfig.builder().withSeedNode(seedUri).build();
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config);

    liveNodes.updateLiveNodes();

    assertEquals("DNS seed should be contacted", 1, requestCount.get());
    assertEquals(2, liveNodes.getLiveNodes().size());
    assertEquals("localhost", liveNodes.getLiveNodes().get(0).getHost());
    assertEquals("node-a.internal", liveNodes.getLiveNodes().get(1).getHost());
  }
}
