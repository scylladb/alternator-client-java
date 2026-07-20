package com.scylladb.alternator.internal;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import com.scylladb.alternator.AlternatorConfig;
import com.scylladb.alternator.IntegrationTestConfig;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Before;
import org.junit.Test;

/** Integration tests for DNS-backed live-node discovery against a real Alternator cluster. */
public class AlternatorLiveNodesDnsDiscoveryIT {

  @Before
  public void setUp() {
    assumeTrue(
        "Integration tests disabled. Set INTEGRATION_TESTS=true to enable.",
        IntegrationTestConfig.ENABLED);
  }

  @Test
  public void testDnsEntrypointDiscoversLiveClusterNodes() throws Exception {
    try (DnsEntrypointProxy proxy = new DnsEntrypointProxy()) {
      URI seedUri = new URI("http://localhost:" + proxy.getPort());
      AlternatorConfig config = AlternatorConfig.builder().withSeedNode(seedUri).build();
      AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config);

      try {
        liveNodes.updateLiveNodes();

        assertTrue("DNS entrypoint should be contacted", proxy.getRequestCount() > 0);
        assertFalse("Should discover live cluster nodes", liveNodes.getLiveNodes().isEmpty());
      } finally {
        liveNodes.shutdownAndWait();
      }
    }
  }

  private static class DnsEntrypointProxy implements AutoCloseable {
    private final HttpServer server;
    private final AtomicInteger requestCount = new AtomicInteger(0);

    DnsEntrypointProxy() throws IOException {
      server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
      server.createContext("/localnodes", this::handleLocalNodes);
      server.start();
    }

    int getPort() {
      return server.getAddress().getPort();
    }

    int getRequestCount() {
      return requestCount.get();
    }

    @Override
    public void close() {
      server.stop(0);
    }

    private void handleLocalNodes(com.sun.net.httpserver.HttpExchange exchange) throws IOException {
      requestCount.incrementAndGet();
      URI upstream =
          URI.create(
              "http://"
                  + IntegrationTestConfig.HOST
                  + ":"
                  + IntegrationTestConfig.HTTP_PORT
                  + "/localnodes");
      HttpURLConnection connection = (HttpURLConnection) upstream.toURL().openConnection();
      connection.setConnectTimeout(5000);
      connection.setReadTimeout(5000);

      int status = connection.getResponseCode();
      byte[] body;
      try (InputStream input = connection.getInputStream()) {
        body = input.readAllBytes();
      } finally {
        connection.disconnect();
      }

      exchange.getResponseHeaders().add("Content-Type", "application/json");
      exchange.sendResponseHeaders(status, body.length);
      try (OutputStream output = exchange.getResponseBody()) {
        output.write(body);
      }
    }
  }
}
