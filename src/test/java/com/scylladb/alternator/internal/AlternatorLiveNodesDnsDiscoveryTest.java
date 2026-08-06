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

import static org.junit.Assert.assertEquals;

import com.scylladb.alternator.AlternatorConfig;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.http.conn.DnsResolver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;

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

  /** Verifies raw IPv6 literals are bracketed for discovery, Host, and learned-node routing. */
  @Test(timeout = 10000)
  public void testIpv6LiteralDiscoveryAndRouting() throws Exception {
    AtomicReference<String> hostHeader = new AtomicReference<>();
    HttpServer ipv6Server =
        startServer(
            InetAddress.getByName("::1"),
            "[\"::1\"]",
            exchange -> hostHeader.set(exchange.getRequestHeaders().getFirst("Host")));
    int ipv6Port = ipv6Server.getAddress().getPort();
    SdkHttpClient httpClient = apacheClient(null);
    try {
      AlternatorConfig config =
          AlternatorConfig.builder()
              .withSeedHosts(Arrays.asList("::1"))
              .withScheme("http")
              .withPort(ipv6Port)
              .build();
      AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, httpClient);

      liveNodes.updateLiveNodes();

      assertEquals("http://[::1]:" + ipv6Port, liveNodes.nextAsURI().toString());
      assertEquals("[::1]:" + ipv6Port, hostHeader.get());
    } finally {
      httpClient.close();
      ipv6Server.stop(0);
    }
  }

  /** Verifies A-only, AAAA-only, and both cross-family DNS fallback orders. */
  @Test(timeout = 10000)
  public void testSingleAndDualFamilyDnsEntrypoints() throws Exception {
    InetAddress ipv4 = InetAddress.getByName("127.0.0.1");
    InetAddress ipv6 = InetAddress.getByName("::1");

    assertDnsDiscovery(ipv4, ipv4);
    assertDnsDiscovery(ipv6, ipv6);
    assertDnsDiscovery(ipv4, ipv6, ipv4);
    assertDnsDiscovery(ipv6, ipv4, ipv6);
  }

  /** Verifies an unavailable dual-stack entrypoint returns promptly and preserves its seed. */
  @Test(timeout = 10000)
  public void testAllDnsRecordsUnavailableKeepsSeed() throws Exception {
    InetAddress ipv4 = InetAddress.getByName("127.0.0.1");
    InetAddress ipv6 = InetAddress.getByName("::1");
    HttpServer closedServer = startServer(ipv4, "[]", exchange -> {});
    int closedPort = closedServer.getAddress().getPort();
    closedServer.stop(0);
    SdkHttpClient httpClient = apacheClient(host -> new InetAddress[] {ipv6, ipv4});
    try {
      AlternatorConfig config = dnsConfig(closedPort);
      AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, httpClient);

      liveNodes.updateLiveNodes();

      assertEquals(1, liveNodes.getLiveNodes().size());
      assertEquals("dual.test", liveNodes.nextAsURI().getHost());
    } finally {
      httpClient.close();
    }
  }

  private void assertDnsDiscovery(InetAddress listenAddress, InetAddress... resolvedAddresses)
      throws Exception {
    AtomicReference<String> hostHeader = new AtomicReference<>();
    HttpServer dnsServer =
        startServer(
            listenAddress,
            "[\"dual.test\"]",
            exchange -> hostHeader.set(exchange.getRequestHeaders().getFirst("Host")));
    int dnsPort = dnsServer.getAddress().getPort();
    SdkHttpClient httpClient = apacheClient(host -> resolvedAddresses);
    try {
      AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(dnsConfig(dnsPort), httpClient);

      liveNodes.updateLiveNodes();

      assertEquals(1, liveNodes.getLiveNodes().size());
      assertEquals("dual.test", liveNodes.nextAsURI().getHost());
      assertEquals("dual.test:" + dnsPort, hostHeader.get());
    } finally {
      httpClient.close();
      dnsServer.stop(0);
    }
  }

  private AlternatorConfig dnsConfig(int dnsPort) {
    return AlternatorConfig.builder()
        .withSeedHosts(Arrays.asList("dual.test"))
        .withScheme("http")
        .withPort(dnsPort)
        .build();
  }

  private SdkHttpClient apacheClient(DnsResolver dnsResolver) {
    ApacheHttpClient.Builder builder =
        ApacheHttpClient.builder()
            .connectionTimeout(Duration.ofMillis(500))
            .socketTimeout(Duration.ofSeconds(2));
    if (dnsResolver != null) {
      builder.dnsResolver(dnsResolver);
    }
    return builder.build();
  }

  private HttpServer startServer(
      InetAddress listenAddress, String responseBody, ExchangeObserver observer)
      throws IOException {
    HttpServer httpServer = HttpServer.create(new InetSocketAddress(listenAddress, 0), 0);
    httpServer.createContext(
        "/localnodes",
        exchange -> {
          observer.observe(exchange);
          byte[] body = responseBody.getBytes();
          exchange.sendResponseHeaders(200, body.length);
          try (OutputStream output = exchange.getResponseBody()) {
            output.write(body);
          }
        });
    httpServer.start();
    return httpServer;
  }

  private interface ExchangeObserver {
    void observe(com.sun.net.httpserver.HttpExchange exchange);
  }
}
