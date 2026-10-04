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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.scylladb.alternator.AlternatorConfig;
import com.scylladb.alternator.routing.ClusterScope;
import com.scylladb.alternator.routing.DatacenterScope;
import com.scylladb.alternator.routing.RoutingScope;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.http.conn.DnsResolver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
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
        "/",
        exchange -> {
          String tableName = scanTable(exchange);
          if (SystemTableTopologyDiscovery.LOCAL_TABLE.equals(tableName)) {
            requestCount.incrementAndGet();
          }
          writeResponse(
              exchange, topologyResponse(tableName, "[\"localhost\",\"node-a.internal\"]"));
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

    try {
      liveNodes.updateLiveNodes();

      assertEquals("DNS seed should be contacted", 1, requestCount.get());
      assertEquals(2, liveNodes.getDiscoveredNodes().size());
      assertEquals(1, liveNodes.getActiveNodes().size());
      assertEquals("localhost", liveNodes.getActiveNodes().get(0).getHost());
      assertEquals("node-a.internal", liveNodes.getQuarantinedNodes().get(0).getHost());
    } finally {
      liveNodes.shutdownAndWait();
    }
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

  /** Verifies either address family can be selected when both DNS records are reachable. */
  @Test(timeout = 10000)
  public void testBothDnsFamiliesReachable() throws Exception {
    InetAddress ipv4 = InetAddress.getByName("127.0.0.1");
    InetAddress ipv6 = InetAddress.getByName("::1");
    HttpServer ipv6Server = startServer(ipv6, 0, "[\"ipv6-node.test\"]", exchange -> {});
    int dualPort = ipv6Server.getAddress().getPort();
    HttpServer ipv4Server = startServer(ipv4, dualPort, "[\"ipv4-node.test\"]", exchange -> {});
    try {
      assertFirstReachableDnsFamily(dualPort, "ipv6-node.test", ipv6, ipv4);
      assertFirstReachableDnsFamily(dualPort, "ipv4-node.test", ipv4, ipv6);
    } finally {
      ipv4Server.stop(0);
      ipv6Server.stop(0);
    }
  }

  /** Verifies a malformed response retains the seed and a later refresh can recover. */
  @Test(timeout = 10000)
  public void testMalformedResponseRetainsSeedThenNextRefreshRecovers() throws Exception {
    InetAddress ipv4 = InetAddress.getByName("127.0.0.1");
    InetAddress ipv6 = InetAddress.getByName("::1");
    HttpServer malformedServer =
        startServer(
            ipv4,
            0,
            "not-json",
            exchange -> exchange.getResponseHeaders().set("Connection", "close"));
    int dualPort = malformedServer.getAddress().getPort();
    HttpServer validServer =
        startServer(ipv6, dualPort, "[\"recovered-node.test\"]", exchange -> {});
    AtomicInteger resolutions = new AtomicInteger();
    SdkHttpClient httpClient =
        apacheClient(
            host ->
                resolutions.getAndIncrement() == 0
                    ? new InetAddress[] {ipv4, ipv6}
                    : new InetAddress[] {ipv6, ipv4});
    try {
      AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(dnsConfig(dualPort), httpClient);

      liveNodes.updateLiveNodes();
      assertEquals("dual.test", liveNodes.nextAsURI().getHost());

      liveNodes.updateLiveNodes();
      assertEquals("recovered-node.test", liveNodes.nextAsURI().getHost());
      assertTrue("DNS should be resolved again after malformed response", resolutions.get() >= 2);
    } finally {
      httpClient.close();
      validServer.stop(0);
      malformedServer.stop(0);
    }
  }

  /**
   * Verifies requests use learned nodes and discovery can recover through the original DNS seed.
   */
  @Test(timeout = 10000)
  public void testLearnedNodeRoutingAndOriginalDnsSeedRecovery() throws Exception {
    InetAddress ipv4 = InetAddress.getByName("127.0.0.1");
    InetAddress ipv6 = InetAddress.getByName("::1");
    AtomicReference<String> seedResponse = new AtomicReference<>("[\"learned.test\"]");
    AtomicInteger seedDiscoveryRequests = new AtomicInteger();
    AtomicInteger learnedDiscoveryRequests = new AtomicInteger();
    AtomicInteger operationRequests = new AtomicInteger();
    HttpServer seedServer =
        startServer(
            ipv4,
            0,
            exchange -> seedResponse.get(),
            exchange -> {
              if (isLocalScan(exchange)) {
                seedDiscoveryRequests.incrementAndGet();
              }
            });
    int dualPort = seedServer.getAddress().getPort();
    HttpServer learnedServer =
        startServer(
            ipv6,
            dualPort,
            "[\"learned.test\"]",
            exchange -> {
              if (isLocalScan(exchange)) {
                learnedDiscoveryRequests.incrementAndGet();
              }
            });
    learnedServer.createContext(
        "/operation",
        exchange -> {
          operationRequests.incrementAndGet();
          writeResponse(exchange, "ok");
        });
    AtomicBoolean recovering = new AtomicBoolean(false);
    SdkHttpClient httpClient =
        apacheClient(
            host -> {
              if ("learned.test".equals(host)) {
                return new InetAddress[] {ipv6};
              }
              if ("dual.test".equals(host) && recovering.get()) {
                return new InetAddress[] {ipv6, ipv4};
              }
              return new InetAddress[] {ipv4};
            });
    try {
      AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(dnsConfig(dualPort), httpClient);

      liveNodes.updateLiveNodes();
      assertEquals("learned.test", liveNodes.nextAsURI().getHost());
      assertEquals(1, seedDiscoveryRequests.get());

      assertEquals(200, executeGet(httpClient, liveNodes.nextAsURI().resolve("/operation")));
      assertEquals(1, operationRequests.get());
      assertEquals(
          "Normal requests must not return to the discovery seed", 1, seedDiscoveryRequests.get());

      liveNodes.updateLiveNodes();
      assertEquals(1, learnedDiscoveryRequests.get());
      assertEquals("learned.test", liveNodes.nextAsURI().getHost());

      learnedServer.stop(0);
      seedResponse.set("[\"recovered.test\"]");
      recovering.set(true);
      liveNodes.updateLiveNodes();

      assertEquals("recovered.test", liveNodes.nextAsURI().getHost());
      assertEquals(2, seedDiscoveryRequests.get());
    } finally {
      httpClient.close();
      learnedServer.stop(0);
      seedServer.stop(0);
    }
  }

  /** Verifies valid, strict-invalid, and fallback scopes over a dual-stack DNS entrypoint. */
  @Test(timeout = 10000)
  public void testScopeBehaviorOverDualStackDns() throws Exception {
    InetAddress ipv4 = InetAddress.getByName("127.0.0.1");
    InetAddress ipv6 = InetAddress.getByName("::1");
    AtomicReference<String> lastMethod = new AtomicReference<>();
    HttpServer scopeServer =
        startServer(
            ipv6,
            0,
            "[\"scoped-node.test\"]",
            exchange -> lastMethod.set(exchange.getRequestMethod()));
    int dualPort = scopeServer.getAddress().getPort();
    DnsResolver resolver = host -> new InetAddress[] {ipv4, ipv6};
    try {
      SdkHttpClient validClient = apacheClient(resolver);
      try {
        AlternatorLiveNodes valid =
            new AlternatorLiveNodes(
                dnsConfig(dualPort, DatacenterScope.of("dc1", null)), validClient);
        valid.updateLiveNodes();
        assertEquals("scoped-node.test", valid.nextAsURI().getHost());
        assertEquals("POST", lastMethod.get());
      } finally {
        validClient.close();
      }

      SdkHttpClient strictClient = apacheClient(resolver);
      try {
        AlternatorLiveNodes strict =
            new AlternatorLiveNodes(
                dnsConfig(dualPort, DatacenterScope.of("wrong", null)), strictClient);
        try {
          strict.checkIfRackAndDatacenterSetCorrectly();
          fail("Expected invalid strict scope to be rejected");
        } catch (AlternatorLiveNodes.ValidationError expected) {
          assertTrue(expected.getMessage().contains("routing scope may be set incorrectly"));
        }
      } finally {
        strictClient.close();
      }

      SdkHttpClient fallbackClient = apacheClient(resolver);
      try {
        AlternatorLiveNodes fallback =
            new AlternatorLiveNodes(
                dnsConfig(dualPort, DatacenterScope.of("wrong", ClusterScope.create())),
                fallbackClient);
        fallback.updateLiveNodes();
        assertEquals("scoped-node.test", fallback.nextAsURI().getHost());
        assertEquals("POST", lastMethod.get());
      } finally {
        fallbackClient.close();
      }
    } finally {
      scopeServer.stop(0);
    }
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

      long startNanos = System.nanoTime();
      liveNodes.updateLiveNodes();
      long elapsedMillis = Duration.ofNanos(System.nanoTime() - startNanos).toMillis();

      assertEquals(1, liveNodes.getDiscoveredNodes().size());
      assertEquals(1, liveNodes.getQuarantinedNodes().size());
      assertEquals("dual.test", liveNodes.nextAsURI().getHost());
      assertTrue("Unavailable DNS records must return promptly", elapsedMillis < 5000);
    } finally {
      httpClient.close();
    }
  }

  private void assertFirstReachableDnsFamily(
      int dnsPort, String expectedHost, InetAddress... resolvedAddresses) throws Exception {
    SdkHttpClient httpClient = apacheClient(host -> resolvedAddresses);
    try {
      AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(dnsConfig(dnsPort), httpClient);
      liveNodes.updateLiveNodes();
      assertEquals(expectedHost, liveNodes.nextAsURI().getHost());
    } finally {
      httpClient.close();
    }
  }

  /** Verifies refresh retries the original DNS seed after every learned node is unavailable. */
  @Test(timeout = 10000)
  public void testRefreshReresolvesInitialDnsEntrypoint() throws Exception {
    InetAddress initialSeedAddress = InetAddress.getByName("127.0.0.2");
    InetAddress oldNodeAddress = InetAddress.getByName("127.0.0.3");
    InetAddress recoveredAddress = InetAddress.getByName("127.0.0.4");
    AtomicInteger recoveryRequests = new AtomicInteger();
    AtomicReference<String> recoveryHostHeader = new AtomicReference<>();

    HttpServer initialSeedServer =
        startServer(
            initialSeedAddress,
            "[\"127.0.0.3\"]",
            exchange -> exchange.getResponseHeaders().add("Connection", "close"));
    int dnsPort = initialSeedServer.getAddress().getPort();
    HttpServer oldNodeServer =
        startServer(oldNodeAddress, dnsPort, "[\"127.0.0.3\"]", exchange -> {});
    HttpServer recoveredServer =
        startServer(
            recoveredAddress,
            dnsPort,
            "[\"127.0.0.4\"]",
            exchange -> {
              if (isLocalScan(exchange)) {
                recoveryRequests.incrementAndGet();
              }
              recoveryHostHeader.set(exchange.getRequestHeaders().getFirst("Host"));
            });

    AtomicReference<InetAddress[]> dnsAnswers =
        new AtomicReference<>(new InetAddress[] {initialSeedAddress});
    SdkHttpClient httpClient =
        apacheClient(
            hostname ->
                "dual.test".equals(hostname)
                    ? dnsAnswers.get()
                    : InetAddress.getAllByName(hostname));
    try {
      AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(dnsConfig(dnsPort), httpClient);

      liveNodes.updateLiveNodes();
      assertEquals("127.0.0.3", liveNodes.nextAsURI().getHost());

      initialSeedServer.stop(0);
      oldNodeServer.stop(0);
      dnsAnswers.set(new InetAddress[] {oldNodeAddress, recoveredAddress});

      liveNodes.updateLiveNodes();

      assertEquals("127.0.0.4", liveNodes.nextAsURI().getHost());
      assertEquals(1, recoveryRequests.get());
      assertEquals("dual.test:" + dnsPort, recoveryHostHeader.get());
    } finally {
      httpClient.close();
      initialSeedServer.stop(0);
      oldNodeServer.stop(0);
      recoveredServer.stop(0);
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

  private AlternatorConfig dnsConfig(int dnsPort, RoutingScope routingScope) {
    return AlternatorConfig.builder()
        .withSeedHosts(Arrays.asList("dual.test"))
        .withScheme("http")
        .withPort(dnsPort)
        .withRoutingScope(routingScope)
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
    return startServer(listenAddress, 0, responseBody, observer);
  }

  private HttpServer startServer(
      InetAddress listenAddress, int listenPort, String responseBody, ExchangeObserver observer)
      throws IOException {
    return startServer(listenAddress, listenPort, exchange -> responseBody, observer);
  }

  private HttpServer startServer(
      InetAddress listenAddress,
      int listenPort,
      ResponseProvider responseProvider,
      ExchangeObserver observer)
      throws IOException {
    HttpServer httpServer = HttpServer.create(new InetSocketAddress(listenAddress, listenPort), 0);
    httpServer.createContext(
        "/",
        exchange -> {
          String tableName = scanTable(exchange);
          exchange.setAttribute("scanTable", tableName);
          observer.observe(exchange);
          writeResponse(exchange, topologyResponse(tableName, responseProvider.response(exchange)));
        });
    httpServer.start();
    return httpServer;
  }

  private int executeGet(SdkHttpClient httpClient, URI uri) throws IOException {
    SdkHttpRequest request =
        SdkHttpRequest.builder()
            .uri(uri)
            .method(SdkHttpMethod.GET)
            .putHeader("Host", uri.getHost() + ":" + uri.getPort())
            .build();
    ExecutableHttpRequest executable =
        httpClient.prepareRequest(HttpExecuteRequest.builder().request(request).build());
    HttpExecuteResponse response = executable.call();
    if (response.responseBody().isPresent()) {
      try (AbortableInputStream ignored = response.responseBody().get()) {
        // Closing body releases connection.
      }
    }
    return response.httpResponse().statusCode();
  }

  private static void writeResponse(HttpExchange exchange, String responseBody) throws IOException {
    byte[] body = responseBody.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().set("Content-Type", "application/x-amz-json-1.0");
    exchange.sendResponseHeaders(200, body.length);
    try (OutputStream output = exchange.getResponseBody()) {
      output.write(body);
    }
  }

  private static String scanTable(HttpExchange exchange) throws IOException {
    String requestBody =
        new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
    if (requestBody.contains(SystemTableTopologyDiscovery.LOCAL_TABLE)) {
      return SystemTableTopologyDiscovery.LOCAL_TABLE;
    }
    if (requestBody.contains(SystemTableTopologyDiscovery.PEERS_TABLE)) {
      return SystemTableTopologyDiscovery.PEERS_TABLE;
    }
    return "";
  }

  private static boolean isLocalScan(HttpExchange exchange) {
    return SystemTableTopologyDiscovery.LOCAL_TABLE.equals(exchange.getAttribute("scanTable"));
  }

  private static String topologyResponse(String tableName, String nodeList) {
    List<String> nodes = parseNodeList(nodeList);
    if (nodes == null) {
      return nodeList;
    }
    if (SystemTableTopologyDiscovery.LOCAL_TABLE.equals(tableName)) {
      return scanResponse(nodes.isEmpty() ? nodes : nodes.subList(0, 1));
    }
    return scanResponse(nodes.size() <= 1 ? new ArrayList<>() : nodes.subList(1, nodes.size()));
  }

  private static List<String> parseNodeList(String nodeList) {
    String value = nodeList.trim();
    if (!value.startsWith("[") || !value.endsWith("]")) {
      return null;
    }
    String contents = value.substring(1, value.length() - 1).trim();
    List<String> nodes = new ArrayList<>();
    if (contents.isEmpty()) {
      return nodes;
    }
    for (String token : contents.split(",")) {
      String host = token.trim();
      if (host.length() < 2 || host.charAt(0) != '"' || host.charAt(host.length() - 1) != '"') {
        return null;
      }
      nodes.add(host.substring(1, host.length() - 1));
    }
    return nodes;
  }

  private static String scanResponse(List<String> addresses) {
    StringBuilder response = new StringBuilder("{\"Items\":[");
    for (int i = 0; i < addresses.size(); i++) {
      if (i > 0) {
        response.append(',');
      }
      response
          .append("{\"rpc_address\":{\"S\":\"")
          .append(addresses.get(i))
          .append("\"},\"data_center\":{\"S\":\"dc1\"},\"rack\":{\"S\":\"rack1\"}}");
    }
    return response
        .append("],\"Count\":")
        .append(addresses.size())
        .append(",\"ScannedCount\":")
        .append(addresses.size())
        .append('}')
        .toString();
  }

  private interface ExchangeObserver {
    void observe(HttpExchange exchange);
  }

  private interface ResponseProvider {
    String response(HttpExchange exchange);
  }
}
