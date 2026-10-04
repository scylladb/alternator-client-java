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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.scylladb.alternator.AlternatorConfig;
import com.scylladb.alternator.routing.ClusterScope;
import com.scylladb.alternator.routing.DatacenterScope;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpFullResponse;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.regions.Region;

/** Unit tests for cluster discovery through DynamoDB system-table scans. */
public class AlternatorLiveNodesClusterDiscoveryTest {
  private static final StaticCredentialsProvider CREDENTIALS =
      StaticCredentialsProvider.create(
          AwsBasicCredentials.create("topology-access-key", "topology-secret-key"));

  @Test
  public void successfulCandidateReadsSignedLocalAndPeersAtCandidateEndpoint() throws Exception {
    ScriptedTopologyHttpClient httpClient = new ScriptedTopologyHttpClient();
    httpClient.succeed(
        "seed.example.com",
        ScriptedTopologyHttpClient.scan(ScriptedTopologyHttpClient.row("10.0.0.1", "dc1", "rack1")),
        ScriptedTopologyHttpClient.scan(
            ScriptedTopologyHttpClient.row("10.0.0.2", "dc1", "rack2"),
            ScriptedTopologyHttpClient.row("10.0.1.1", "dc2", "rack1")));
    AlternatorLiveNodes liveNodes =
        liveNodes(
            AlternatorConfig.builder()
                .withSeedHost("seed.example.com")
                .withScheme("http")
                .withPort(8123)
                .withRoutingScope(ClusterScope.create())
                .build(),
            httpClient);

    try {
      liveNodes.refreshDiscoveredNodes();

      assertEquals(
          setOf("10.0.0.1", "10.0.0.2", "10.0.1.1"), hostSet(liveNodes.getDiscoveredNodes()));
      assertEquals(2, httpClient.requests.size());
      assertEquals(
          Arrays.asList(
              SystemTableTopologyDiscovery.LOCAL_TABLE, SystemTableTopologyDiscovery.PEERS_TABLE),
          tableNames(httpClient.requests));
      for (ScriptedTopologyHttpClient.CapturedRequest request : httpClient.requests) {
        assertEquals("seed.example.com", request.httpRequest.host());
        assertEquals("http", request.httpRequest.protocol());
        assertEquals(8123, request.httpRequest.getUri().getPort());
        assertEquals(SdkHttpMethod.POST, request.httpRequest.method());
        assertEquals("/", request.httpRequest.encodedPath());
        assertTrue(request.httpRequest.rawQueryParameters().isEmpty());
        assertEquals(
            "DynamoDB_20120810.Scan",
            request.httpRequest.firstMatchingHeader("X-Amz-Target").orElse(null));
        assertTrue(request.httpRequest.firstMatchingHeader("X-Amz-Date").isPresent());
        assertTrue(
            request
                .httpRequest
                .firstMatchingHeader("Authorization")
                .orElse("")
                .startsWith("AWS4-HMAC-SHA256 Credential=topology-access-key/"));
        assertTrue(request.body.contains("\"ConsistentRead\":true"));
        assertTrue(
            request.body.contains("\"ProjectionExpression\":\"rpc_address,data_center,rack\""));
        assertTrue(request.body.contains("\"TableName\":\"" + request.tableName + "\""));
      }
    } finally {
      liveNodes.shutdown();
    }
  }

  @Test
  public void firstSuccessfulCandidateSuppliesSnapshotWithoutMergingSeeds() throws Exception {
    ScriptedTopologyHttpClient httpClient = new ScriptedTopologyHttpClient();
    httpClient.succeed(
        "seed-a.example.com",
        ScriptedTopologyHttpClient.scan(
            ScriptedTopologyHttpClient.row("a-local.example.com", "dc1", "rack1")),
        ScriptedTopologyHttpClient.scan(
            ScriptedTopologyHttpClient.row("a-peer.example.com", "dc1", "rack1")));
    httpClient.succeed(
        "seed-b.example.com",
        ScriptedTopologyHttpClient.scan(
            ScriptedTopologyHttpClient.row("b-local.example.com", "dc2", "rack1")),
        ScriptedTopologyHttpClient.scan(
            ScriptedTopologyHttpClient.row("b-peer.example.com", "dc2", "rack1")));
    AlternatorLiveNodes liveNodes =
        liveNodes(
            AlternatorConfig.builder()
                .withSeedHosts(Arrays.asList("seed-a.example.com", "seed-b.example.com"))
                .withScheme("http")
                .withPort(8000)
                .withRoutingScope(ClusterScope.create())
                .build(),
            httpClient);

    try {
      liveNodes.refreshDiscoveredNodes();

      assertEquals(2, httpClient.requests.size());
      String contactedSeed = httpClient.requests.get(0).httpRequest.host();
      assertEquals(contactedSeed, httpClient.requests.get(1).httpRequest.host());
      Set<String> expected =
          contactedSeed.equals("seed-a.example.com")
              ? setOf("a-local.example.com", "a-peer.example.com")
              : setOf("b-local.example.com", "b-peer.example.com");
      assertEquals(expected, hostSet(liveNodes.getDiscoveredNodes()));
      assertEquals(Collections.singleton(contactedSeed), capturedHosts(httpClient.requests));
    } finally {
      liveNodes.shutdown();
    }
  }

  @Test
  public void incompleteSnapshotFallsThroughToNextCurrentCandidate() throws Exception {
    ScriptedTopologyHttpClient httpClient = new ScriptedTopologyHttpClient();
    httpClient.succeed(
        "seed.example.com",
        ScriptedTopologyHttpClient.scan(
            ScriptedTopologyHttpClient.row("seed.example.com", "dc1", "rack1")),
        ScriptedTopologyHttpClient.scan(
            ScriptedTopologyHttpClient.row("replacement.example.com", "dc1", "rack1")));
    AlternatorLiveNodes liveNodes =
        liveNodes(
            AlternatorConfig.builder()
                .withSeedHost("seed.example.com")
                .withScheme("http")
                .withPort(8000)
                .withRoutingScope(ClusterScope.create())
                .build(),
            httpClient);

    try {
      liveNodes.refreshDiscoveredNodes();
      assertEquals(
          setOf("seed.example.com", "replacement.example.com"),
          hostSet(liveNodes.getDiscoveredNodes()));

      httpClient.clearRequests();
      httpClient.failPeers("seed.example.com");
      httpClient.succeed(
          "replacement.example.com",
          ScriptedTopologyHttpClient.scan(
              ScriptedTopologyHttpClient.row("replacement.example.com", "dc1", "rack1")),
          ScriptedTopologyHttpClient.scan(
              ScriptedTopologyHttpClient.row("recovered.example.com", "dc1", "rack2")));

      liveNodes.refreshDiscoveredNodes();

      assertEquals(
          Arrays.asList(
              "seed.example.com",
              "seed.example.com",
              "replacement.example.com",
              "replacement.example.com"),
          capturedHostList(httpClient.requests));
      assertEquals(
          Arrays.asList(
              SystemTableTopologyDiscovery.LOCAL_TABLE,
              SystemTableTopologyDiscovery.PEERS_TABLE,
              SystemTableTopologyDiscovery.LOCAL_TABLE,
              SystemTableTopologyDiscovery.PEERS_TABLE),
          tableNames(httpClient.requests));
      assertEquals(
          setOf("replacement.example.com", "recovered.example.com"),
          hostSet(liveNodes.getDiscoveredNodes()));
    } finally {
      liveNodes.shutdown();
    }
  }

  @Test
  public void allCandidateFailuresPreserveRingAndTryOriginalSeed() throws Exception {
    ScriptedTopologyHttpClient httpClient = new ScriptedTopologyHttpClient();
    httpClient.succeed(
        "seed.example.com",
        ScriptedTopologyHttpClient.scan(
            ScriptedTopologyHttpClient.row("seed.example.com", "dc2", "rack1")),
        ScriptedTopologyHttpClient.scan(
            ScriptedTopologyHttpClient.row("node-b.example.com", "dc1", "rack1"),
            ScriptedTopologyHttpClient.row("node-c.example.com", "dc1", "rack2")));
    AlternatorLiveNodes liveNodes =
        liveNodes(
            AlternatorConfig.builder()
                .withSeedHost("seed.example.com")
                .withScheme("http")
                .withPort(8000)
                .withRoutingScope(DatacenterScope.of("dc1", null))
                .build(),
            httpClient);

    try {
      liveNodes.refreshDiscoveredNodes();
      Set<String> publishedRing = setOf("node-b.example.com", "node-c.example.com");
      assertEquals(publishedRing, hostSet(liveNodes.getDiscoveredNodes()));
      assertFalse(publishedRing.contains("seed.example.com"));

      httpClient.clearRequests();
      httpClient.fail("node-b.example.com");
      httpClient.fail("node-c.example.com");
      httpClient.fail("seed.example.com");

      liveNodes.refreshDiscoveredNodes();

      assertEquals(publishedRing, hostSet(liveNodes.getDiscoveredNodes()));
      assertEquals(3, httpClient.requests.size());
      assertEquals(
          setOf("node-b.example.com", "node-c.example.com", "seed.example.com"),
          capturedHosts(httpClient.requests));
      for (ScriptedTopologyHttpClient.CapturedRequest request : httpClient.requests) {
        assertEquals(SystemTableTopologyDiscovery.LOCAL_TABLE, request.tableName);
      }
    } finally {
      liveNodes.shutdown();
    }
  }

  private static AlternatorLiveNodes liveNodes(
      AlternatorConfig config, ScriptedTopologyHttpClient httpClient) {
    return new AlternatorLiveNodes(config, httpClient, CREDENTIALS, Region.US_EAST_1);
  }

  private static Set<String> hostSet(List<URI> nodes) {
    Set<String> hosts = new LinkedHashSet<>();
    for (URI node : nodes) {
      hosts.add(node.getHost());
    }
    return hosts;
  }

  private static Set<String> setOf(String... values) {
    return new LinkedHashSet<>(Arrays.asList(values));
  }

  private static Set<String> capturedHosts(
      List<ScriptedTopologyHttpClient.CapturedRequest> requests) {
    return new HashSet<>(capturedHostList(requests));
  }

  private static List<String> capturedHostList(
      List<ScriptedTopologyHttpClient.CapturedRequest> requests) {
    List<String> hosts = new ArrayList<>();
    for (ScriptedTopologyHttpClient.CapturedRequest request : requests) {
      hosts.add(request.httpRequest.host());
    }
    return hosts;
  }

  private static List<String> tableNames(
      List<ScriptedTopologyHttpClient.CapturedRequest> requests) {
    List<String> tables = new ArrayList<>();
    for (ScriptedTopologyHttpClient.CapturedRequest request : requests) {
      tables.add(request.tableName);
    }
    return tables;
  }
}

/** Scripted DynamoDB transport shared by live-node discovery tests. */
final class ScriptedTopologyHttpClient implements SdkHttpClient {
  static final class CapturedRequest {
    final SdkHttpRequest httpRequest;
    final String body;
    final String tableName;

    private CapturedRequest(SdkHttpRequest httpRequest, String body, String tableName) {
      this.httpRequest = httpRequest;
      this.body = body;
      this.tableName = tableName;
    }
  }

  private static final class TopologyResponses {
    final String local;
    final String peers;

    private TopologyResponses(String local, String peers) {
      this.local = local;
      this.peers = peers;
    }
  }

  final List<CapturedRequest> requests = new CopyOnWriteArrayList<>();
  private final Map<String, TopologyResponses> responsesByHost = new ConcurrentHashMap<>();
  private final Set<String> failingHosts = ConcurrentHashMap.newKeySet();
  private final Set<String> peersFailingHosts = ConcurrentHashMap.newKeySet();

  void succeed(String host, String localResponse, String peersResponse) {
    responsesByHost.put(host, new TopologyResponses(localResponse, peersResponse));
    failingHosts.remove(host);
    peersFailingHosts.remove(host);
  }

  void fail(String host) {
    failingHosts.add(host);
  }

  void failPeers(String host) {
    peersFailingHosts.add(host);
  }

  void clearRequests() {
    requests.clear();
  }

  @Override
  public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
    SdkHttpRequest httpRequest = request.httpRequest();
    String body = readBody(request);
    String tableName = tableName(body);
    requests.add(new CapturedRequest(httpRequest, body, tableName));
    return new ExecutableHttpRequest() {
      @Override
      public HttpExecuteResponse call() throws IOException {
        String host = httpRequest.host();
        if (failingHosts.contains(host)
            || (SystemTableTopologyDiscovery.PEERS_TABLE.equals(tableName)
                && peersFailingHosts.contains(host))) {
          throw new IOException("simulated topology failure for " + host + " and " + tableName);
        }
        TopologyResponses responses = responsesByHost.get(host);
        if (responses == null) {
          throw new AssertionError("Unexpected topology endpoint: " + host);
        }
        String responseBody =
            SystemTableTopologyDiscovery.LOCAL_TABLE.equals(tableName)
                ? responses.local
                : responses.peers;
        byte[] bytes = responseBody.getBytes(StandardCharsets.UTF_8);
        return HttpExecuteResponse.builder()
            .response(
                SdkHttpFullResponse.builder()
                    .statusCode(200)
                    .putHeader("Content-Type", "application/x-amz-json-1.0")
                    .putHeader("Content-Length", String.valueOf(bytes.length))
                    .build())
            .responseBody(AbortableInputStream.create(new ByteArrayInputStream(bytes)))
            .build();
      }

      @Override
      public void abort() {}
    };
  }

  @Override
  public void close() {}

  @Override
  public String clientName() {
    return "ScriptedTopologyHttpClient";
  }

  static String row(String address, String dataCenter, String rack) {
    StringBuilder json =
        new StringBuilder("{\"rpc_address\":{\"S\":\"").append(address).append("\"}");
    if (dataCenter != null) {
      json.append(",\"data_center\":{\"S\":\"").append(dataCenter).append("\"}");
    }
    if (rack != null) {
      json.append(",\"rack\":{\"S\":\"").append(rack).append("\"}");
    }
    return json.append('}').toString();
  }

  static String scan(String... rows) {
    return "{\"Items\":["
        + String.join(",", rows)
        + "],\"Count\":"
        + rows.length
        + ",\"ScannedCount\":"
        + rows.length
        + "}";
  }

  private static String readBody(HttpExecuteRequest request) {
    if (!request.contentStreamProvider().isPresent()) {
      throw new AssertionError("DynamoDB Scan request has no body");
    }
    try (InputStream stream = request.contentStreamProvider().get().newStream()) {
      return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static String tableName(String body) {
    if (body.contains("\"TableName\":\"" + SystemTableTopologyDiscovery.LOCAL_TABLE + "\"")) {
      return SystemTableTopologyDiscovery.LOCAL_TABLE;
    }
    if (body.contains("\"TableName\":\"" + SystemTableTopologyDiscovery.PEERS_TABLE + "\"")) {
      return SystemTableTopologyDiscovery.PEERS_TABLE;
    }
    throw new AssertionError("Unexpected DynamoDB request body: " + body);
  }
}
