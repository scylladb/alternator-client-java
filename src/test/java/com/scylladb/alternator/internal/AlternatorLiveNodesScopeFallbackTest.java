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
import com.scylladb.alternator.NodeHealthConfig;
import com.scylladb.alternator.NodeHealthObservation;
import com.scylladb.alternator.routing.ClusterScope;
import com.scylladb.alternator.routing.DatacenterScope;
import com.scylladb.alternator.routing.RackScope;
import com.scylladb.alternator.routing.RoutingScope;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.junit.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;

/** Tests client-side routing-scope filtering and fallback over one topology snapshot. */
public class AlternatorLiveNodesScopeFallbackTest {
  private static final StaticCredentialsProvider CREDENTIALS =
      StaticCredentialsProvider.create(
          AwsBasicCredentials.create("topology-access-key", "topology-secret-key"));

  @Test
  public void rackMissFallsBackToDatacenterWithoutAdditionalScans() throws Exception {
    ScriptedTopologyHttpClient httpClient = new ScriptedTopologyHttpClient();
    httpClient.succeed(
        "seed.example.com",
        ScriptedTopologyHttpClient.scan(ScriptedTopologyHttpClient.row("10.0.0.1", "dc1", "rack2")),
        ScriptedTopologyHttpClient.scan(
            ScriptedTopologyHttpClient.row("10.0.0.2", "dc1", "rack3"),
            ScriptedTopologyHttpClient.row("10.0.1.1", "dc2", "rack1")));
    AlternatorLiveNodes liveNodes =
        liveNodes(
            RackScope.of("dc1", "rack1", DatacenterScope.of("dc1", ClusterScope.create())),
            httpClient);

    try {
      liveNodes.refreshDiscoveredNodes();

      assertEquals(setOf("10.0.0.1", "10.0.0.2"), hostSet(liveNodes.getDiscoveredNodes()));
      assertEquals(2, httpClient.requests.size());
      assertEquals(
          Arrays.asList(
              SystemTableTopologyDiscovery.LOCAL_TABLE, SystemTableTopologyDiscovery.PEERS_TABLE),
          tableNames(httpClient.requests));
      for (ScriptedTopologyHttpClient.CapturedRequest request : httpClient.requests) {
        assertEquals("seed.example.com", request.httpRequest.host());
        assertTrue(request.httpRequest.rawQueryParameters().isEmpty());
        assertFalse(request.body.contains("dc1"));
        assertFalse(request.body.contains("rack1"));
      }
    } finally {
      liveNodes.shutdown();
    }
  }

  @Test
  public void rackAndDatacenterMissFallBackToClusterFromSameSnapshot() throws Exception {
    ScriptedTopologyHttpClient httpClient = new ScriptedTopologyHttpClient();
    httpClient.succeed(
        "seed.example.com",
        ScriptedTopologyHttpClient.scan(ScriptedTopologyHttpClient.row("10.0.2.1", "dc2", "rack9")),
        ScriptedTopologyHttpClient.scan(
            ScriptedTopologyHttpClient.row("10.0.3.1", "dc3", "rack9")));
    AlternatorLiveNodes liveNodes =
        liveNodes(
            RackScope.of("dc1", "rack1", DatacenterScope.of("dc1", ClusterScope.create())),
            httpClient);

    try {
      liveNodes.refreshDiscoveredNodes();

      assertEquals(setOf("10.0.2.1", "10.0.3.1"), hostSet(liveNodes.getDiscoveredNodes()));
      assertEquals(2, httpClient.requests.size());
    } finally {
      liveNodes.shutdown();
    }
  }

  @Test
  public void emptyStrictScopePublishesEmptyRingAfterCompleteSnapshot() throws Exception {
    ScriptedTopologyHttpClient httpClient = new ScriptedTopologyHttpClient();
    httpClient.succeed(
        "seed.example.com",
        ScriptedTopologyHttpClient.scan(
            ScriptedTopologyHttpClient.row("seed.example.com", "dc2", "rack1")),
        ScriptedTopologyHttpClient.scan(
            ScriptedTopologyHttpClient.row("scoped.example.com", "dc1", "rack1")));
    AlternatorLiveNodes liveNodes = liveNodes(RackScope.of("dc1", "rack1", null), httpClient);

    try {
      liveNodes.refreshDiscoveredNodes();
      assertEquals(setOf("scoped.example.com"), hostSet(liveNodes.getDiscoveredNodes()));

      httpClient.clearRequests();
      httpClient.succeed(
          "scoped.example.com",
          ScriptedTopologyHttpClient.scan(
              ScriptedTopologyHttpClient.row("scoped.example.com", "dc2", "rack2")),
          ScriptedTopologyHttpClient.scan(
              ScriptedTopologyHttpClient.row("other.example.com", "dc2", "rack2")));
      httpClient.succeed(
          "seed.example.com",
          ScriptedTopologyHttpClient.scan(
              ScriptedTopologyHttpClient.row("seed.example.com", "dc2", "rack1")),
          ScriptedTopologyHttpClient.scan(
              ScriptedTopologyHttpClient.row("other.example.com", "dc2", "rack2")));

      liveNodes.refreshDiscoveredNodes();

      assertTrue(liveNodes.getDiscoveredNodes().isEmpty());
      assertEquals(
          Arrays.asList(
              "scoped.example.com", "scoped.example.com", "seed.example.com", "seed.example.com"),
          capturedHostList(httpClient.requests));
    } finally {
      liveNodes.shutdown();
    }
  }

  @Test
  public void failedScopedNodeFallsBackToSeedWithoutPublishingSeed() throws Exception {
    ScriptedTopologyHttpClient httpClient = new ScriptedTopologyHttpClient();
    httpClient.succeed(
        "seed.example.com",
        ScriptedTopologyHttpClient.scan(
            ScriptedTopologyHttpClient.row("seed.example.com", "dc2", "rack1")),
        ScriptedTopologyHttpClient.scan(
            ScriptedTopologyHttpClient.row("scoped.example.com", "dc1", "rack1")));
    AlternatorLiveNodes liveNodes = liveNodes(DatacenterScope.of("dc1", null), httpClient);

    try {
      liveNodes.refreshDiscoveredNodes();
      assertEquals(setOf("scoped.example.com"), hostSet(liveNodes.getDiscoveredNodes()));
      assertFalse(hostSet(liveNodes.getDiscoveredNodes()).contains("seed.example.com"));

      httpClient.clearRequests();
      httpClient.fail("scoped.example.com");
      httpClient.succeed(
          "seed.example.com",
          ScriptedTopologyHttpClient.scan(
              ScriptedTopologyHttpClient.row("seed.example.com", "dc2", "rack1")),
          ScriptedTopologyHttpClient.scan(
              ScriptedTopologyHttpClient.row("recovered.example.com", "dc1", "rack2")));

      liveNodes.refreshDiscoveredNodes();

      assertEquals(
          Arrays.asList("scoped.example.com", "seed.example.com", "seed.example.com"),
          capturedHostList(httpClient.requests));
      assertEquals(
          Arrays.asList(
              SystemTableTopologyDiscovery.LOCAL_TABLE,
              SystemTableTopologyDiscovery.LOCAL_TABLE,
              SystemTableTopologyDiscovery.PEERS_TABLE),
          tableNames(httpClient.requests));
      assertEquals(setOf("recovered.example.com"), hostSet(liveNodes.getDiscoveredNodes()));
      assertFalse(hostSet(liveNodes.getDiscoveredNodes()).contains("seed.example.com"));
    } finally {
      liveNodes.shutdown();
    }
  }

  @Test
  public void laterCandidateWithPreferredScopeBeatsFirstCandidatesFallback() throws Exception {
    ScriptedTopologyHttpClient httpClient = new ScriptedTopologyHttpClient();
    httpClient.succeed(
        "seed-a.example.com",
        ScriptedTopologyHttpClient.scan(
            ScriptedTopologyHttpClient.row("seed-a.example.com", "dc2", "rack1")),
        ScriptedTopologyHttpClient.scan());
    httpClient.succeed(
        "seed-b.example.com",
        ScriptedTopologyHttpClient.scan(
            ScriptedTopologyHttpClient.row("target.example.com", "dc1", "rack1")),
        ScriptedTopologyHttpClient.scan());
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedHosts(Arrays.asList("seed-a.example.com", "seed-b.example.com"))
            .withScheme("http")
            .withPort(8000)
            .withRoutingScope(DatacenterScope.of("dc1", ClusterScope.create()))
            .build();
    AlternatorLiveNodes liveNodes =
        new AlternatorLiveNodes(config, httpClient, CREDENTIALS, Region.US_EAST_1);
    liveNodes.reportNodeResult(
        URI.create("http://seed-a.example.com:8000"), NodeHealthObservation.PROBE_SUCCESS);

    try {
      liveNodes.refreshDiscoveredNodes();

      assertEquals(setOf("target.example.com"), hostSet(liveNodes.getDiscoveredNodes()));
      assertEquals(
          Arrays.asList(
              "seed-a.example.com",
              "seed-a.example.com",
              "seed-b.example.com",
              "seed-b.example.com"),
          capturedHostList(httpClient.requests));
    } finally {
      liveNodes.shutdown();
    }
  }

  @Test
  public void allPreferredScopeNodesDownAllowsHealthyClusterFallback() throws Exception {
    ScriptedTopologyHttpClient httpClient = new ScriptedTopologyHttpClient();
    String seedLocal =
        ScriptedTopologyHttpClient.scan(
            ScriptedTopologyHttpClient.row("seed.example.com", "dc2", "rack1"));
    String seedPeers =
        ScriptedTopologyHttpClient.scan(
            ScriptedTopologyHttpClient.row("stale.example.com", "dc1", "rack1"));
    httpClient.succeed("seed.example.com", seedLocal, seedPeers);
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedHost("seed.example.com")
            .withScheme("http")
            .withPort(8000)
            .withNodeHealthConfig(
                NodeHealthConfig.builder().withQuarantineFailureThreshold(1).build())
            .withRoutingScope(DatacenterScope.of("dc1", ClusterScope.create()))
            .build();
    AlternatorLiveNodes liveNodes =
        new AlternatorLiveNodes(config, httpClient, CREDENTIALS, Region.US_EAST_1);
    URI stale = URI.create("http://stale.example.com:8000");

    try {
      liveNodes.refreshDiscoveredNodes();
      assertEquals(setOf("stale.example.com"), hostSet(liveNodes.getDiscoveredNodes()));
      liveNodes.reportNodeResult(stale, NodeHealthObservation.TRAFFIC_FAILURE);
      httpClient.succeed(
          "stale.example.com",
          ScriptedTopologyHttpClient.scan(
              ScriptedTopologyHttpClient.row("stale.example.com", "dc1", "rack1")),
          ScriptedTopologyHttpClient.scan(
              ScriptedTopologyHttpClient.row("seed.example.com", "dc2", "rack1")));
      httpClient.clearRequests();

      liveNodes.refreshDiscoveredNodes();

      assertEquals(
          setOf("stale.example.com", "seed.example.com"), hostSet(liveNodes.getDiscoveredNodes()));
      assertEquals(Arrays.asList(stale), liveNodes.getDownNodes());
    } finally {
      liveNodes.shutdown();
    }
  }

  @Test
  public void candidateFailureIsNotRetriedForEachScopeLevel() throws Exception {
    ScriptedTopologyHttpClient httpClient = new ScriptedTopologyHttpClient();
    httpClient.fail("seed.example.com");
    AlternatorLiveNodes liveNodes =
        liveNodes(
            RackScope.of("dc1", "rack1", DatacenterScope.of("dc1", ClusterScope.create())),
            httpClient);

    try {
      liveNodes.refreshDiscoveredNodes();

      assertEquals(1, httpClient.requests.size());
      assertEquals("seed.example.com", httpClient.requests.get(0).httpRequest.host());
      assertEquals(SystemTableTopologyDiscovery.LOCAL_TABLE, httpClient.requests.get(0).tableName);
    } finally {
      liveNodes.shutdown();
    }
  }

  private static AlternatorLiveNodes liveNodes(
      RoutingScope routingScope, ScriptedTopologyHttpClient httpClient) {
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedHost("seed.example.com")
            .withScheme("http")
            .withPort(8000)
            .withRoutingScope(routingScope)
            .build();
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
