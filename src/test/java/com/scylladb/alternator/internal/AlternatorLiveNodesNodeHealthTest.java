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
import com.scylladb.alternator.CoversRequirements;
import com.scylladb.alternator.NodeHealthConfig;
import com.scylladb.alternator.NodeHealthObservation;
import com.scylladb.alternator.NodeHealthState;
import com.scylladb.alternator.routing.DatacenterScope;
import com.scylladb.alternator.routing.RoutingScope;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Test;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpFullResponse;
import software.amazon.awssdk.http.SdkHttpRequest;

public class AlternatorLiveNodesNodeHealthTest {
  @Test
  @CoversRequirements("HEALTH-REQ-005")
  public void discoveryActivatesContactedSeedButQuarantinesNewNodesUntilDirectProbe()
      throws Exception {
    SystemTableHttpClient httpClient = new SystemTableHttpClient("seed.local", "new.local");
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedHosts(Arrays.asList("seed.local"))
            .withScheme("http")
            .withPort(8080)
            .withActiveRefreshIntervalMs(60_000)
            .withIdleRefreshIntervalMs(60_000)
            .build();
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, httpClient);
    URI seed = node("seed.local");
    URI discovered = node("new.local");

    assertEquals(Arrays.asList(seed), liveNodes.getQuarantinedNodes());
    assertTrue(liveNodes.getActiveNodes().isEmpty());

    liveNodes.refreshDiscoveredNodes();

    assertEquals(Arrays.asList(seed), liveNodes.getActiveNodes());
    assertEquals(Arrays.asList(discovered), liveNodes.getQuarantinedNodes());

    assertEquals(Arrays.asList(discovered), liveNodes.probeQuarantinedNodes());

    assertEquals(Arrays.asList(discovered, seed), liveNodes.getActiveNodes());
    assertTrue(liveNodes.getQuarantinedNodes().isEmpty());
    assertEquals(3, httpClient.requests.size());
    assertEquals("new.local", httpClient.requests.get(2).host());
    assertDirectProbe(httpClient, 2);
  }

  @Test
  public void probeQuarantinedNodesReturnsPartialSuccessAndLeavesFailuresQuarantined() {
    SystemTableHttpClient httpClient = new SystemTableHttpClient(Arrays.asList(), 1);
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedHosts(Arrays.asList("node-a.local", "node-b.local"))
            .withScheme("http")
            .withPort(8080)
            .withActiveRefreshIntervalMs(60_000)
            .withIdleRefreshIntervalMs(60_000)
            .build();
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, httpClient);

    List<URI> successful = liveNodes.probeQuarantinedNodes();

    assertEquals(1, successful.size());
    assertEquals(successful, liveNodes.getActiveNodes());
    assertEquals(1, liveNodes.getQuarantinedNodes().size());
  }

  @Test
  public void probeQuarantinedNodesSkipsActiveAndDownNodes() {
    SystemTableHttpClient httpClient = new SystemTableHttpClient();
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedHosts(Arrays.asList("active.local", "down.local", "quarantined.local"))
            .withScheme("http")
            .withPort(8080)
            .withNodeHealthConfig(
                NodeHealthConfig.builder().withQuarantineFailureThreshold(1).build())
            .build();
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, httpClient);
    URI active = node("active.local");
    URI down = node("down.local");
    URI quarantined = node("quarantined.local");
    liveNodes.reportNodeResult(active, NodeHealthObservation.PROBE_SUCCESS);
    liveNodes.reportNodeResult(down, NodeHealthObservation.TRAFFIC_FAILURE);

    assertEquals(Arrays.asList(quarantined), liveNodes.probeQuarantinedNodes());

    assertEquals(1, httpClient.requests.size());
    assertEquals("quarantined.local", httpClient.requests.get(0).host());
    assertDirectProbe(httpClient, 0);
    assertEquals(NodeHealthState.ACTIVE, liveNodes.getNodeHealthStatus(active).getState());
    assertEquals(NodeHealthState.DOWN, liveNodes.getNodeHealthStatus(down).getState());
    assertEquals(NodeHealthState.ACTIVE, liveNodes.getNodeHealthStatus(quarantined).getState());
  }

  @Test
  @CoversRequirements("HEALTH-REQ-008")
  public void topologyRefreshUsesHealthBucketsAndDownFallbackIsHealthNeutral() throws Exception {
    SystemTableHttpClient httpClient =
        new SystemTableHttpClient(
            Arrays.asList("down.local"),
            new HashSet<>(Arrays.asList("active.local", "quarantined.local")));
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedHosts(Arrays.asList("down.local", "quarantined.local", "active.local"))
            .withScheme("http")
            .withPort(8080)
            .withNodeHealthConfig(
                NodeHealthConfig.builder().withQuarantineFailureThreshold(1).build())
            .build();
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, httpClient);
    URI active = node("active.local");
    URI quarantined = node("quarantined.local");
    URI down = node("down.local");
    liveNodes.reportNodeResult(active, NodeHealthObservation.PROBE_SUCCESS);
    liveNodes.reportNodeResult(down, NodeHealthObservation.TRAFFIC_FAILURE);
    int downRecoveryBefore = liveNodes.getNodeHealthStatus(down).getConsecutiveSuccesses();

    liveNodes.refreshDiscoveredNodes();

    assertEquals(4, httpClient.requests.size());
    assertEquals("active.local", httpClient.requests.get(0).host());
    assertEquals("quarantined.local", httpClient.requests.get(1).host());
    assertEquals("down.local", httpClient.requests.get(2).host());
    assertEquals("down.local", httpClient.requests.get(3).host());
    assertEquals(
        NodeHealthState.QUARANTINED, liveNodes.getNodeHealthStatus(quarantined).getState());
    assertEquals(NodeHealthState.DOWN, liveNodes.getNodeHealthStatus(down).getState());
    assertEquals(downRecoveryBefore, liveNodes.getNodeHealthStatus(down).getConsecutiveSuccesses());
  }

  @Test
  public void runDownNodeProbesMovesRecoveredNodeToQuarantine() {
    AlternatorLiveNodes liveNodes =
        liveNodes(
            NodeHealthConfig.builder().withDownNodeRecoverySuccessThreshold(1).build(),
            new SystemTableHttpClient());
    URI recovering = node("recovering.local");

    markNodeDown(liveNodes, recovering);
    List<URI> recovered = liveNodes.runDownNodeProbes();

    assertEquals(Arrays.asList(recovering), recovered);
    assertEquals(Arrays.asList(recovering), liveNodes.getQuarantinedNodes());
    assertTrue(liveNodes.getDownNodes().isEmpty());
  }

  @Test
  public void runDownNodeProbesRequiresConfiguredRecoverySuccessesBeforeQuarantine() {
    AlternatorLiveNodes liveNodes =
        liveNodes(
            NodeHealthConfig.builder().withDownNodeRecoverySuccessThreshold(2).build(),
            new SystemTableHttpClient());
    URI recovering = node("recovering.local");

    markNodeDown(liveNodes, recovering);

    assertTrue(liveNodes.runDownNodeProbes().isEmpty());
    assertEquals(Arrays.asList(recovering), liveNodes.getDownNodes());
    assertEquals(1, liveNodes.getNodeHealthStatus(recovering).getConsecutiveSuccesses());

    assertEquals(Arrays.asList(recovering), liveNodes.runDownNodeProbes());
    assertEquals(Arrays.asList(recovering), liveNodes.getQuarantinedNodes());
    assertEquals(0, liveNodes.getNodeHealthStatus(recovering).getConsecutiveSuccesses());
  }

  @Test
  public void featureCheckUsesHealthTieredNodeForSystemTableScans() throws Exception {
    SystemTableHttpClient httpClient = new SystemTableHttpClient("active.local");
    AlternatorLiveNodes liveNodes = liveNodes(NodeHealthConfig.getDefault(), httpClient);
    URI recovering = node("recovering.local");

    markNodeDown(liveNodes, recovering);

    assertTrue(liveNodes.checkIfRackDatacenterFeatureIsSupported());

    assertEquals(2, httpClient.requests.size());
    assertEquals("active.local", httpClient.requests.get(0).host());
    assertEquals("active.local", httpClient.requests.get(1).host());
    assertEquals("/", httpClient.requests.get(0).encodedPath());
    assertEquals("/", httpClient.requests.get(1).encodedPath());
    assertScanTargetsTable(httpClient, 0, SystemTableTopologyDiscovery.LOCAL_TABLE);
    assertScanTargetsTable(httpClient, 1, SystemTableTopologyDiscovery.PEERS_TABLE);
  }

  @Test
  public void featureCheckTriesNextHealthCandidateWhenOneNodeFails() throws Exception {
    SystemTableHttpClient httpClient = new SystemTableHttpClient(Arrays.asList("active.local"), 1);
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedHosts(Arrays.asList("node-a.local", "node-b.local"))
            .withScheme("http")
            .withPort(8080)
            .withActiveRefreshIntervalMs(60_000)
            .withIdleRefreshIntervalMs(60_000)
            .build();
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, httpClient);

    assertTrue(liveNodes.checkIfRackDatacenterFeatureIsSupported());

    assertEquals(3, httpClient.requests.size());
    assertNotEquals(httpClient.requests.get(0).host(), httpClient.requests.get(1).host());
    assertEquals(httpClient.requests.get(1).host(), httpClient.requests.get(2).host());
    assertScanTargetsTable(httpClient, 0, SystemTableTopologyDiscovery.LOCAL_TABLE);
    assertScanTargetsTable(httpClient, 1, SystemTableTopologyDiscovery.LOCAL_TABLE);
    assertScanTargetsTable(httpClient, 2, SystemTableTopologyDiscovery.PEERS_TABLE);
  }

  @Test
  public void featureCheckProbeOutcomesDoNotAffectActiveNodeHealth() throws Exception {
    SystemTableHttpClient httpClient = new SystemTableHttpClient(Arrays.asList("active.local"), 1);
    AlternatorLiveNodes liveNodes =
        liveNodes(
            NodeHealthConfig.builder().withConsecutiveFailureThreshold(2).build(),
            httpClient,
            null,
            Arrays.asList("active.local"));
    URI active = node("active.local");

    try {
      liveNodes.checkIfRackDatacenterFeatureIsSupported();
      fail("expected feature check failure");
    } catch (AlternatorLiveNodes.FailedToCheck expected) {
      // expected
    }
    assertEquals(0, liveNodes.getNodeHealthStatus(active).getConsecutiveFailures());

    assertTrue(liveNodes.checkIfRackDatacenterFeatureIsSupported());

    assertEquals(0, liveNodes.getNodeHealthStatus(active).getConsecutiveFailures());
    assertTrue(liveNodes.getDownNodes().isEmpty());
  }

  @Test
  public void randomQueryPlanKeepsQuarantinedCandidateInKnownNodeSet() {
    AlternatorLiveNodes liveNodes =
        liveNodes(
            NodeHealthConfig.builder()
                .withDownNodeRecoverySuccessThreshold(1)
                .withQuarantineSuccessThreshold(3)
                .build(),
            new SystemTableHttpClient(),
            null,
            Arrays.asList(
                "active01.local",
                "active02.local",
                "active03.local",
                "active04.local",
                "active05.local",
                "active06.local",
                "active07.local",
                "active08.local",
                "active09.local",
                "active10.local",
                "active11.local",
                "active12.local",
                "recovering.local"));
    URI recovering = node("recovering.local");

    markNodeDown(liveNodes, recovering);
    liveNodes.runDownNodeProbes();

    assertTrue(collectNodes(new LazyQueryPlan(liveNodes)).contains(recovering));
  }

  @Test
  public void preferredQueryPlanNodeForHashKeepsQuarantinedPreferredNode() {
    AlternatorLiveNodes liveNodes =
        liveNodes(
            NodeHealthConfig.builder()
                .withDownNodeRecoverySuccessThreshold(1)
                .withQuarantineSuccessThreshold(3)
                .build(),
            new SystemTableHttpClient());
    URI active = node("active.local");
    URI recovering = node("recovering.local");
    long recoveringHash = hashWhereFirstNodeIs(Arrays.asList(active, recovering), recovering);

    markNodeDown(liveNodes, recovering);
    liveNodes.runDownNodeProbes();

    URI preferred = liveNodes.getPreferredQueryPlanNodeForHash(recoveringHash);
    LazyQueryPlan plan = new LazyQueryPlan(liveNodes, recoveringHash);

    assertEquals(recovering, preferred);
    assertEquals(preferred, plan.next());
  }

  @Test
  public void quarantineHashAssignmentKeepsDownNodeButFilterSkipsIt() {
    AlternatorLiveNodes liveNodes =
        liveNodes(
            NodeHealthConfig.builder()
                .withDownNodeRecoverySuccessThreshold(1)
                .withQuarantineSuccessThreshold(3)
                .build(),
            new SystemTableHttpClient());
    URI active = node("active.local");
    URI recovering = node("recovering.local");
    long recoveringHash = hashWhereFirstNodeIs(Arrays.asList(active, recovering), recovering);

    markNodeDown(liveNodes, recovering);
    liveNodes.runDownNodeProbes();
    assertEquals(
        Arrays.asList(recovering, active), liveNodes.getQueryPlanNodesForHash(recoveringHash));

    markNodeDown(liveNodes, recovering);

    assertEquals(Arrays.asList(recovering), liveNodes.getDownNodes());
    assertEquals(
        Arrays.asList(recovering, active), liveNodes.getQueryPlanNodesForHash(recoveringHash));
    NodeHealthQueryPlan plan =
        liveNodes.newAffinityQueryPlan(new LazyQueryPlan(liveNodes, recoveringHash));
    assertEquals(active, plan.nextRouteCandidate());
  }

  @Test
  public void quarantinePromotesAfterSuccessfulTraffic() {
    AlternatorLiveNodes liveNodes =
        liveNodes(
            NodeHealthConfig.builder()
                .withDownNodeRecoverySuccessThreshold(1)
                .withQuarantineSuccessThreshold(2)
                .build(),
            new SystemTableHttpClient());
    URI recovering = node("recovering.local");

    markNodeDown(liveNodes, recovering);
    liveNodes.runDownNodeProbes();
    assertEquals(Arrays.asList(recovering), liveNodes.getQuarantinedNodes());

    liveNodes.reportNodeResult(recovering, NodeHealthObservation.TRAFFIC_SUCCESS);

    assertEquals(Arrays.asList(recovering), liveNodes.getQuarantinedNodes());

    liveNodes.reportNodeResult(recovering, NodeHealthObservation.TRAFFIC_SUCCESS);

    assertTrue(liveNodes.getQuarantinedNodes().isEmpty());
    assertTrue(liveNodes.getDownNodes().isEmpty());
    assertEquals(Arrays.asList(node("active.local"), recovering), liveNodes.getActiveNodes());
  }

  @Test
  public void backgroundRecoversDownNodeAndActivatesItWithDirectProbe() throws Exception {
    SystemTableHttpClient httpClient = new SystemTableHttpClient();
    AlternatorLiveNodes liveNodes =
        liveNodes(
            NodeHealthConfig.builder()
                .withDownNodeProbePeriodMs(10)
                .withQuarantineSuccessThreshold(2)
                .build(),
            httpClient);
    URI recovering = node("recovering.local");

    markNodeDown(liveNodes, recovering);
    liveNodes.start();
    try {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (!liveNodes.getActiveNodes().contains(recovering) && System.nanoTime() < deadline) {
        Thread.sleep(10);
      }
    } finally {
      liveNodes.shutdownAndWait();
    }

    assertFalse(httpClient.requests.isEmpty());
    assertTrue(liveNodes.getActiveNodes().contains(recovering));
  }

  @Test
  public void backgroundDirectlyProbesAndActivatesNewlyDiscoveredNode() throws Exception {
    SystemTableHttpClient httpClient = new SystemTableHttpClient("active.local", "new.local");
    AlternatorLiveNodes liveNodes =
        liveNodes(
            NodeHealthConfig.builder().withDownNodeProbePeriodMs(10).build(),
            httpClient,
            null,
            Arrays.asList("active.local"));
    URI discovered = node("new.local");

    liveNodes.start();
    try {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (!liveNodes.getActiveNodes().contains(discovered) && System.nanoTime() < deadline) {
        Thread.sleep(10);
      }
    } finally {
      liveNodes.shutdownAndWait();
    }

    assertTrue(liveNodes.getActiveNodes().contains(discovered));
    assertTrue(
        httpClient.requests.stream().anyMatch(request -> "new.local".equals(request.host())));
  }

  @Test
  public void refreshPublishesDiscoveredNodesInSortedOrder() throws Exception {
    SystemTableHttpClient httpClient = new SystemTableHttpClient("z.local", "a.local");
    AlternatorLiveNodes liveNodes = liveNodes(NodeHealthConfig.getDefault(), httpClient);

    liveNodes.refreshDiscoveredNodes();

    assertEquals(Arrays.asList(node("a.local"), node("z.local")), liveNodes.getDiscoveredNodes());
  }

  @Test
  public void systemTableNonOkDoesNotMarkActiveSeedDown() throws Exception {
    SystemTableHttpClient httpClient = new SystemTableHttpClient(400);
    AlternatorLiveNodes liveNodes =
        liveNodes(
            NodeHealthConfig.builder().withConsecutiveFailureThreshold(1).build(),
            httpClient,
            null,
            Arrays.asList("active.local"));

    liveNodes.refreshDiscoveredNodes();

    assertEquals(Arrays.asList(node("active.local")), liveNodes.getActiveNodes());
    assertTrue(liveNodes.getDownNodes().isEmpty());
  }

  @Test
  public void duplicateSeedDiscoveryFailureRunsOncePerRefresh() throws Exception {
    SystemTableHttpClient httpClient = new SystemTableHttpClient(400);
    AlternatorLiveNodes liveNodes =
        liveNodes(
            NodeHealthConfig.builder().withConsecutiveFailureThreshold(2).build(),
            httpClient,
            null,
            Arrays.asList("active.local", "active.local"));
    URI active = node("active.local");

    liveNodes.refreshDiscoveredNodes();

    assertTrue(liveNodes.getDownNodes().isEmpty());
    assertEquals(0, liveNodes.getNodeHealthStatus(active).getConsecutiveFailures());
    assertEquals(1, httpClient.requests.size());

    liveNodes.refreshDiscoveredNodes();

    assertEquals(Arrays.asList(active), liveNodes.getActiveNodes());
    assertTrue(liveNodes.getDownNodes().isEmpty());
    assertEquals(0, liveNodes.getNodeHealthStatus(active).getConsecutiveFailures());
  }

  @Test
  public void systemTableRuntimeFailureDoesNotMarkActiveSeedDown() throws Exception {
    RuntimeFailureHttpClient httpClient = new RuntimeFailureHttpClient();
    AlternatorLiveNodes liveNodes =
        liveNodes(
            NodeHealthConfig.builder().withConsecutiveFailureThreshold(1).build(),
            httpClient,
            null,
            Arrays.asList("active.local"));

    liveNodes.refreshDiscoveredNodes();

    assertEquals(Arrays.asList(node("active.local")), liveNodes.getActiveNodes());
    assertTrue(liveNodes.getDownNodes().isEmpty());
  }

  @Test
  public void systemTableInvalidBodyDoesNotMarkActiveSeedDown() throws Exception {
    SystemTableHttpClient httpClient = SystemTableHttpClient.withRawResponse("not-json");
    AlternatorLiveNodes liveNodes =
        liveNodes(
            NodeHealthConfig.builder().withConsecutiveFailureThreshold(1).build(),
            httpClient,
            null,
            Arrays.asList("active.local"));

    liveNodes.refreshDiscoveredNodes();

    assertEquals(Arrays.asList(node("active.local")), liveNodes.getActiveNodes());
    assertTrue(liveNodes.getDownNodes().isEmpty());
  }

  @Test
  public void systemTableMissingBodyDoesNotMarkActiveSeedDown() throws Exception {
    SystemTableHttpClient httpClient = SystemTableHttpClient.withRawResponse(null);
    AlternatorLiveNodes liveNodes =
        liveNodes(
            NodeHealthConfig.builder().withConsecutiveFailureThreshold(1).build(),
            httpClient,
            null,
            Arrays.asList("active.local"));

    liveNodes.refreshDiscoveredNodes();

    assertEquals(Arrays.asList(node("active.local")), liveNodes.getActiveNodes());
    assertTrue(liveNodes.getDownNodes().isEmpty());
  }

  @Test
  public void featureCheckRuntimeFailureDoesNotMarkSelectedNodeDown() throws Exception {
    RuntimeFailureHttpClient httpClient = new RuntimeFailureHttpClient();
    AlternatorLiveNodes liveNodes =
        liveNodes(
            NodeHealthConfig.builder().withConsecutiveFailureThreshold(1).build(),
            httpClient,
            null,
            Arrays.asList("active.local"));

    try {
      liveNodes.checkIfRackDatacenterFeatureIsSupported();
      fail("expected feature check failure");
    } catch (AlternatorLiveNodes.FailedToCheck expected) {
      // expected
    }

    assertEquals(Arrays.asList(node("active.local")), liveNodes.getActiveNodes());
    assertTrue(liveNodes.getDownNodes().isEmpty());
  }

  @Test
  public void featureCheckInvalidBodyDoesNotMarkSelectedNodeDown() throws Exception {
    SystemTableHttpClient httpClient = SystemTableHttpClient.withRawResponse("not-json");
    AlternatorLiveNodes liveNodes =
        liveNodes(
            NodeHealthConfig.builder().withConsecutiveFailureThreshold(1).build(),
            httpClient,
            null,
            Arrays.asList("active.local"));

    try {
      liveNodes.checkIfRackDatacenterFeatureIsSupported();
      fail("expected feature check failure");
    } catch (AlternatorLiveNodes.FailedToCheck expected) {
      // expected
    }

    assertEquals(Arrays.asList(node("active.local")), liveNodes.getActiveNodes());
    assertTrue(liveNodes.getDownNodes().isEmpty());
  }

  @Test
  public void rediscoveredNodeKeepsPreviousHealthState() throws Exception {
    SystemTableHttpClient httpClient = new SystemTableHttpClient("transient.local");
    AlternatorLiveNodes liveNodes =
        liveNodes(
            NodeHealthConfig.builder().withConsecutiveFailureThreshold(1).build(),
            httpClient,
            null,
            Arrays.asList("active.local"));
    URI transientNode = node("transient.local");

    liveNodes.refreshDiscoveredNodes();
    markNodeDown(liveNodes, transientNode);
    assertEquals(NodeHealthState.DOWN, liveNodes.getNodeHealthStatus(transientNode).getState());

    httpClient.setTopologyHosts("other.local");
    liveNodes.refreshDiscoveredNodes();
    assertFalse(liveNodes.getDiscoveredNodes().contains(transientNode));
    assertEquals(NodeHealthState.DOWN, liveNodes.getNodeHealthStatus(transientNode).getState());

    httpClient.setTopologyHosts("transient.local");
    liveNodes.refreshDiscoveredNodes();

    assertTrue(liveNodes.getDiscoveredNodes().contains(transientNode));
    assertEquals(NodeHealthState.DOWN, liveNodes.getNodeHealthStatus(transientNode).getState());
    assertFalse(liveNodes.getActiveNodes().contains(transientNode));
  }

  @Test
  public void runDownNodeProbesSkipsDownNodeMissingFromDiscoveredRing() throws Exception {
    SystemTableHttpClient httpClient = new SystemTableHttpClient("transient.local");
    AlternatorLiveNodes liveNodes =
        liveNodes(
            NodeHealthConfig.builder()
                .withConsecutiveFailureThreshold(1)
                .withDownNodeRecoverySuccessThreshold(1)
                .build(),
            httpClient,
            null,
            Arrays.asList("active.local"));
    URI transientNode = node("transient.local");

    liveNodes.refreshDiscoveredNodes();
    markNodeDown(liveNodes, transientNode);
    assertEquals(NodeHealthState.DOWN, liveNodes.getNodeHealthStatus(transientNode).getState());

    httpClient.setTopologyHosts("other.local");
    liveNodes.refreshDiscoveredNodes();
    httpClient.clearCapturedRequests();

    assertTrue(liveNodes.runDownNodeProbes().isEmpty());
    assertTrue(httpClient.requests.isEmpty());
    assertEquals(NodeHealthState.DOWN, liveNodes.getNodeHealthStatus(transientNode).getState());
  }

  @Test
  public void runDownNodeProbesProbesDownSeedMissingFromDiscoveredRing() throws Exception {
    SystemTableHttpClient httpClient = new SystemTableHttpClient(Arrays.asList("active.local"), 1);
    AlternatorLiveNodes liveNodes =
        liveNodes(
            NodeHealthConfig.builder()
                .withConsecutiveFailureThreshold(1)
                .withDownNodeRecoverySuccessThreshold(1)
                .build(),
            httpClient,
            null,
            Arrays.asList("seed.local", "active.local"));
    URI seed = node("seed.local");

    liveNodes.refreshDiscoveredNodes();
    assertFalse(liveNodes.getDiscoveredNodes().contains(seed));
    markNodeDown(liveNodes, seed);
    assertEquals(NodeHealthState.DOWN, liveNodes.getNodeHealthStatus(seed).getState());
    httpClient.clearCapturedRequests();

    assertEquals(Arrays.asList(seed), liveNodes.runDownNodeProbes());

    assertEquals(1, httpClient.requests.size());
    assertEquals("seed.local", httpClient.requests.get(0).host());
    assertEquals(NodeHealthState.QUARANTINED, liveNodes.getNodeHealthStatus(seed).getState());
  }

  @Test
  public void duplicateDiscoveredDownNodeProbeCountsOncePerCycle() throws Exception {
    SystemTableHttpClient httpClient =
        new SystemTableHttpClient("recovering.local", "recovering.local");
    AlternatorLiveNodes liveNodes =
        liveNodes(
            NodeHealthConfig.builder()
                .withConsecutiveFailureThreshold(1)
                .withDownNodeRecoverySuccessThreshold(2)
                .build(),
            httpClient,
            null,
            Arrays.asList("active.local"));
    URI recovering = node("recovering.local");

    liveNodes.refreshDiscoveredNodes();
    markNodeDown(liveNodes, recovering);
    httpClient.clearCapturedRequests();

    assertTrue(liveNodes.runDownNodeProbes().isEmpty());

    assertEquals(1, liveNodes.getNodeHealthStatus(recovering).getConsecutiveSuccesses());
    assertEquals(1, httpClient.requests.size());

    assertEquals(Arrays.asList(recovering), liveNodes.runDownNodeProbes());
  }

  @Test
  public void omittedActiveNodeDoesNotMakeDownDiscoveredNodeEligible() throws Exception {
    SystemTableHttpClient httpClient = new SystemTableHttpClient("stale-active.local");
    AlternatorLiveNodes liveNodes =
        liveNodes(
            NodeHealthConfig.builder().withConsecutiveFailureThreshold(1).build(),
            httpClient,
            null,
            Arrays.asList("seed.local"));
    URI seed = node("seed.local");
    URI current = node("current.local");

    liveNodes.refreshDiscoveredNodes();
    liveNodes.probeQuarantinedNodes();
    assertTrue(liveNodes.getActiveNodes().contains(node("stale-active.local")));

    httpClient.setTopologyHosts("current.local");
    liveNodes.refreshDiscoveredNodes();
    markNodeDown(liveNodes, seed);
    markNodeDown(liveNodes, current);

    assertTrue(liveNodes.getActiveNodes().isEmpty());
    NodeHealthQueryPlan plan = liveNodes.newRegularQueryPlan(new LazyQueryPlan(liveNodes));
    assertNull(plan.nextRouteCandidate());
  }

  @Test
  public void downNodeProbeRequiresHttpOk() {
    SystemTableHttpClient httpClient = new SystemTableHttpClient(403);
    AlternatorLiveNodes liveNodes =
        liveNodes(
            NodeHealthConfig.builder().withConsecutiveFailureThreshold(1).build(), httpClient);
    URI recovering = node("recovering.local");

    markNodeDown(liveNodes, recovering);

    assertTrue(liveNodes.runDownNodeProbes().isEmpty());
    assertEquals(Arrays.asList(recovering), liveNodes.getDownNodes());
  }

  @Test
  public void clusterRefreshUsesDownNodeAsHealthNeutralControlPlaneFallback() throws Exception {
    SystemTableHttpClient httpClient =
        new SystemTableHttpClient(
            Arrays.asList("active.local", "recovering.local"),
            new HashSet<>(Arrays.asList("active.local")));
    AlternatorLiveNodes liveNodes =
        liveNodes(NodeHealthConfig.builder().withDownNodeProbePeriodMs(1).build(), httpClient);
    URI recovering = node("recovering.local");

    markNodeDown(liveNodes, recovering);
    httpClient.clearCapturedRequests();
    liveNodes.refreshDiscoveredNodes();

    assertEquals(3, httpClient.requests.size());
    assertEquals("active.local", httpClient.requests.get(0).host());
    assertEquals("recovering.local", httpClient.requests.get(1).host());
    assertEquals("recovering.local", httpClient.requests.get(2).host());
    assertEquals(Arrays.asList(recovering), liveNodes.getDownNodes());
  }

  @Test
  public void backgroundDiscoveryDoesNotMarkClientActivity() throws Exception {
    SystemTableHttpClient httpClient =
        new SystemTableHttpClient("active.local", "recovering.local");
    AlternatorLiveNodes liveNodes =
        liveNodes(NodeHealthConfig.getDefault(), httpClient, DatacenterScope.of("dc1", null));

    assertEquals(0, lastActivityTime(liveNodes).get());
    liveNodes.refreshDiscoveredNodes();

    assertEquals(0, lastActivityTime(liveNodes).get());
  }

  @Test
  public void scopedRefreshDoesNotChangeActiveThenQuarantineQueryOrder() throws Exception {
    SystemTableHttpClient httpClient =
        new SystemTableHttpClient("active.local", "recovering.local");
    AlternatorLiveNodes liveNodes =
        liveNodes(
            NodeHealthConfig.builder()
                .withDownNodeRecoverySuccessThreshold(1)
                .withQuarantineSuccessThreshold(3)
                .build(),
            httpClient,
            DatacenterScope.of("dc1", null));
    URI active = node("active.local");
    URI recovering = node("recovering.local");

    markNodeDown(liveNodes, recovering);
    liveNodes.runDownNodeProbes();
    assertEquals(Arrays.asList(recovering), liveNodes.getQuarantinedNodes());

    liveNodes.refreshDiscoveredNodes();

    assertEquals(Arrays.asList(active, recovering), liveNodes.getQueryPlanNodes());
    NodeHealthQueryPlan firstPlan =
        liveNodes.newAffinityQueryPlan(
            new LazyQueryPlan(liveNodes, Arrays.asList(active, recovering)));
    assertEquals(active, firstPlan.nextRouteCandidate());
    NodeHealthQueryPlan secondPlan =
        liveNodes.newAffinityQueryPlan(
            new LazyQueryPlan(liveNodes, Arrays.asList(active, recovering)));
    assertEquals(active, secondPlan.nextRouteCandidate());
    assertEquals(recovering, secondPlan.nextRouteCandidate());
  }

  private static AlternatorLiveNodes liveNodes(
      NodeHealthConfig nodeHealthConfig, SdkHttpClient httpClient) {
    return liveNodes(nodeHealthConfig, httpClient, null);
  }

  private static AlternatorLiveNodes liveNodes(
      NodeHealthConfig nodeHealthConfig, SdkHttpClient httpClient, RoutingScope routingScope) {
    return liveNodes(
        nodeHealthConfig,
        httpClient,
        routingScope,
        Arrays.asList("active.local", "recovering.local"));
  }

  private static AlternatorLiveNodes liveNodes(
      NodeHealthConfig nodeHealthConfig,
      SdkHttpClient httpClient,
      RoutingScope routingScope,
      List<String> seedHosts) {
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedHosts(seedHosts)
            .withScheme("http")
            .withPort(8080)
            .withActiveRefreshIntervalMs(60_000)
            .withIdleRefreshIntervalMs(60_000)
            .withRoutingScope(routingScope)
            .withNodeHealthConfig(nodeHealthConfig)
            .build();
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, httpClient);
    for (String seedHost : seedHosts) {
      liveNodes.reportNodeResult(node(seedHost), NodeHealthObservation.PROBE_SUCCESS, false);
    }
    return liveNodes;
  }

  private static long hashWhereFirstNodeIs(List<URI> nodes, URI expected) {
    for (long hash = -1000; hash < 1000; hash++) {
      if (expected.equals(AlternatorLiveNodes.firstNodeWithSeed(nodes, hash))) {
        return hash;
      }
    }
    throw new AssertionError("Could not find hash for " + expected);
  }

  private static URI node(String host) {
    return URI.create("http://" + host + ":8080");
  }

  private static void markNodeDown(AlternatorLiveNodes liveNodes, URI node) {
    for (int i = 0; i < NodeHealthConfig.DEFAULT_CONSECUTIVE_FAILURE_THRESHOLD; i++) {
      liveNodes.reportNodeResult(node, NodeHealthObservation.TRAFFIC_FAILURE);
    }
  }

  private static List<URI> collectNodes(LazyQueryPlan plan) {
    List<URI> nodes = new java.util.ArrayList<>();
    while (plan.hasNext()) {
      nodes.add(plan.next());
    }
    return nodes;
  }

  private static AtomicLong lastActivityTime(AlternatorLiveNodes liveNodes) throws Exception {
    Field field = AlternatorLiveNodes.class.getDeclaredField("lastActivityTime");
    field.setAccessible(true);
    return (AtomicLong) field.get(liveNodes);
  }

  private static void assertScanTargetsTable(
      SystemTableHttpClient client, int requestIndex, String tableName) {
    assertTrue(
        client.requestBodies.get(requestIndex).contains("\"TableName\":\"" + tableName + "\""));
  }

  private static void assertDirectProbe(SystemTableHttpClient client, int requestIndex) {
    assertScanTargetsTable(client, requestIndex, SystemTableTopologyDiscovery.LOCAL_TABLE);
    assertTrue(client.requestBodies.get(requestIndex).contains("\"Select\":\"COUNT\""));
    assertTrue(client.requestBodies.get(requestIndex).contains("\"Limit\":1"));
  }

  private static final class SystemTableHttpClient implements SdkHttpClient {
    private final List<SdkHttpRequest> requests = new CopyOnWriteArrayList<>();
    private final List<String> requestBodies = new CopyOnWriteArrayList<>();
    private volatile String topologyResponseBody;
    private final Set<String> failingHosts;
    private final int statusCode;
    private final AtomicLong requestsToFail;

    private SystemTableHttpClient(String... topologyHosts) {
      this(scanResponse(Arrays.asList(topologyHosts)), Collections.emptySet(), 200, 0);
    }

    private SystemTableHttpClient(int statusCode) {
      this(scanResponse(Collections.emptyList()), Collections.emptySet(), statusCode, 0);
    }

    private SystemTableHttpClient(List<String> topologyHosts, int requestsToFail) {
      this(scanResponse(topologyHosts), Collections.emptySet(), 200, requestsToFail);
    }

    private SystemTableHttpClient(List<String> topologyHosts, Set<String> failingHosts) {
      this(scanResponse(topologyHosts), failingHosts, 200, 0);
    }

    private SystemTableHttpClient(
        String topologyResponseBody,
        Set<String> failingHosts,
        int statusCode,
        long requestsToFail) {
      this.topologyResponseBody = topologyResponseBody;
      this.failingHosts = new HashSet<>(failingHosts);
      this.statusCode = statusCode;
      this.requestsToFail = new AtomicLong(requestsToFail);
    }

    private static SystemTableHttpClient withRawResponse(String responseBody) {
      return new SystemTableHttpClient(responseBody, Collections.emptySet(), 200, 0);
    }

    private void setTopologyHosts(String... topologyHosts) {
      this.topologyResponseBody = scanResponse(Arrays.asList(topologyHosts));
    }

    private synchronized void clearCapturedRequests() {
      requests.clear();
      requestBodies.clear();
    }

    private static String scanResponse(List<String> topologyHosts) {
      StringBuilder response = new StringBuilder("{\"Items\":[");
      for (int i = 0; i < topologyHosts.size(); i++) {
        if (i > 0) {
          response.append(',');
        }
        response
            .append("{\"rpc_address\":{\"S\":\"")
            .append(topologyHosts.get(i))
            .append("\"},\"data_center\":{\"S\":\"dc1\"},\"rack\":{\"S\":\"rack1\"}}");
      }
      return response
          .append("],\"Count\":")
          .append(topologyHosts.size())
          .append(",\"ScannedCount\":")
          .append(topologyHosts.size())
          .append('}')
          .toString();
    }

    @Override
    public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
      SdkHttpRequest httpRequest = request.httpRequest();
      String requestBody = readRequestBody(request);
      synchronized (this) {
        requests.add(httpRequest);
        requestBodies.add(requestBody);
      }
      return new ExecutableHttpRequest() {
        @Override
        public HttpExecuteResponse call() throws IOException {
          if (requestsToFail.getAndUpdate(value -> value > 0 ? value - 1 : value) > 0) {
            throw new IOException("simulated connection failure");
          }
          if (failingHosts.contains(httpRequest.host())) {
            throw new IOException("simulated connection failure for " + httpRequest.host());
          }
          HttpExecuteResponse.Builder response =
              HttpExecuteResponse.builder()
                  .response(SdkHttpFullResponse.builder().statusCode(statusCode).build());
          String responseBody;
          if (requestBody.contains("\"Select\":\"COUNT\"")) {
            responseBody = "{\"Count\":1,\"ScannedCount\":1}";
          } else if (requestBody.contains(
              "\"TableName\":\"" + SystemTableTopologyDiscovery.PEERS_TABLE + "\"")) {
            responseBody = scanResponse(Collections.emptyList());
          } else {
            responseBody = topologyResponseBody;
          }
          if (responseBody != null) {
            byte[] body = responseBody.getBytes(StandardCharsets.UTF_8);
            response.responseBody(AbortableInputStream.create(new ByteArrayInputStream(body)));
          }
          return response.build();
        }

        @Override
        public void abort() {}
      };
    }

    @Override
    public void close() {}

    @Override
    public String clientName() {
      return "system-tables";
    }

    private static String readRequestBody(HttpExecuteRequest request) {
      if (!request.contentStreamProvider().isPresent()) {
        return "";
      }
      try (InputStream input = request.contentStreamProvider().get().newStream();
          ByteArrayOutputStream output = new ByteArrayOutputStream()) {
        byte[] buffer = new byte[256];
        int read;
        while ((read = input.read(buffer)) != -1) {
          output.write(buffer, 0, read);
        }
        return new String(output.toByteArray(), StandardCharsets.UTF_8);
      } catch (IOException e) {
        throw new IllegalStateException("failed to capture DynamoDB request", e);
      }
    }
  }

  private static final class RuntimeFailureHttpClient implements SdkHttpClient {
    @Override
    public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
      return new ExecutableHttpRequest() {
        @Override
        public HttpExecuteResponse call() {
          throw new IllegalStateException("simulated runtime transport failure");
        }

        @Override
        public void abort() {}
      };
    }

    @Override
    public void close() {}

    @Override
    public String clientName() {
      return "runtime-failure";
    }
  }
}
