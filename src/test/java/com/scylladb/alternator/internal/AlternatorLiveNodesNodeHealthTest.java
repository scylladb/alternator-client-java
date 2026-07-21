package com.scylladb.alternator.internal;

import static org.junit.Assert.*;

import com.scylladb.alternator.AlternatorConfig;
import com.scylladb.alternator.NodeHealthConfig;
import com.scylladb.alternator.NodeHealthObservation;
import com.scylladb.alternator.NodeHealthState;
import com.scylladb.alternator.routing.DatacenterScope;
import com.scylladb.alternator.routing.RoutingScope;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
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
  public void runDownNodeProbesMovesRecoveredNodeToQuarantine() {
    AlternatorLiveNodes liveNodes =
        liveNodes(
            NodeHealthConfig.builder().withDownNodeRecoverySuccessThreshold(1).build(),
            new LocalNodesHttpClient());
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
            new LocalNodesHttpClient());
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
  public void queryPlanFilterSamplesQuarantineByConfiguredInterval() {
    AlternatorLiveNodes liveNodes =
        liveNodes(
            NodeHealthConfig.builder()
                .withDownNodeRecoverySuccessThreshold(1)
                .withQuarantineSuccessThreshold(3)
                .withQuarantineTrafficInterval(2)
                .build(),
            new LocalNodesHttpClient());
    URI active = node("active.local");
    URI recovering = node("recovering.local");

    markNodeDown(liveNodes, recovering);
    liveNodes.runDownNodeProbes();

    assertEquals(Arrays.asList(active, recovering), liveNodes.getQueryPlanNodes());

    AlternatorLiveNodes.QueryPlanNodeFilter firstFilter = liveNodes.newQueryPlanNodeFilter();
    assertTrue(firstFilter.shouldRouteTo(active));
    assertFalse(firstFilter.shouldRouteTo(recovering));

    AlternatorLiveNodes.QueryPlanNodeFilter secondFilter = liveNodes.newQueryPlanNodeFilter();
    assertTrue(secondFilter.shouldRouteTo(active));
    assertTrue(secondFilter.shouldRouteTo(recovering));

    AlternatorLiveNodes.QueryPlanNodeFilter thirdFilter = liveNodes.newQueryPlanNodeFilter();
    assertTrue(thirdFilter.shouldRouteTo(active));
    assertFalse(thirdFilter.shouldRouteTo(recovering));
  }

  @Test
  public void queryPlanFilterAllowsAllDownNodesWhenNoActiveNodes() {
    AlternatorLiveNodes liveNodes =
        liveNodes(NodeHealthConfig.getDefault(), new LocalNodesHttpClient());
    URI first = node("active.local");
    URI second = node("recovering.local");

    markNodeDown(liveNodes, first);
    markNodeDown(liveNodes, second);

    AlternatorLiveNodes.QueryPlanNodeFilter filter = liveNodes.newQueryPlanNodeFilter();

    assertTrue(liveNodes.getActiveNodes().isEmpty());
    assertTrue(filter.shouldRouteTo(first));
    assertTrue(filter.shouldRouteTo(second));
    assertEquals(NodeHealthState.DOWN, liveNodes.getNodeHealthStatus(first).getState());
    assertEquals(NodeHealthState.DOWN, liveNodes.getNodeHealthStatus(second).getState());
  }

  @Test
  public void queryPlanFilterFailsOpenAfterLastActiveNodeGoesDown() {
    AlternatorLiveNodes liveNodes =
        liveNodes(NodeHealthConfig.getDefault(), new LocalNodesHttpClient());
    URI active = node("active.local");
    URI down = node("recovering.local");

    markNodeDown(liveNodes, down);
    AlternatorLiveNodes.QueryPlanNodeFilter filter = liveNodes.newQueryPlanNodeFilter();

    assertFalse(filter.shouldRouteTo(down));

    markNodeDown(liveNodes, active);

    assertTrue(liveNodes.getActiveNodes().isEmpty());
    assertTrue(filter.shouldRouteTo(down));
    assertTrue(filter.shouldRouteTo(active));
  }

  @Test
  public void queryPlanFilterAllowsAllQuarantinedNodesWhenNoActiveNodes() {
    AlternatorLiveNodes liveNodes =
        liveNodes(
            NodeHealthConfig.builder().withDownNodeRecoverySuccessThreshold(1).build(),
            new LocalNodesHttpClient());
    URI first = node("active.local");
    URI second = node("recovering.local");

    markNodeDown(liveNodes, first);
    markNodeDown(liveNodes, second);
    liveNodes.runDownNodeProbes();

    AlternatorLiveNodes.QueryPlanNodeFilter filter = liveNodes.newQueryPlanNodeFilter();

    assertTrue(liveNodes.getActiveNodes().isEmpty());
    assertTrue(filter.shouldRouteTo(first));
    assertTrue(filter.shouldRouteTo(second));
    assertEquals(NodeHealthState.QUARANTINED, liveNodes.getNodeHealthStatus(first).getState());
    assertEquals(NodeHealthState.QUARANTINED, liveNodes.getNodeHealthStatus(second).getState());
  }

  @Test
  public void featureCheckUsesRandomQueryPlanNode() throws Exception {
    LocalNodesHttpClient httpClient = new LocalNodesHttpClient("[\"active.local\"]");
    AlternatorLiveNodes liveNodes = liveNodes(NodeHealthConfig.getDefault(), httpClient);
    URI recovering = node("recovering.local");

    markNodeDown(liveNodes, recovering);

    assertFalse(liveNodes.checkIfRackDatacenterFeatureIsSupported());

    assertEquals(2, httpClient.requests.size());
    assertEquals("active.local", httpClient.requests.get(0).host());
    assertEquals("active.local", httpClient.requests.get(1).host());
    assertEquals("/localnodes", httpClient.requests.get(0).encodedPath());
    assertEquals("/localnodes", httpClient.requests.get(1).encodedPath());
    assertTrue(httpClient.requests.get(0).rawQueryParameters().containsKey("rack"));
    assertTrue(httpClient.requests.get(1).rawQueryParameters().isEmpty());
  }

  @Test
  public void featureCheckTriesNextQueryPlanNodeWhenOneNodeFails() throws Exception {
    LocalNodesHttpClient httpClient = new LocalNodesHttpClient("[\"active.local\"]", 1);
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedHosts(Arrays.asList("node-a.local", "node-b.local"))
            .withScheme("http")
            .withPort(8080)
            .withActiveRefreshIntervalMs(60_000)
            .withIdleRefreshIntervalMs(60_000)
            .build();
    AlternatorLiveNodes liveNodes =
        new AlternatorLiveNodes(config, httpClient) {
          @Override
          public List<URI> getQueryPlanNodes() {
            return Arrays.asList(node("node-a.local"), node("node-b.local"));
          }
        };

    assertFalse(liveNodes.checkIfRackDatacenterFeatureIsSupported());

    assertEquals(3, httpClient.requests.size());
    assertTrue(httpClient.requests.get(0).rawQueryParameters().containsKey("rack"));
    assertNotEquals(httpClient.requests.get(0).host(), httpClient.requests.get(1).host());
    assertEquals(httpClient.requests.get(1).host(), httpClient.requests.get(2).host());
    assertTrue(httpClient.requests.get(1).rawQueryParameters().containsKey("rack"));
    assertTrue(httpClient.requests.get(2).rawQueryParameters().isEmpty());
  }

  @Test
  public void featureCheckSuccessClearsPreviousProbeFailures() throws Exception {
    LocalNodesHttpClient httpClient = new LocalNodesHttpClient("[\"active.local\"]", 1);
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
    assertEquals(1, liveNodes.getNodeHealthStatus(active).getConsecutiveFailures());

    assertFalse(liveNodes.checkIfRackDatacenterFeatureIsSupported());

    assertEquals(0, liveNodes.getNodeHealthStatus(active).getConsecutiveFailures());
    assertTrue(liveNodes.getDownNodes().isEmpty());
  }

  @Test
  public void featureCheckRejectsNullQueryPlanNodeWithoutNpe() throws Exception {
    LocalNodesHttpClient httpClient = new LocalNodesHttpClient();
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedHosts(Arrays.asList("active.local"))
            .withScheme("http")
            .withPort(8080)
            .withActiveRefreshIntervalMs(60_000)
            .withIdleRefreshIntervalMs(60_000)
            .build();
    AlternatorLiveNodes liveNodes =
        new AlternatorLiveNodes(config, httpClient) {
          @Override
          public List<URI> getQueryPlanNodes() {
            return Arrays.asList((URI) null);
          }
        };

    try {
      liveNodes.checkIfRackDatacenterFeatureIsSupported();
      fail("Expected FailedToCheck");
    } catch (AlternatorLiveNodes.FailedToCheck e) {
      assertEquals("query plan returned null node", e.getMessage());
    }
    assertTrue(httpClient.requests.isEmpty());
  }

  @Test
  public void normalQueryPlanContainsQuarantinedNodeAndFilterAdmitsSample() {
    AlternatorLiveNodes liveNodes =
        liveNodes(
            NodeHealthConfig.builder()
                .withDownNodeRecoverySuccessThreshold(1)
                .withQuarantineSuccessThreshold(3)
                .withQuarantineTrafficInterval(1)
                .build(),
            new LocalNodesHttpClient());
    URI active = node("active.local");
    URI recovering = node("recovering.local");

    markNodeDown(liveNodes, recovering);
    liveNodes.runDownNodeProbes();

    LazyQueryPlan plan = new LazyQueryPlan(liveNodes);
    List<URI> selected = Arrays.asList(plan.next(), plan.next());
    AlternatorLiveNodes.QueryPlanNodeFilter filter = liveNodes.newQueryPlanNodeFilter();

    assertTrue(selected.contains(recovering));
    assertTrue(selected.contains(active));
    assertFalse(plan.hasNext());
    assertTrue(filter.shouldRouteTo(recovering));
  }

  @Test
  public void randomQueryPlanKeepsQuarantinedCandidateInKnownNodeSet() {
    AlternatorLiveNodes liveNodes =
        liveNodes(
            NodeHealthConfig.builder()
                .withDownNodeRecoverySuccessThreshold(1)
                .withQuarantineSuccessThreshold(3)
                .withQuarantineTrafficInterval(1)
                .build(),
            new LocalNodesHttpClient(),
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
  public void quarantineHashSamplingUsesSeededPlanOrderUntilVerified() {
    AlternatorLiveNodes liveNodes =
        liveNodes(
            NodeHealthConfig.builder()
                .withDownNodeRecoverySuccessThreshold(1)
                .withQuarantineSuccessThreshold(2)
                .withQuarantineTrafficInterval(2)
                .build(),
            new LocalNodesHttpClient());
    URI active = node("active.local");
    URI recovering = node("recovering.local");
    long activeHash = hashWhereFirstNodeIs(Arrays.asList(active, recovering), active);
    long recoveringHash = hashWhereFirstNodeIs(Arrays.asList(active, recovering), recovering);

    markNodeDown(liveNodes, recovering);
    liveNodes.runDownNodeProbes();

    assertEquals(
        Arrays.asList(recovering, active), liveNodes.getQueryPlanNodesForHash(recoveringHash));
    AlternatorLiveNodes.QueryPlanNodeFilter firstFilter = liveNodes.newQueryPlanNodeFilter();
    assertFalse(firstFilter.shouldRouteTo(recovering));
    assertTrue(firstFilter.shouldRouteTo(active));

    AlternatorLiveNodes.QueryPlanNodeFilter secondFilter = liveNodes.newQueryPlanNodeFilter();
    assertTrue(secondFilter.shouldRouteTo(recovering));
    liveNodes.reportNodeResult(recovering, NodeHealthObservation.TRAFFIC_SUCCESS);

    assertEquals(Arrays.asList(active, recovering), liveNodes.getQueryPlanNodesForHash(activeHash));
    liveNodes.reportNodeResult(recovering, NodeHealthObservation.TRAFFIC_SUCCESS);

    assertTrue(liveNodes.getQuarantinedNodes().isEmpty());
    assertEquals(NodeHealthState.ACTIVE, liveNodes.getNodeHealthStatus(recovering).getState());
    assertEquals(Arrays.asList(active, recovering), liveNodes.getActiveNodes());
    assertEquals(
        Arrays.asList(recovering, active), liveNodes.getQueryPlanNodesForHash(recoveringHash));
  }

  @Test
  public void queryPlanFilterCountsQuarantineSamplingWhenQuarantinedNodeReached() throws Exception {
    AlternatorLiveNodes liveNodes =
        liveNodes(
            NodeHealthConfig.builder()
                .withDownNodeRecoverySuccessThreshold(1)
                .withQuarantineSuccessThreshold(3)
                .withQuarantineTrafficInterval(2)
                .build(),
            new LocalNodesHttpClient());
    URI active = node("active.local");
    URI recovering = node("recovering.local");
    long activeHash = hashWhereFirstNodeIs(Arrays.asList(active, recovering), active);

    markNodeDown(liveNodes, recovering);
    liveNodes.runDownNodeProbes();

    assertEquals(Arrays.asList(active, recovering), liveNodes.getQueryPlanNodesForHash(activeHash));
    AlternatorLiveNodes.QueryPlanNodeFilter firstFilter = liveNodes.newQueryPlanNodeFilter();
    assertTrue(firstFilter.shouldRouteTo(active));
    assertEquals(0, quarantineSamplingCounter(liveNodes).get());
    assertFalse(firstFilter.shouldRouteTo(recovering));
    assertEquals(1, quarantineSamplingCounter(liveNodes).get());

    assertEquals(Arrays.asList(active, recovering), liveNodes.getQueryPlanNodesForHash(activeHash));
    AlternatorLiveNodes.QueryPlanNodeFilter secondFilter = liveNodes.newQueryPlanNodeFilter();
    assertTrue(secondFilter.shouldRouteTo(active));
    assertEquals(1, quarantineSamplingCounter(liveNodes).get());
    assertTrue(secondFilter.shouldRouteTo(recovering));
    assertEquals(2, quarantineSamplingCounter(liveNodes).get());
  }

  @Test
  @SuppressWarnings("deprecation")
  public void deprecatedNextAsUriSamplesQuarantinedNodeByInterval() throws Exception {
    AlternatorLiveNodes liveNodes =
        liveNodes(
            NodeHealthConfig.builder()
                .withDownNodeRecoverySuccessThreshold(1)
                .withQuarantineSuccessThreshold(3)
                .withQuarantineTrafficInterval(2)
                .build(),
            new LocalNodesHttpClient());
    URI active = node("active.local");
    URI recovering = node("recovering.local");

    markNodeDown(liveNodes, recovering);
    liveNodes.runDownNodeProbes();

    assertEquals(active, liveNodes.nextAsURI());
    assertEquals(1, quarantineSamplingCounter(liveNodes).get());
    assertEquals(recovering, liveNodes.nextAsURI());
    assertEquals(2, quarantineSamplingCounter(liveNodes).get());
  }

  @Test
  public void preferredQueryPlanNodeForHashKeepsQuarantinedPreferredNode() {
    AlternatorLiveNodes liveNodes =
        liveNodes(
            NodeHealthConfig.builder()
                .withDownNodeRecoverySuccessThreshold(1)
                .withQuarantineSuccessThreshold(3)
                .withQuarantineTrafficInterval(1)
                .build(),
            new LocalNodesHttpClient());
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
                .withQuarantineTrafficInterval(1)
                .build(),
            new LocalNodesHttpClient());
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
    AlternatorLiveNodes.QueryPlanNodeFilter filter = liveNodes.newQueryPlanNodeFilter();
    assertFalse(filter.shouldRouteTo(recovering));
    assertTrue(filter.shouldRouteTo(active));
  }

  @Test
  public void quarantinePromotesAfterSuccessfulTraffic() {
    AlternatorLiveNodes liveNodes =
        liveNodes(
            NodeHealthConfig.builder()
                .withDownNodeRecoverySuccessThreshold(1)
                .withQuarantineSuccessThreshold(2)
                .build(),
            new LocalNodesHttpClient());
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
  public void backgroundMovesDownNodeToQuarantine() throws Exception {
    LocalNodesHttpClient httpClient = new LocalNodesHttpClient();
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
      while (liveNodes.getQuarantinedNodes().isEmpty() && System.nanoTime() < deadline) {
        Thread.sleep(10);
      }
    } finally {
      liveNodes.shutdownAndWait();
    }

    assertFalse(httpClient.requests.isEmpty());
    assertEquals(Arrays.asList(recovering), liveNodes.getQuarantinedNodes());
  }

  @Test
  public void refreshPublishesDiscoveredNodesInSortedOrder() throws Exception {
    LocalNodesHttpClient httpClient = new LocalNodesHttpClient("[\"z.local\",\"a.local\"]");
    AlternatorLiveNodes liveNodes = liveNodes(NodeHealthConfig.getDefault(), httpClient);

    liveNodes.refreshDiscoveredNodes();

    assertEquals(Arrays.asList(node("a.local"), node("z.local")), liveNodes.getDiscoveredNodes());
  }

  @Test
  public void localNodesNonOkMarksSeedDownWhenThresholdReached() throws Exception {
    LocalNodesHttpClient httpClient = new LocalNodesHttpClient(400);
    AlternatorLiveNodes liveNodes =
        liveNodes(
            NodeHealthConfig.builder().withConsecutiveFailureThreshold(1).build(),
            httpClient,
            null,
            Arrays.asList("active.local"));

    liveNodes.refreshDiscoveredNodes();

    assertEquals(Arrays.asList(node("active.local")), liveNodes.getDownNodes());
  }

  @Test
  public void duplicateSeedFailureCountsOncePerRefresh() throws Exception {
    LocalNodesHttpClient httpClient = new LocalNodesHttpClient(400);
    AlternatorLiveNodes liveNodes =
        liveNodes(
            NodeHealthConfig.builder().withConsecutiveFailureThreshold(2).build(),
            httpClient,
            null,
            Arrays.asList("active.local", "active.local"));
    URI active = node("active.local");

    liveNodes.refreshDiscoveredNodes();

    assertTrue(liveNodes.getDownNodes().isEmpty());
    assertEquals(1, liveNodes.getNodeHealthStatus(active).getConsecutiveFailures());
    assertEquals(1, httpClient.requests.size());

    liveNodes.refreshDiscoveredNodes();

    assertEquals(Arrays.asList(active), liveNodes.getDownNodes());
    assertEquals(2, liveNodes.getNodeHealthStatus(active).getConsecutiveFailures());
  }

  @Test
  public void localNodesRuntimeFailureMarksSeedDownWhenThresholdReached() throws Exception {
    RuntimeFailureHttpClient httpClient = new RuntimeFailureHttpClient();
    AlternatorLiveNodes liveNodes =
        liveNodes(
            NodeHealthConfig.builder().withConsecutiveFailureThreshold(1).build(),
            httpClient,
            null,
            Arrays.asList("active.local"));

    liveNodes.refreshDiscoveredNodes();

    assertEquals(Arrays.asList(node("active.local")), liveNodes.getDownNodes());
  }

  @Test
  public void localNodesInvalidBodyMarksSeedDownWhenThresholdReached() throws Exception {
    LocalNodesHttpClient httpClient = new LocalNodesHttpClient("not-json");
    AlternatorLiveNodes liveNodes =
        liveNodes(
            NodeHealthConfig.builder().withConsecutiveFailureThreshold(1).build(),
            httpClient,
            null,
            Arrays.asList("active.local"));

    liveNodes.refreshDiscoveredNodes();

    assertEquals(Arrays.asList(node("active.local")), liveNodes.getDownNodes());
  }

  @Test
  public void featureCheckRuntimeFailureMarksSelectedNodeDown() throws Exception {
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

    assertEquals(Arrays.asList(node("active.local")), liveNodes.getDownNodes());
  }

  @Test
  public void featureCheckInvalidBodyMarksSelectedNodeDown() throws Exception {
    LocalNodesHttpClient httpClient = new LocalNodesHttpClient("not-json");
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

    assertEquals(Arrays.asList(node("active.local")), liveNodes.getDownNodes());
  }

  @Test
  public void rediscoveredNodeKeepsPreviousHealthState() throws Exception {
    LocalNodesHttpClient httpClient = new LocalNodesHttpClient("[\"transient.local\"]");
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

    httpClient.setResponseBody("[\"other.local\"]");
    liveNodes.refreshDiscoveredNodes();
    assertFalse(liveNodes.getDiscoveredNodes().contains(transientNode));
    assertEquals(NodeHealthState.DOWN, liveNodes.getNodeHealthStatus(transientNode).getState());

    httpClient.setResponseBody("[\"transient.local\"]");
    liveNodes.refreshDiscoveredNodes();

    assertTrue(liveNodes.getDiscoveredNodes().contains(transientNode));
    assertEquals(NodeHealthState.DOWN, liveNodes.getNodeHealthStatus(transientNode).getState());
    assertFalse(liveNodes.getLiveNodes().contains(transientNode));
  }

  @Test
  public void runDownNodeProbesSkipsDownNodeMissingFromDiscoveredRing() throws Exception {
    LocalNodesHttpClient httpClient = new LocalNodesHttpClient("[\"transient.local\"]");
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

    httpClient.setResponseBody("[\"other.local\"]");
    liveNodes.refreshDiscoveredNodes();
    httpClient.requests.clear();

    assertTrue(liveNodes.runDownNodeProbes().isEmpty());
    assertTrue(httpClient.requests.isEmpty());
    assertEquals(NodeHealthState.DOWN, liveNodes.getNodeHealthStatus(transientNode).getState());
  }

  @Test
  public void runDownNodeProbesProbesDownSeedMissingFromDiscoveredRing() throws Exception {
    LocalNodesHttpClient httpClient = new LocalNodesHttpClient("[\"active.local\"]", 1);
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
    assertEquals(NodeHealthState.DOWN, liveNodes.getNodeHealthStatus(seed).getState());
    httpClient.requests.clear();

    assertEquals(Arrays.asList(seed), liveNodes.runDownNodeProbes());

    assertEquals(1, httpClient.requests.size());
    assertEquals("seed.local", httpClient.requests.get(0).host());
    assertEquals(NodeHealthState.QUARANTINED, liveNodes.getNodeHealthStatus(seed).getState());
  }

  @Test
  public void duplicateDiscoveredDownNodeProbeCountsOncePerCycle() throws Exception {
    LocalNodesHttpClient httpClient =
        new LocalNodesHttpClient("[\"recovering.local\",\"recovering.local\"]");
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
    httpClient.requests.clear();

    assertTrue(liveNodes.runDownNodeProbes().isEmpty());

    assertEquals(1, liveNodes.getNodeHealthStatus(recovering).getConsecutiveSuccesses());
    assertEquals(1, httpClient.requests.size());

    assertEquals(Arrays.asList(recovering), liveNodes.runDownNodeProbes());
  }

  @Test
  public void omittedActiveNodeDoesNotBlockNoActiveFallback() throws Exception {
    LocalNodesHttpClient httpClient = new LocalNodesHttpClient("[\"stale-active.local\"]");
    AlternatorLiveNodes liveNodes =
        liveNodes(
            NodeHealthConfig.builder().withConsecutiveFailureThreshold(1).build(),
            httpClient,
            null,
            Arrays.asList("seed.local"));
    URI seed = node("seed.local");
    URI current = node("current.local");

    liveNodes.refreshDiscoveredNodes();
    assertTrue(liveNodes.getActiveNodes().contains(node("stale-active.local")));

    httpClient.setResponseBody("[\"current.local\"]");
    liveNodes.refreshDiscoveredNodes();
    markNodeDown(liveNodes, seed);
    markNodeDown(liveNodes, current);

    assertTrue(liveNodes.getActiveNodes().isEmpty());
    AlternatorLiveNodes.QueryPlanNodeFilter filter = liveNodes.newQueryPlanNodeFilter();
    assertTrue(filter.shouldRouteTo(current));
  }

  @Test
  public void downNodeProbeRequiresHttpOk() {
    LocalNodesHttpClient httpClient = new LocalNodesHttpClient(403);
    AlternatorLiveNodes liveNodes =
        liveNodes(
            NodeHealthConfig.builder().withConsecutiveFailureThreshold(1).build(), httpClient);
    URI recovering = node("recovering.local");

    markNodeDown(liveNodes, recovering);

    assertTrue(liveNodes.runDownNodeProbes().isEmpty());
    assertEquals(Arrays.asList(recovering), liveNodes.getDownNodes());
  }

  @Test
  public void refreshDoesNotRunDownNodeProbesWhenBackgroundProbesDisabled() throws Exception {
    LocalNodesHttpClient httpClient =
        new LocalNodesHttpClient("[\"active.local\",\"recovering.local\"]");
    AlternatorLiveNodes liveNodes =
        liveNodes(
            NodeHealthConfig.builder().withDownNodeProbePeriodMs(0).build(),
            httpClient,
            DatacenterScope.of("dc1", null));
    URI recovering = node("recovering.local");

    markNodeDown(liveNodes, recovering);
    httpClient.requests.clear();
    liveNodes.refreshDiscoveredNodes();

    assertEquals(1, httpClient.requests.size());
    assertEquals("active.local", httpClient.requests.get(0).host());
    assertEquals(Arrays.asList(recovering), liveNodes.getDownNodes());
  }

  @Test
  public void clusterRefreshDoesNotProbeDownSeedWhenBackgroundProbesDisabled() throws Exception {
    LocalNodesHttpClient httpClient =
        new LocalNodesHttpClient("[\"active.local\",\"recovering.local\"]");
    AlternatorLiveNodes liveNodes =
        liveNodes(NodeHealthConfig.builder().withDownNodeProbePeriodMs(0).build(), httpClient);
    URI recovering = node("recovering.local");

    markNodeDown(liveNodes, recovering);
    httpClient.requests.clear();
    liveNodes.refreshDiscoveredNodes();

    assertEquals(1, httpClient.requests.size());
    assertEquals("active.local", httpClient.requests.get(0).host());
    assertEquals(Arrays.asList(recovering), liveNodes.getDownNodes());
  }

  @Test
  public void clusterRefreshDoesNotProbeDownSeedAfterProbePeriod() throws Exception {
    LocalNodesHttpClient httpClient =
        new LocalNodesHttpClient("[\"active.local\",\"recovering.local\"]");
    AlternatorLiveNodes liveNodes =
        liveNodes(NodeHealthConfig.builder().withDownNodeProbePeriodMs(1).build(), httpClient);
    URI recovering = node("recovering.local");

    markNodeDown(liveNodes, recovering);
    Thread.sleep(5);
    httpClient.requests.clear();
    liveNodes.refreshDiscoveredNodes();

    assertEquals(1, httpClient.requests.size());
    assertEquals("active.local", httpClient.requests.get(0).host());
    assertEquals(Arrays.asList(recovering), liveNodes.getDownNodes());
  }

  @Test
  public void backgroundDiscoveryDoesNotMarkClientActivity() throws Exception {
    LocalNodesHttpClient httpClient =
        new LocalNodesHttpClient("[\"active.local\",\"recovering.local\"]");
    AlternatorLiveNodes liveNodes =
        liveNodes(NodeHealthConfig.getDefault(), httpClient, DatacenterScope.of("dc1", null));

    assertEquals(0, lastActivityTime(liveNodes).get());
    liveNodes.refreshDiscoveredNodes();

    assertEquals(0, lastActivityTime(liveNodes).get());
  }

  @Test
  public void scopedRefreshDoesNotConsumeQuarantineTrafficSample() throws Exception {
    LocalNodesHttpClient httpClient =
        new LocalNodesHttpClient("[\"active.local\",\"recovering.local\"]");
    AlternatorLiveNodes liveNodes =
        liveNodes(
            NodeHealthConfig.builder()
                .withDownNodeRecoverySuccessThreshold(1)
                .withQuarantineSuccessThreshold(3)
                .withQuarantineTrafficInterval(2)
                .build(),
            httpClient,
            DatacenterScope.of("dc1", null));
    URI active = node("active.local");
    URI recovering = node("recovering.local");

    markNodeDown(liveNodes, recovering);
    liveNodes.runDownNodeProbes();
    assertEquals(Arrays.asList(recovering), liveNodes.getQuarantinedNodes());
    assertEquals(0, quarantineSamplingCounter(liveNodes).get());

    liveNodes.refreshDiscoveredNodes();

    assertEquals(0, quarantineSamplingCounter(liveNodes).get());
    assertEquals(Arrays.asList(active, recovering), liveNodes.getQueryPlanNodes());
    AlternatorLiveNodes.QueryPlanNodeFilter firstFilter = liveNodes.newQueryPlanNodeFilter();
    assertTrue(firstFilter.shouldRouteTo(active));
    assertFalse(firstFilter.shouldRouteTo(recovering));
    AlternatorLiveNodes.QueryPlanNodeFilter secondFilter = liveNodes.newQueryPlanNodeFilter();
    assertTrue(secondFilter.shouldRouteTo(active));
    assertTrue(secondFilter.shouldRouteTo(recovering));
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
    return new AlternatorLiveNodes(config, httpClient);
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

  private static AtomicLong quarantineSamplingCounter(AlternatorLiveNodes liveNodes)
      throws Exception {
    Field field = AlternatorLiveNodes.class.getDeclaredField("quarantineSamplingCounter");
    field.setAccessible(true);
    return (AtomicLong) field.get(liveNodes);
  }

  private static final class LocalNodesHttpClient implements SdkHttpClient {
    private final List<SdkHttpRequest> requests = new CopyOnWriteArrayList<>();
    private volatile String responseBody;
    private final String failingHost;
    private final int statusCode;
    private final AtomicLong requestsToFail;

    private LocalNodesHttpClient() {
      this("[]");
    }

    private LocalNodesHttpClient(int statusCode) {
      this("[]", null, statusCode);
    }

    private LocalNodesHttpClient(String responseBody) {
      this(responseBody, null);
    }

    private LocalNodesHttpClient(String responseBody, String failingHost) {
      this(responseBody, failingHost, 200);
    }

    private LocalNodesHttpClient(String responseBody, int requestsToFail) {
      this(responseBody, null, 200, requestsToFail);
    }

    private LocalNodesHttpClient(String responseBody, String failingHost, int statusCode) {
      this(responseBody, failingHost, statusCode, 0);
    }

    private LocalNodesHttpClient(
        String responseBody, String failingHost, int statusCode, long requestsToFail) {
      this.responseBody = responseBody;
      this.failingHost = failingHost;
      this.statusCode = statusCode;
      this.requestsToFail = new AtomicLong(requestsToFail);
    }

    private void setResponseBody(String responseBody) {
      this.responseBody = responseBody;
    }

    @Override
    public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
      SdkHttpRequest httpRequest = request.httpRequest();
      requests.add(httpRequest);
      return new ExecutableHttpRequest() {
        @Override
        public HttpExecuteResponse call() throws IOException {
          if (requestsToFail.getAndUpdate(value -> value > 0 ? value - 1 : value) > 0) {
            throw new IOException("simulated connection failure");
          }
          if (httpRequest.host().equals(failingHost)) {
            throw new IOException("simulated connection failure for " + failingHost);
          }
          byte[] body = responseBody.getBytes(StandardCharsets.UTF_8);
          return HttpExecuteResponse.builder()
              .response(SdkHttpFullResponse.builder().statusCode(statusCode).build())
              .responseBody(AbortableInputStream.create(new ByteArrayInputStream(body)))
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
      return "localnodes";
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
