package com.scylladb.alternator.internal;

import static org.junit.Assert.*;

import com.scylladb.alternator.NodeHealthConfig;
import com.scylladb.alternator.NodeHealthObservation;
import com.scylladb.alternator.NodeHealthState;
import com.scylladb.alternator.NodeHealthStatus;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

public class NodeHealthStoreTest {
  @Test
  public void failuresMarkNodeDownAfterConfiguredThreshold() {
    URI node = node("node1.local");
    NodeHealthConfig config = NodeHealthConfig.builder().withConsecutiveFailureThreshold(2).build();
    NodeHealthStore store = new NodeHealthStore(config, Arrays.asList(node));

    assertEquals(Arrays.asList(node), store.getActiveNodes());

    store.reportNodeResult(node, NodeHealthObservation.TRAFFIC_FAILURE);

    assertEquals(Arrays.asList(node), store.getActiveNodes());
    assertTrue(store.getDownNodes().isEmpty());
    assertEquals(1, store.getNodeStatus(node).getConsecutiveFailures());

    store.reportNodeResult(node, NodeHealthObservation.TRAFFIC_FAILURE);

    assertTrue(store.getActiveNodes().isEmpty());
    assertTrue(store.getQuarantinedNodes().isEmpty());
    assertEquals(Arrays.asList(node), store.getDownNodes());
    assertEquals(NodeHealthState.DOWN, store.getNodeStatus(node).getState());
    assertEquals(2, store.getNodeStatus(node).getConsecutiveFailures());
  }

  @Test
  public void httpDefaultPortReportMatchesNodeWithoutExplicitPort() {
    URI configured = URI.create("http://node1.local");
    URI reported = URI.create("http://node1.local:80");
    NodeHealthConfig config = NodeHealthConfig.builder().withConsecutiveFailureThreshold(1).build();
    NodeHealthStore store = new NodeHealthStore(config, Arrays.asList(configured));

    store.reportNodeResult(reported, NodeHealthObservation.TRAFFIC_FAILURE);

    assertTrue(store.getActiveNodes().isEmpty());
    assertEquals(Arrays.asList(configured), store.getDownNodes());
    assertEquals(NodeHealthState.DOWN, store.getNodeStatus(configured).getState());
    assertEquals(NodeHealthState.DOWN, store.getNodeStatus(reported).getState());
  }

  @Test
  public void httpsDefaultPortReportMatchesNodeWithoutExplicitPort() {
    URI configured = URI.create("https://node1.local");
    URI reported = URI.create("https://node1.local:443");
    NodeHealthConfig config = NodeHealthConfig.builder().withConsecutiveFailureThreshold(1).build();
    NodeHealthStore store = new NodeHealthStore(config, Arrays.asList(configured));

    store.reportNodeResult(reported, NodeHealthObservation.TRAFFIC_FAILURE);

    assertTrue(store.getActiveNodes().isEmpty());
    assertEquals(Arrays.asList(configured), store.getDownNodes());
    assertEquals(NodeHealthState.DOWN, store.getNodeStatus(configured).getState());
    assertEquals(NodeHealthState.DOWN, store.getNodeStatus(reported).getState());
  }

  @Test
  public void consecutiveFailuresMarkNodeDown() {
    URI node = node("node1.local");
    NodeHealthConfig config = NodeHealthConfig.builder().withConsecutiveFailureThreshold(2).build();
    NodeHealthStore store = new NodeHealthStore(config, Arrays.asList(node));

    store.reportNodeResult(node, NodeHealthObservation.TRAFFIC_FAILURE);
    assertEquals(Arrays.asList(node), store.getActiveNodes());
    assertEquals(1, store.getNodeStatus(node).getConsecutiveFailures());

    store.reportNodeResult(node, NodeHealthObservation.TRAFFIC_FAILURE);
    assertTrue(store.getActiveNodes().isEmpty());
    assertEquals(Arrays.asList(node), store.getDownNodes());
    assertEquals(NodeHealthState.DOWN, store.getNodeStatus(node).getState());
    assertEquals(2, store.getNodeStatus(node).getConsecutiveFailures());
  }

  @Test
  public void downNodeMovesToQuarantineAfterConfiguredRecoverySuccesses() {
    URI node = node("node1.local");
    NodeHealthConfig config =
        NodeHealthConfig.builder()
            .withConsecutiveFailureThreshold(1)
            .withDownNodeRecoverySuccessThreshold(2)
            .withQuarantineSuccessThreshold(1)
            .build();
    NodeHealthStore store = new NodeHealthStore(config, Arrays.asList(node));

    reportFailures(store, node, config.getConsecutiveFailureThreshold());
    assertEquals(Arrays.asList(node), store.getDownNodes());

    assertEquals(
        Collections.emptyList(),
        store.probeDownNodes(
            (uri, status) ->
                status.getState() == NodeHealthState.DOWN
                    ? NodeHealthObservation.PROBE_SUCCESS
                    : NodeHealthObservation.PROBE_FAILURE));

    assertEquals(Arrays.asList(node), store.getDownNodes());
    NodeHealthStatus firstSuccessStatus = store.getNodeStatus(node);
    assertEquals(NodeHealthState.DOWN, firstSuccessStatus.getState());
    assertEquals(1, firstSuccessStatus.getConsecutiveSuccesses());

    assertEquals(
        Arrays.asList(node),
        store.probeDownNodes(
            (uri, status) ->
                status.getState() == NodeHealthState.DOWN
                    ? NodeHealthObservation.PROBE_SUCCESS
                    : NodeHealthObservation.PROBE_FAILURE));

    assertTrue(store.getActiveNodes().isEmpty());
    assertEquals(Arrays.asList(node), store.getQuarantinedNodes());
    assertTrue(store.getDownNodes().isEmpty());
    NodeHealthStatus status = store.getNodeStatus(node);
    assertEquals(NodeHealthState.QUARANTINED, status.getState());
    assertEquals(0, status.getConsecutiveSuccesses());
  }

  @Test
  public void downNodeProbeCountsDuplicateCandidatesOncePerCycle() {
    URI node = URI.create("http://node1.local");
    URI nodeWithDefaultPort = URI.create("http://node1.local:80");
    NodeHealthConfig config =
        NodeHealthConfig.builder()
            .withConsecutiveFailureThreshold(1)
            .withDownNodeRecoverySuccessThreshold(2)
            .build();
    NodeHealthStore store = new NodeHealthStore(config, Arrays.asList(node));
    AtomicInteger probes = new AtomicInteger();

    store.reportNodeResult(node, NodeHealthObservation.TRAFFIC_FAILURE);

    assertTrue(
        store
            .probeDownNodes(
                Arrays.asList(node, nodeWithDefaultPort, node),
                (uri, status) -> {
                  probes.incrementAndGet();
                  return NodeHealthObservation.PROBE_SUCCESS;
                })
            .isEmpty());

    assertEquals(1, probes.get());
    assertEquals(NodeHealthState.DOWN, store.getNodeStatus(node).getState());
    assertEquals(1, store.getNodeStatus(node).getConsecutiveSuccesses());
  }

  @Test
  public void probeSuccessDoesNotClearActiveTrafficFailures() {
    URI node = node("node1.local");
    NodeHealthConfig config = NodeHealthConfig.builder().withConsecutiveFailureThreshold(2).build();
    NodeHealthStore store = new NodeHealthStore(config, Arrays.asList(node));

    store.reportNodeResult(node, NodeHealthObservation.TRAFFIC_FAILURE);
    store.reportNodeResult(node, NodeHealthObservation.PROBE_SUCCESS);

    assertEquals(1, store.getNodeStatus(node).getConsecutiveFailures());
    assertEquals(NodeHealthState.ACTIVE, store.getNodeStatus(node).getState());

    store.reportNodeResult(node, NodeHealthObservation.TRAFFIC_FAILURE);

    assertEquals(Arrays.asList(node), store.getDownNodes());
    assertEquals(2, store.getNodeStatus(node).getConsecutiveFailures());
  }

  @Test
  public void trafficSuccessDoesNotClearActiveProbeFailures() {
    URI node = node("node1.local");
    NodeHealthConfig config = NodeHealthConfig.builder().withConsecutiveFailureThreshold(2).build();
    NodeHealthStore store = new NodeHealthStore(config, Arrays.asList(node));

    store.reportNodeResult(node, NodeHealthObservation.PROBE_FAILURE);
    store.reportNodeResult(node, NodeHealthObservation.TRAFFIC_SUCCESS);

    assertEquals(1, store.getNodeStatus(node).getConsecutiveFailures());
    assertEquals(NodeHealthState.ACTIVE, store.getNodeStatus(node).getState());

    store.reportNodeResult(node, NodeHealthObservation.PROBE_FAILURE);

    assertEquals(Arrays.asList(node), store.getDownNodes());
    assertEquals(2, store.getNodeStatus(node).getConsecutiveFailures());
  }

  @Test
  public void probeSuccessClearsActiveProbeFailures() {
    URI node = node("node1.local");
    NodeHealthConfig config = NodeHealthConfig.builder().withConsecutiveFailureThreshold(2).build();
    NodeHealthStore store = new NodeHealthStore(config, Arrays.asList(node));

    store.reportNodeResult(node, NodeHealthObservation.PROBE_FAILURE);
    store.reportNodeResult(node, NodeHealthObservation.PROBE_SUCCESS);
    store.reportNodeResult(node, NodeHealthObservation.PROBE_FAILURE);

    assertEquals(NodeHealthState.ACTIVE, store.getNodeStatus(node).getState());
    assertEquals(1, store.getNodeStatus(node).getConsecutiveFailures());
  }

  @Test
  public void downTrafficSuccessDoesNotMoveNodeToQuarantine() {
    URI node = node("node1.local");
    NodeHealthConfig config =
        NodeHealthConfig.builder()
            .withConsecutiveFailureThreshold(1)
            .withDownNodeRecoverySuccessThreshold(2)
            .withQuarantineSuccessThreshold(1)
            .build();
    NodeHealthStore store = new NodeHealthStore(config, Arrays.asList(node));

    reportFailures(store, node, config.getConsecutiveFailureThreshold());

    store.reportNodeResult(node, NodeHealthObservation.TRAFFIC_SUCCESS);
    store.reportNodeResult(node, NodeHealthObservation.TRAFFIC_SUCCESS);

    NodeHealthStatus status = store.getNodeStatus(node);
    assertEquals(NodeHealthState.DOWN, status.getState());
    assertEquals(0, status.getConsecutiveFailures());
    assertEquals(0, status.getConsecutiveSuccesses());
    assertEquals(Arrays.asList(node), store.getDownNodes());
    assertTrue(store.getQuarantinedNodes().isEmpty());
  }

  @Test
  public void quarantinePromotesToActiveAfterConfiguredTrafficSuccesses() {
    URI node = node("node1.local");
    NodeHealthConfig config =
        NodeHealthConfig.builder()
            .withConsecutiveFailureThreshold(1)
            .withDownNodeRecoverySuccessThreshold(1)
            .withQuarantineSuccessThreshold(2)
            .build();
    NodeHealthStore store = new NodeHealthStore(config, Arrays.asList(node));

    reportFailures(store, node, config.getConsecutiveFailureThreshold());
    store.reportNodeResult(node, NodeHealthObservation.PROBE_SUCCESS);
    assertEquals(Arrays.asList(node), store.getQuarantinedNodes());

    store.reportNodeResult(node, NodeHealthObservation.TRAFFIC_SUCCESS);
    assertEquals(Arrays.asList(node), store.getQuarantinedNodes());
    assertEquals(1, store.getNodeStatus(node).getConsecutiveSuccesses());

    store.reportNodeResult(node, NodeHealthObservation.TRAFFIC_SUCCESS);

    assertEquals(Arrays.asList(node), store.getActiveNodes());
    assertTrue(store.getQuarantinedNodes().isEmpty());
    assertTrue(store.getDownNodes().isEmpty());
    assertEquals(NodeHealthState.ACTIVE, store.getNodeStatus(node).getState());
    assertEquals(2, store.getNodeStatus(node).getConsecutiveSuccesses());
  }

  @Test
  public void downNodeDoesNotPromoteImmediatelyWhenQuarantineThresholdIsOne() {
    URI node = node("node1.local");
    NodeHealthConfig config =
        NodeHealthConfig.builder()
            .withConsecutiveFailureThreshold(1)
            .withDownNodeRecoverySuccessThreshold(1)
            .withQuarantineSuccessThreshold(1)
            .build();
    NodeHealthStore store = new NodeHealthStore(config, Arrays.asList(node));

    store.reportNodeResult(node, NodeHealthObservation.TRAFFIC_FAILURE);
    store.reportNodeResult(node, NodeHealthObservation.PROBE_SUCCESS);

    assertEquals(Arrays.asList(node), store.getQuarantinedNodes());
    assertTrue(store.getActiveNodes().isEmpty());
    assertEquals(NodeHealthState.QUARANTINED, store.getNodeStatus(node).getState());
    assertEquals(0, store.getNodeStatus(node).getConsecutiveSuccesses());

    store.reportNodeResult(node, NodeHealthObservation.TRAFFIC_SUCCESS);

    assertEquals(Arrays.asList(node), store.getActiveNodes());
    assertTrue(store.getQuarantinedNodes().isEmpty());
    assertTrue(store.getDownNodes().isEmpty());
    assertEquals(NodeHealthState.ACTIVE, store.getNodeStatus(node).getState());
    assertEquals(1, store.getNodeStatus(node).getConsecutiveSuccesses());
  }

  @Test
  public void probeSuccessDoesNotPromoteQuarantinedNode() {
    URI node = node("node1.local");
    NodeHealthConfig config =
        NodeHealthConfig.builder()
            .withConsecutiveFailureThreshold(3)
            .withDownNodeRecoverySuccessThreshold(1)
            .withQuarantineSuccessThreshold(2)
            .build();
    NodeHealthStore store = new NodeHealthStore(config, Arrays.asList(node));

    reportFailures(store, node, config.getConsecutiveFailureThreshold());
    store.reportNodeResult(node, NodeHealthObservation.PROBE_SUCCESS);
    assertEquals(Arrays.asList(node), store.getQuarantinedNodes());

    store.reportNodeResult(node, NodeHealthObservation.TRAFFIC_SUCCESS);

    assertTrue(store.getActiveNodes().isEmpty());
    assertEquals(Arrays.asList(node), store.getQuarantinedNodes());
    assertEquals(NodeHealthState.QUARANTINED, store.getNodeStatus(node).getState());
    assertEquals(1, store.getNodeStatus(node).getConsecutiveSuccesses());

    store.reportNodeResult(node, NodeHealthObservation.PROBE_SUCCESS);

    assertTrue(store.getActiveNodes().isEmpty());
    assertEquals(Arrays.asList(node), store.getQuarantinedNodes());
    assertEquals(NodeHealthState.QUARANTINED, store.getNodeStatus(node).getState());
    assertEquals(1, store.getNodeStatus(node).getConsecutiveSuccesses());

    store.reportNodeResult(node, NodeHealthObservation.TRAFFIC_SUCCESS);

    assertEquals(Arrays.asList(node), store.getActiveNodes());
    assertTrue(store.getQuarantinedNodes().isEmpty());
    assertEquals(NodeHealthState.ACTIVE, store.getNodeStatus(node).getState());
  }

  @Test
  public void quarantineFailureReturnsNodeToDown() {
    URI node = node("node1.local");
    NodeHealthConfig config =
        NodeHealthConfig.builder()
            .withConsecutiveFailureThreshold(3)
            .withDownNodeRecoverySuccessThreshold(1)
            .withQuarantineSuccessThreshold(2)
            .build();
    NodeHealthStore store = new NodeHealthStore(config, Arrays.asList(node));

    reportFailures(store, node, config.getConsecutiveFailureThreshold());
    store.reportNodeResult(node, NodeHealthObservation.PROBE_SUCCESS);
    assertEquals(Arrays.asList(node), store.getQuarantinedNodes());

    store.reportNodeResult(node, NodeHealthObservation.TRAFFIC_FAILURE);

    assertTrue(store.getQuarantinedNodes().isEmpty());
    assertEquals(Arrays.asList(node), store.getDownNodes());
    assertEquals(NodeHealthState.DOWN, store.getNodeStatus(node).getState());
    assertEquals(3, store.getNodeStatus(node).getConsecutiveFailures());
  }

  @Test
  public void disabledKeepsAllNodesActive() {
    URI node = node("node1.local");
    NodeHealthStore store = new NodeHealthStore(NodeHealthConfig.disabled(), Arrays.asList(node));

    store.reportNodeResult(node, NodeHealthObservation.TRAFFIC_FAILURE);

    assertEquals(Arrays.asList(node), store.getActiveNodes());
    assertTrue(store.getQuarantinedNodes().isEmpty());
    assertTrue(store.getDownNodes().isEmpty());
    assertEquals(
        Collections.emptyList(),
        store.probeDownNodes((uri, status) -> NodeHealthObservation.PROBE_SUCCESS));
  }

  private static URI node(String host) {
    return URI.create("http://" + host + ":8080");
  }

  private static void reportFailures(NodeHealthStore store, URI node, int count) {
    for (int i = 0; i < count; i++) {
      store.reportNodeResult(node, NodeHealthObservation.TRAFFIC_FAILURE);
    }
  }
}
