package com.scylladb.alternator.internal;

import com.scylladb.alternator.NodeHealthConfig;
import com.scylladb.alternator.NodeHealthObservation;
import com.scylladb.alternator.NodeHealthState;
import com.scylladb.alternator.NodeHealthStatus;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.BiFunction;

final class NodeHealthStore {
  private final NodeHealthConfig config;
  private final Map<URI, MutableStatus> statuses = new TreeMap<>();

  NodeHealthStore(NodeHealthConfig config, List<URI> initialNodes) {
    this.config = config != null ? config : NodeHealthConfig.getDefault();
    for (URI node : initialNodes) {
      addNode(node);
    }
  }

  synchronized List<URI> getActiveNodes() {
    if (config.isDisabled()) {
      return getStoredNodes();
    }
    return getNodesByState(NodeHealthState.ACTIVE);
  }

  synchronized List<URI> getQuarantinedNodes() {
    if (config.isDisabled()) {
      return Collections.emptyList();
    }
    return getNodesByState(NodeHealthState.QUARANTINED);
  }

  synchronized List<URI> getDownNodes() {
    if (config.isDisabled()) {
      return Collections.emptyList();
    }
    return getNodesByState(NodeHealthState.DOWN);
  }

  synchronized NodeHealthStatus getNodeStatus(URI node) {
    MutableStatus status = getMutableStatus(node);
    return status != null ? status.snapshot() : null;
  }

  synchronized void addNode(URI node) {
    URI key = canonicalNodeKey(node);
    if (key == null || statuses.containsKey(key)) {
      return;
    }
    MutableStatus status = new MutableStatus(node);
    status.state = NodeHealthState.ACTIVE;
    status.consecutiveTrafficFailures = 0;
    status.consecutiveProbeFailures = 0;
    status.consecutiveSuccesses = config.getQuarantineSuccessThreshold();
    status.updatedAtNanos = System.nanoTime();
    statuses.put(key, status);
  }

  void reportNodeResult(URI node, NodeHealthObservation observation) {
    if (config.isDisabled()) {
      return;
    }
    synchronized (this) {
      MutableStatus status = getMutableStatus(node);
      if (status == null || observation == null) {
        return;
      }

      switch (observation) {
        case TRAFFIC_SUCCESS:
          reportTrafficSuccess(status);
          break;
        case PROBE_SUCCESS:
          reportProbeSuccess(status);
          break;
        case TRAFFIC_FAILURE:
        case PROBE_FAILURE:
          reportHealthFailure(status, observation);
          break;
        default:
          break;
      }
      status.updatedAtNanos = System.nanoTime();
    }
  }

  private void reportTrafficSuccess(MutableStatus status) {
    status.consecutiveTrafficFailures = 0;
    if (status.state == NodeHealthState.QUARANTINED) {
      reportQuarantineTrafficSuccess(status);
    } else if (status.state == NodeHealthState.ACTIVE) {
      reportActiveSuccess(status);
    } else {
      status.consecutiveSuccesses = 0;
    }
  }

  private void reportProbeSuccess(MutableStatus status) {
    status.consecutiveProbeFailures = 0;
    if (status.state == NodeHealthState.DOWN) {
      status.consecutiveTrafficFailures = 0;
      status.consecutiveSuccesses++;
      if (status.consecutiveSuccesses >= config.getDownNodeRecoverySuccessThreshold()) {
        status.state = NodeHealthState.QUARANTINED;
        status.consecutiveSuccesses = 0;
      }
    } else if (status.state == NodeHealthState.ACTIVE) {
      reportActiveSuccess(status);
    }
  }

  private void reportActiveSuccess(MutableStatus status) {
    status.consecutiveSuccesses = config.getQuarantineSuccessThreshold();
  }

  private void reportQuarantineTrafficSuccess(MutableStatus status) {
    status.consecutiveSuccesses++;
    if (status.consecutiveSuccesses >= config.getQuarantineSuccessThreshold()) {
      status.state = NodeHealthState.ACTIVE;
      status.consecutiveSuccesses = config.getQuarantineSuccessThreshold();
    }
  }

  private void reportHealthFailure(MutableStatus status, NodeHealthObservation observation) {
    int consecutiveFailures = incrementFailure(status, observation);
    if (status.state == NodeHealthState.QUARANTINED) {
      markDown(status, observation);
      return;
    }
    status.consecutiveSuccesses = 0;
    if (consecutiveFailures >= config.getConsecutiveFailureThreshold()) {
      markDown(status, observation);
    }
  }

  private int incrementFailure(MutableStatus status, NodeHealthObservation observation) {
    if (observation == NodeHealthObservation.PROBE_FAILURE) {
      return ++status.consecutiveProbeFailures;
    }
    return ++status.consecutiveTrafficFailures;
  }

  private void markDown(MutableStatus status, NodeHealthObservation observation) {
    status.state = NodeHealthState.DOWN;
    if (observation == NodeHealthObservation.PROBE_FAILURE) {
      status.consecutiveProbeFailures =
          Math.max(status.consecutiveProbeFailures, config.getConsecutiveFailureThreshold());
    } else {
      status.consecutiveTrafficFailures =
          Math.max(status.consecutiveTrafficFailures, config.getConsecutiveFailureThreshold());
    }
    status.consecutiveSuccesses = 0;
  }

  List<URI> probeDownNodes(BiFunction<URI, NodeHealthStatus, NodeHealthObservation> probe) {
    return probeDownNodes(getDownNodes(), probe);
  }

  List<URI> probeDownNodes(
      List<URI> candidateNodes, BiFunction<URI, NodeHealthStatus, NodeHealthObservation> probe) {
    if (config.isDisabled() || probe == null) {
      return Collections.emptyList();
    }

    List<URI> candidates =
        candidateNodes != null ? new ArrayList<>(candidateNodes) : Collections.emptyList();
    List<URI> recovered = new ArrayList<>();
    Set<URI> probedNodes = new HashSet<>();
    for (URI node : candidates) {
      URI key = canonicalNodeKey(node);
      if (key == null || !probedNodes.add(key)) {
        continue;
      }
      NodeHealthStatus status = getNodeStatus(node);
      if (status == null || status.getState() != NodeHealthState.DOWN) {
        continue;
      }
      NodeHealthObservation observation = probe.apply(node, status);
      reportNodeResult(node, observation);
      NodeHealthStatus updatedStatus = getNodeStatus(node);
      if (observation == NodeHealthObservation.PROBE_SUCCESS
          && updatedStatus != null
          && updatedStatus.getState() == NodeHealthState.QUARANTINED) {
        recovered.add(node);
      }
    }
    return recovered;
  }

  private List<URI> getNodesByState(NodeHealthState state) {
    List<URI> nodes = new ArrayList<>();
    for (MutableStatus status : statuses.values()) {
      if (status.state == state) {
        nodes.add(status.node);
      }
    }
    Collections.sort(nodes);
    return nodes;
  }

  private List<URI> getStoredNodes() {
    List<URI> nodes = new ArrayList<>();
    for (MutableStatus status : statuses.values()) {
      nodes.add(status.node);
    }
    Collections.sort(nodes);
    return nodes;
  }

  private MutableStatus getMutableStatus(URI node) {
    URI key = canonicalNodeKey(node);
    return key != null ? statuses.get(key) : null;
  }

  static URI canonicalNodeKey(URI node) {
    int defaultPort = node != null ? defaultPort(node.getScheme()) : -1;
    if (node == null || defaultPort < 0 || node.getPort() != defaultPort) {
      return node;
    }
    try {
      return new URI(
          node.getScheme(),
          node.getUserInfo(),
          node.getHost(),
          -1,
          node.getPath(),
          node.getQuery(),
          node.getFragment());
    } catch (IllegalArgumentException | URISyntaxException e) {
      return node;
    }
  }

  private static int defaultPort(String scheme) {
    if ("http".equalsIgnoreCase(scheme)) {
      return 80;
    }
    if ("https".equalsIgnoreCase(scheme)) {
      return 443;
    }
    return -1;
  }

  private static final class MutableStatus {
    private final URI node;
    private NodeHealthState state = NodeHealthState.ACTIVE;
    private int consecutiveTrafficFailures = 0;
    private int consecutiveProbeFailures = 0;
    private int consecutiveSuccesses = 0;
    private long updatedAtNanos = 0;

    private MutableStatus(URI node) {
      this.node = node;
    }

    private NodeHealthStatus snapshot() {
      return new NodeHealthStatus(
          state, consecutiveFailures(), consecutiveSuccesses, updatedAtNanos);
    }

    private int consecutiveFailures() {
      return Math.max(consecutiveTrafficFailures, consecutiveProbeFailures);
    }
  }
}
