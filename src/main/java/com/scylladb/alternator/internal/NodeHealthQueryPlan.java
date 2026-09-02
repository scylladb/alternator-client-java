package com.scylladb.alternator.internal;

import com.scylladb.alternator.NodeHealthState;
import java.net.URI;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.Set;

/** Request-scoped health-aware wrapper around a {@link LazyQueryPlan}. */
public final class NodeHealthQueryPlan {
  /** Routing behavior for the wrapped plan. */
  public enum Mode {
    /** Random traffic may put a reserved quarantine verification node first. */
    REGULAR,
    /** Affinity order is preserved and quarantine is sampled only when encountered naturally. */
    AFFINITY,
    /** Control-plane probes prefer active nodes and fall back to quarantined nodes. */
    PROBE
  }

  private final AlternatorLiveNodes liveNodes;
  private final LazyQueryPlan delegate;
  private final Mode mode;
  private final Deque<URI> deferred = new ArrayDeque<>();
  private final Set<URI> suppressedDelegateNodes = new HashSet<>();
  private boolean delegateExhausted;
  private boolean injectionAttempted;
  private boolean verificationNodeSelected;

  NodeHealthQueryPlan(AlternatorLiveNodes liveNodes, LazyQueryPlan delegate, Mode mode) {
    if (liveNodes == null || delegate == null || mode == null) {
      throw new IllegalArgumentException("liveNodes, delegate, and mode cannot be null");
    }
    this.liveNodes = liveNodes;
    this.delegate = delegate;
    this.mode = mode;
    if (mode != Mode.PROBE) {
      liveNodes.registerLogicalTrafficQuery();
    }
  }

  /**
   * Returns the next routable candidate, or the fallback endpoint after the plan is exhausted.
   *
   * <p>Each call applies node health at the last possible moment. Deferred candidates remain
   * available to later retries if their state changes.
   *
   * @param fallbackEndpoint endpoint to consider after all plan candidates, or null
   * @return the next candidate, or null when none is currently routable
   */
  public synchronized URI nextRouteCandidate(URI fallbackEndpoint) {
    URI candidate = mode == Mode.PROBE ? nextProbeCandidate() : nextTrafficCandidate();
    if (candidate != null) {
      return candidate;
    }
    return fallbackEndpoint != null && routeTrafficCandidate(fallbackEndpoint)
        ? fallbackEndpoint
        : null;
  }

  /** Returns the next routable plan candidate without an endpoint fallback. */
  public URI nextRouteCandidate() {
    return nextRouteCandidate(null);
  }

  private URI nextTrafficCandidate() {
    if (mode == Mode.REGULAR && !injectionAttempted) {
      injectionAttempted = true;
      URI reserved = liveNodes.reserveQuarantineSample();
      if (reserved != null) {
        URI key = NodeHealthStore.canonicalNodeKey(reserved);
        suppressedDelegateNodes.add(key);
        if (liveNodes.getQueryPlanNodeState(reserved) == NodeHealthState.QUARANTINED) {
          verificationNodeSelected = true;
          return reserved;
        }
        liveNodes.rearmQuarantineSample();
        deferred.addLast(reserved);
      }
    }

    int deferredCandidates = deferred.size();
    for (int i = 0; i < deferredCandidates; i++) {
      URI candidate = deferred.removeFirst();
      if (mode == Mode.AFFINITY
          && !delegateExhausted
          && liveNodes.hasActiveQueryPlanNodes()
          && liveNodes.getQueryPlanNodeState(candidate) == NodeHealthState.QUARANTINED) {
        deferred.addLast(candidate);
        continue;
      }
      if (routeTrafficCandidate(candidate)) {
        return candidate;
      }
      deferred.addLast(candidate);
    }

    while (!delegateExhausted && delegate.hasNext()) {
      URI candidate = delegate.next();
      if (candidate == null) {
        continue;
      }
      if (suppressedDelegateNodes.remove(NodeHealthStore.canonicalNodeKey(candidate))) {
        continue;
      }
      if (routeTrafficCandidate(candidate)) {
        return candidate;
      }
      deferred.addLast(candidate);
    }
    delegateExhausted = true;
    return null;
  }

  private boolean routeTrafficCandidate(URI candidate) {
    NodeHealthState state = liveNodes.getQueryPlanNodeState(candidate);
    if (state == NodeHealthState.DOWN) {
      return false;
    }
    boolean activeNodesAvailable = liveNodes.hasActiveQueryPlanNodes();
    if (!activeNodesAvailable) {
      if (state != NodeHealthState.QUARANTINED) {
        return false;
      }
      // With no active nodes, quarantined nodes preserve service in plan order. Any armed sample
      // is satisfied by this real traffic, but the node is routable even without a pending sample.
      liveNodes.consumePendingQuarantineSample();
      return true;
    }
    if (state == NodeHealthState.ACTIVE) {
      return true;
    }
    if (state == NodeHealthState.QUARANTINED) {
      if (verificationNodeSelected) {
        return false;
      }
      if (mode == Mode.AFFINITY && liveNodes.consumeQuarantineSampleFor(candidate)) {
        verificationNodeSelected = true;
        return true;
      }
      return false;
    }
    return false;
  }

  private URI nextProbeCandidate() {
    int deferredCandidates = deferred.size();
    for (int i = 0; i < deferredCandidates; i++) {
      URI candidate = deferred.removeFirst();
      if (liveNodes.getQueryPlanNodeState(candidate) == NodeHealthState.ACTIVE) {
        return candidate;
      }
      deferred.addLast(candidate);
    }

    while (!delegateExhausted && delegate.hasNext()) {
      URI candidate = delegate.next();
      if (candidate == null) {
        continue;
      }
      if (liveNodes.getQueryPlanNodeState(candidate) == NodeHealthState.ACTIVE) {
        return candidate;
      }
      deferred.addLast(candidate);
    }
    delegateExhausted = true;

    deferredCandidates = deferred.size();
    for (int i = 0; i < deferredCandidates; i++) {
      URI candidate = deferred.removeFirst();
      if (liveNodes.getQueryPlanNodeState(candidate) == NodeHealthState.QUARANTINED) {
        return candidate;
      }
      deferred.addLast(candidate);
    }
    return null;
  }
}
