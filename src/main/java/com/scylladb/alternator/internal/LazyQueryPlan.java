package com.scylladb.alternator.internal;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * A lazy iterator over URI nodes that pulls nodes from {@link AlternatorLiveNodes} one at a time
 * only when needed.
 *
 * <p>When created with a seed, this implementation uses a Go-compatible Lagged Fibonacci Generator
 * ({@link GoRand}) and pick-and-remove selection to produce identical node sequences across all
 * Alternator client implementations (Go, Java, etc.) for the same seed. This cross-language
 * compatibility is critical for key route affinity, where requests with the same partition key hash
 * must be routed to the same coordinator node regardless of client language.
 *
 * <p>When created without a seed, uses {@link ThreadLocalRandom} for non-deterministic load
 * balancing.
 *
 * <p>The pick-and-remove algorithm works by maintaining a mutable copy of the node list. On each
 * {@link #next()} call, a random index is chosen, the node at that index is returned, and the node
 * is replaced with the last element in the list (which is then truncated). This ensures each node
 * is returned exactly once with O(1) per selection.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * AlternatorLiveNodes liveNodes = ...;
 *
 * // Create with random seed for load balancing
 * LazyQueryPlan plan = new LazyQueryPlan(liveNodes);
 *
 * // Or create with deterministic seed for key affinity
 * long partitionKeyHash = AttributeValueHasher.hash(pkValue);
 * LazyQueryPlan affinityPlan = new LazyQueryPlan(liveNodes, partitionKeyHash);
 *
 * // Get the first available node
 * if (plan.hasNext()) {
 *     URI node = plan.next();
 *     // Use node...
 * }
 * }</pre>
 *
 * <p><strong>Thread Safety:</strong> This class is NOT thread-safe. It is designed for single
 * request use only, where the same thread (or async context) iterates through nodes during retry
 * attempts. Do not share instances between concurrent requests.
 *
 * @author dmitry.kropachev
 * @since 2.0.0
 */
public class LazyQueryPlan implements Iterator<URI>, Iterable<URI> {
  private final AlternatorLiveNodes liveNodes;
  private final GoRand goRand;
  private final List<URI> preferredNodes;

  /**
   * Mutable list of remaining nodes for pick-and-remove. Initialized lazily on first access when
   * using a seeded GoRand. Null when using ThreadLocalRandom (non-seeded mode).
   */
  private List<URI> remaining;

  /** Whether the remaining list has been initialized (only used in seeded mode). */
  private boolean initialized;

  /** Cached next node for hasNext()/next() contract in non-seeded mode. */
  private URI nextNode;

  /**
   * Tracks nodes already returned in non-seeded mode to avoid duplicates. Lazily allocated on the
   * second {@link #next()} call: the overwhelming majority of requests succeed on their first node
   * attempt and never need a duplicate-skip set, so allocating it eagerly costs a HashSet per
   * request for no benefit.
   */
  private Set<URI> usedNodes;

  /**
   * First node returned in non-seeded mode, kept separately so we avoid materializing {@link
   * #usedNodes} when the request only ever picks one node (the common case).
   */
  private URI firstUsedNode;

  /**
   * Creates a LazyQueryPlan with a random seed. The iteration order will be non-deterministic,
   * suitable for general load balancing.
   *
   * @param liveNodes the {@link AlternatorLiveNodes} instance to pull nodes from
   */
  public LazyQueryPlan(AlternatorLiveNodes liveNodes) {
    if (liveNodes == null) {
      throw new IllegalArgumentException("liveNodes cannot be null");
    }
    this.liveNodes = liveNodes;
    this.goRand = null;
    this.preferredNodes = null;
  }

  /**
   * Creates a LazyQueryPlan with a specified seed for deterministic iteration. Uses Go-compatible
   * PRNG and pick-and-remove selection to produce identical sequences across all Alternator client
   * implementations.
   *
   * @param liveNodes the {@link AlternatorLiveNodes} instance to pull nodes from
   * @param seed a long value used to initialize the Go-compatible PRNG
   */
  public LazyQueryPlan(AlternatorLiveNodes liveNodes, long seed) {
    if (liveNodes == null) {
      throw new IllegalArgumentException("liveNodes cannot be null");
    }
    this.liveNodes = liveNodes;
    this.goRand = new GoRand(seed);
    this.preferredNodes = null;
  }

  /**
   * Creates a LazyQueryPlan that tries the given preferred nodes first, then the remaining live
   * nodes in canonical affinity order.
   *
   * @param liveNodes the {@link AlternatorLiveNodes} instance to pull nodes from
   * @param preferredNodes nodes to try first, in preference order
   */
  public LazyQueryPlan(AlternatorLiveNodes liveNodes, List<URI> preferredNodes) {
    if (liveNodes == null) {
      throw new IllegalArgumentException("liveNodes cannot be null");
    }
    if (preferredNodes == null) {
      throw new IllegalArgumentException("preferredNodes cannot be null");
    }
    this.liveNodes = liveNodes;
    this.goRand = null;
    this.preferredNodes = new ArrayList<>(preferredNodes);
  }

  /**
   * Initializes the remaining list from current live nodes (seeded mode only). Called lazily on
   * first access so the snapshot is as fresh as possible.
   */
  private void ensureInitialized() {
    if (!initialized) {
      remaining = sortedAffinityNodes(liveNodes);
      if (preferredNodes != null) {
        remaining = orderPreferredNodesFirst(remaining, preferredNodes);
      }
      initialized = true;
    }
  }

  /**
   * Returns the current live nodes in the canonical order used by affinity routing.
   *
   * @param liveNodes the live nodes manager
   * @return a sorted copy of current live nodes
   */
  public static List<URI> sortedAffinityNodes(AlternatorLiveNodes liveNodes) {
    if (liveNodes == null) {
      throw new IllegalArgumentException("liveNodes cannot be null");
    }
    List<URI> nodes = new ArrayList<>(liveNodes.getLiveNodesInternal());
    nodes.sort(Comparator.comparing(URI::toString));
    return nodes;
  }

  /**
   * Returns the first node selected by the seeded affinity plan without consuming a full query
   * plan.
   *
   * @param liveNodes the live nodes manager
   * @param seed deterministic seed
   * @return the preferred node, or null when no live nodes exist
   */
  public static URI preferredNodeForHash(AlternatorLiveNodes liveNodes, long seed) {
    List<URI> nodes = sortedAffinityNodes(liveNodes);
    if (nodes.isEmpty()) {
      return null;
    }
    return nodes.get(new GoRand(seed).intn(nodes.size()));
  }

  private static List<URI> orderPreferredNodesFirst(List<URI> sortedNodes, List<URI> preferred) {
    List<URI> ordered = new ArrayList<>(sortedNodes.size());
    List<URI> remainingNodes = new ArrayList<>(sortedNodes);
    for (URI preferredNode : preferred) {
      int index = remainingNodes.indexOf(preferredNode);
      if (index >= 0) {
        ordered.add(remainingNodes.remove(index));
      }
    }
    ordered.addAll(remainingNodes);
    return ordered;
  }

  /**
   * Picks and removes a random node from the remaining list using Go's pick-and-remove algorithm.
   *
   * @return the selected node, or null if no nodes remain
   */
  private URI pickAndRemove() {
    ensureInitialized();
    if (remaining.isEmpty()) {
      return null;
    }
    int idx = goRand.intn(remaining.size());
    URI node = remaining.get(idx);
    int last = remaining.size() - 1;
    remaining.set(idx, remaining.get(last));
    remaining.remove(last);
    return node;
  }

  /**
   * Computes next node for non-seeded mode (ThreadLocalRandom).
   *
   * <p>Fast paths for the request hot path:
   *
   * <ul>
   *   <li>First call (no nodes used yet): pick directly from the live list without building a
   *       filtered copy — saves an {@link ArrayList} allocation per request.
   *   <li>Single-node cluster: return the only node — saves the random call too.
   *   <li>Second call: only one node was used; iterate live nodes and pick the first that isn't it,
   *       with a single uniformly-distributed pick across the remaining count.
   * </ul>
   *
   * The general filter+pick path only kicks in from the third call onward, which is the
   * retry-after-2-failures case that's already off the happy path.
   *
   * @return the next node, or null if no more available
   */
  private URI computeNextNonSeeded() {
    if (nextNode != null) {
      return nextNode;
    }

    List<URI> currentNodes = liveNodes.getLiveNodesInternal();
    int size = currentNodes.size();
    if (size == 0) {
      return null;
    }

    // First call — nothing used yet, no filter needed.
    if (firstUsedNode == null) {
      nextNode =
          (size == 1)
              ? currentNodes.get(0)
              : currentNodes.get(ThreadLocalRandom.current().nextInt(size));
      return nextNode;
    }

    // Second call — only firstUsedNode is set; skip it without materializing a HashSet.
    if (usedNodes == null) {
      int remaining = 0;
      for (URI node : currentNodes) {
        if (!node.equals(firstUsedNode)) {
          remaining++;
        }
      }
      if (remaining == 0) {
        return null;
      }
      int pick = ThreadLocalRandom.current().nextInt(remaining);
      for (URI node : currentNodes) {
        if (node.equals(firstUsedNode)) {
          continue;
        }
        if (pick-- == 0) {
          nextNode = node;
          return nextNode;
        }
      }
      throw new AssertionError("pick loop exhausted without match — remaining=" + remaining);
    }

    // Third+ call — full filter against the used set.
    int remaining = 0;
    for (URI node : currentNodes) {
      if (!usedNodes.contains(node)) {
        remaining++;
      }
    }
    if (remaining == 0) {
      return null;
    }
    int pick = ThreadLocalRandom.current().nextInt(remaining);
    for (URI node : currentNodes) {
      if (usedNodes.contains(node)) {
        continue;
      }
      if (pick-- == 0) {
        nextNode = node;
        return nextNode;
      }
    }
    throw new AssertionError("pick loop exhausted without match — remaining=" + remaining);
  }

  @Override
  public boolean hasNext() {
    if (goRand != null || preferredNodes != null) {
      ensureInitialized();
      return !remaining.isEmpty();
    }
    return computeNextNonSeeded() != null;
  }

  @Override
  public URI next() {
    if (goRand != null) {
      URI node = pickAndRemove();
      if (node == null) {
        throw new NoSuchElementException("No more nodes available in query plan");
      }
      return node;
    }

    if (preferredNodes != null) {
      ensureInitialized();
      if (remaining.isEmpty()) {
        throw new NoSuchElementException("No more nodes available in query plan");
      }
      return remaining.remove(0);
    }

    URI node = computeNextNonSeeded();
    if (node == null) {
      throw new NoSuchElementException("No more nodes available in query plan");
    }
    // Promote firstUsedNode → usedNodes on the second consumption, so subsequent calls have
    // a real Set to query against. Allocations are bounded to the slow (retry) path.
    if (firstUsedNode == null) {
      firstUsedNode = node;
    } else if (usedNodes == null) {
      usedNodes = new HashSet<>(4);
      usedNodes.add(firstUsedNode);
      usedNodes.add(node);
    } else {
      usedNodes.add(node);
    }
    nextNode = null;
    return node;
  }

  /** Kept for binary-compat callers that still reference the field via reflection in tests. */
  @SuppressWarnings("unused")
  private Set<URI> usedNodesView() {
    if (usedNodes != null) {
      return usedNodes;
    }
    if (firstUsedNode != null) {
      return Collections.singleton(firstUsedNode);
    }
    return Collections.emptySet();
  }

  @Override
  public Iterator<URI> iterator() {
    return this;
  }
}
