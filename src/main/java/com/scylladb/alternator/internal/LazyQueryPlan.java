package com.scylladb.alternator.internal;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * A lazy iterator over URI nodes that pulls nodes from {@link AlternatorLiveNodes} one at a time
 * only when needed. The iterator tracks which nodes have already been returned to avoid duplicates.
 *
 * <p>The pseudo-random order is determined by a seed value, which can be either provided explicitly
 * (for reproducible sequences based on partition key hash) or generated randomly (for
 * non-deterministic load balancing).
 *
 * <p>This implementation is truly lazy - it doesn't copy the node list upfront. Instead, when
 * {@link #hasNext()} or {@link #next()} is called, it:
 *
 * <ol>
 *   <li>Reads the current live nodes from {@link AlternatorLiveNodes}
 *   <li>Filters out nodes that have already been returned
 *   <li>Randomly selects one from the remaining nodes and caches it
 *   <li>On {@link #next()}, returns the cached node and marks it as used
 * </ol>
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
  private final Set<URI> usedNodes;
  private final Random rnd;
  private URI nextNode;

  /**
   * Tracks the last node returned by {@link #next()} before it's added to {@code usedNodes}. This
   * optimization defers adding to the usedNodes set until another node is requested, avoiding set
   * modification when the first request succeeds without retries.
   */
  private URI lastUsedNode;

  /**
   * Creates a LazyQueryPlan with a random seed. The iteration order will be non-deterministic,
   * suitable for general load balancing.
   *
   * @param liveNodes the {@link AlternatorLiveNodes} instance to pull nodes from
   */
  public LazyQueryPlan(AlternatorLiveNodes liveNodes) {
    this(liveNodes, null);
  }

  /**
   * Creates a LazyQueryPlan with a specified seed for pseudo-random iteration. Using the same seed
   * with the same node list will produce the same first node selection, enabling reproducible
   * behavior for key route affinity.
   *
   * @param liveNodes the {@link AlternatorLiveNodes} instance to pull nodes from
   * @param seed a long value used to initialize the pseudo-random number generator
   */
  public LazyQueryPlan(AlternatorLiveNodes liveNodes, long seed) {
    this(liveNodes, new Random(seed));
  }

  /**
   * Creates a LazyQueryPlan with an optional Random instance.
   *
   * @param liveNodes the {@link AlternatorLiveNodes} instance to pull nodes from
   * @param rnd a pseudo-random number generator, or null to use ThreadLocalRandom
   */
  private LazyQueryPlan(AlternatorLiveNodes liveNodes, Random rnd) {
    if (liveNodes == null) {
      throw new IllegalArgumentException("liveNodes cannot be null");
    }
    this.liveNodes = liveNodes;
    this.usedNodes = new HashSet<>();
    this.rnd = rnd;
  }

  private Random getRandom() {
    if (rnd != null) {
      return rnd;
    }
    return ThreadLocalRandom.current();
  }

  /**
   * Computes and caches the next node if not already cached.
   *
   * @return the next node, or null if no more nodes are available
   */
  private URI computeNextIfNeeded() {
    if (nextNode != null) {
      return nextNode;
    }

    // Track this node as used and clear the cache
    if (lastUsedNode != null) {
      usedNodes.add(lastUsedNode);
      lastUsedNode = null;
    }

    List<URI> currentNodes = liveNodes.getLiveNodesInternal();

    // Build list of available nodes (not yet used)
    List<URI> availableNodes = new ArrayList<>();
    for (URI node : currentNodes) {
      if (!usedNodes.contains(node)) {
        availableNodes.add(node);
      }
    }

    if (availableNodes.isEmpty()) {
      return null;
    }

    // Select a random node from available nodes
    int index = getRandom().nextInt(availableNodes.size());
    nextNode = availableNodes.get(index);

    return nextNode;
  }

  /**
   * Returns true if there are more nodes available that haven't been returned yet.
   *
   * @return true if there are unused nodes available
   */
  @Override
  public boolean hasNext() {
    return computeNextIfNeeded() != null;
  }

  /**
   * Returns the next available node that hasn't been returned yet. Nodes are selected in
   * pseudo-random order based on the seed.
   *
   * @return the next available node URI
   * @throws NoSuchElementException if no more nodes are available
   */
  @Override
  public URI next() {
    URI node = computeNextIfNeeded();
    if (node == null) {
      throw new NoSuchElementException("No more nodes available in query plan");
    }

    lastUsedNode = nextNode;
    nextNode = null;

    return node;
  }

  /**
   * Returns this iterator, allowing use in for-each loops.
   *
   * @return this iterator
   */
  @Override
  public Iterator<URI> iterator() {
    return this;
  }
}
