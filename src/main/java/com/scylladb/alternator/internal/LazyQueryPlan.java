package com.scylladb.alternator.internal;

import java.net.URI;
import java.util.*;

/**
 * A lazy iterator over URI nodes that provides pseudo-random access to active nodes followed by
 * quarantined nodes. The iterator reads from active nodes first in pseudo-random order, then
 * continues with quarantined nodes, also in pseudo-random order.
 *
 * <p>The pseudo-random order is determined by a seed value, which can be either provided explicitly
 * (for reproducible sequences) or generated randomly (for non-deterministic behavior).
 *
 * <p>Example usage:
 * <pre>{@code
 * List<URI> activeNodes = Arrays.asList(uri1, uri2, uri3);
 * List<URI> quarantinedNodes = Arrays.asList(uri4, uri5);
 *
 * // Create with random seed
 * LazyQueryPlan plan = new LazyQueryPlan(activeNodes, quarantinedNodes);
 *
 * // Or create with hardcoded seed for reproducibility
 * LazyQueryPlan deterministicPlan = new LazyQueryPlan(activeNodes, quarantinedNodes, 42L);
 *
 * // Iterate through nodes
 * while (plan.hasNext()) {
 *     URI node = plan.next();
 *     // Use node...
 * }
 * }</pre>
 *
 * @author dmitry.kropachev
 * @since 1.0.5
 */
public class LazyQueryPlan implements Iterator<URI>, Iterable<URI> {
  private final List<URI> shuffledNodes;
  private int currentIndex;

  /**
   * Creates a LazyQueryPlan with a random seed. The iteration order will be non-deterministic.
   *
   * @param activeNodes a {@link java.util.List} of active URI nodes to iterate first
   * @param quarantinedNodes a {@link java.util.List} of quarantined URI nodes to iterate after
   *     active nodes
   */
  public LazyQueryPlan(List<URI> activeNodes, List<URI> quarantinedNodes) {
    this(activeNodes, quarantinedNodes, System.nanoTime());
  }

  /**
   * Creates a LazyQueryPlan with a specified seed for pseudo-random iteration. Using the same seed
   * with the same node lists will produce the same iteration order, enabling reproducible behavior.
   *
   * @param activeNodes a {@link java.util.List} of active URI nodes to iterate first
   * @param quarantinedNodes a {@link java.util.List} of quarantined URI nodes to iterate after
   *     active nodes
   * @param seed a long value used to initialize the pseudo-random number generator
   */
  public LazyQueryPlan(List<URI> activeNodes, List<URI> quarantinedNodes, long seed) {
    if (activeNodes == null) {
      throw new IllegalArgumentException("activeNodes cannot be null");
    }
    if (quarantinedNodes == null) {
      throw new IllegalArgumentException("quarantinedNodes cannot be null");
    }

    // Combine both lists - active nodes first, then quarantined
    this.shuffledNodes = new ArrayList<>(activeNodes.size() + quarantinedNodes.size());

    // Shuffle active nodes
    List<URI> activeNodesCopy = new ArrayList<>(activeNodes);
    Collections.shuffle(activeNodesCopy, new Random(seed));
    this.shuffledNodes.addAll(activeNodesCopy);

    // Shuffle quarantined nodes with a different seed derived from the original
    List<URI> quarantinedNodesCopy = new ArrayList<>(quarantinedNodes);
    Collections.shuffle(quarantinedNodesCopy, new Random(seed + 1));
    this.shuffledNodes.addAll(quarantinedNodesCopy);

    this.currentIndex = 0;
  }

  /** {@inheritDoc} */
  @Override
  public boolean hasNext() {
    return currentIndex < shuffledNodes.size();
  }

  /** {@inheritDoc} */
  @Override
  public URI next() {
    if (!hasNext()) {
      throw new NoSuchElementException("No more nodes available in query plan");
    }
    return shuffledNodes.get(currentIndex++);
  }

  /**
   * Returns the total number of nodes (active + quarantined) in this query plan.
   *
   * @return the total number of nodes
   */
  public int size() {
    return shuffledNodes.size();
  }

  /**
   * Returns the number of remaining nodes that haven't been iterated yet.
   *
   * @return the number of remaining nodes
   */
  public int remaining() {
    return shuffledNodes.size() - currentIndex;
  }

  /**
   * Resets the iterator to the beginning, allowing re-iteration over the same shuffled sequence.
   */
  public void reset() {
    this.currentIndex = 0;
  }

  /**
   * Returns an iterator over the nodes in this query plan. Note that this returns the same
   * instance (after resetting), so only one iteration can be active at a time.
   *
   * @return this query plan as an iterator, reset to the beginning
   */
  @Override
  public Iterator<URI> iterator() {
    reset();
    return this;
  }

  /**
   * Returns a copy of all nodes in this query plan in their shuffled order. This is useful when
   * you need to examine the full sequence without consuming the iterator.
   *
   * @return an unmodifiable list of all nodes in shuffled order (active nodes first, then
   *     quarantined)
   */
  public List<URI> getNodes() {
    return Collections.unmodifiableList(shuffledNodes);
  }
}
