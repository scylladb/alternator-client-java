package com.scylladb.alternator.routing;

/**
 * A routing scope that targets all nodes in the Alternator cluster without filtering.
 *
 * <p>ClusterScope is the broadest scope and typically serves as the terminal fallback in a scope
 * chain. It does not apply any datacenter or rack filtering to the node list.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * // Direct cluster-wide targeting
 * RoutingScope scope = ClusterScope.create();
 *
 * // As a fallback in a scope chain
 * RoutingScope scope = DatacenterScope.of("dc1", ClusterScope.create());
 * }</pre>
 *
 * @author dmitry.kropachev
 * @see RoutingScope
 * @see DatacenterScope
 * @see RackScope
 * @since 2.0.0
 */
public final class ClusterScope implements RoutingScope {

  private static final ClusterScope INSTANCE = new ClusterScope();

  private ClusterScope() {}

  /**
   * Creates a new ClusterScope instance.
   *
   * <p>This method returns a singleton instance since ClusterScope has no configuration state.
   *
   * @return a ClusterScope instance
   */
  public static ClusterScope create() {
    return INSTANCE;
  }

  /** {@inheritDoc} */
  @Override
  public String getName() {
    return "Cluster";
  }

  /** {@inheritDoc} */
  @Override
  public String getDescription() {
    return "Cluster (all nodes)";
  }

  /**
   * Returns {@code null} since ClusterScope is a terminal scope with no fallback.
   *
   * @return {@code null}
   */
  @Override
  public RoutingScope getFallback() {
    return null;
  }

  /**
   * Returns an empty string since ClusterScope does not filter nodes.
   *
   * @return empty string
   */
  @Override
  public String getLocalNodesQuery() {
    return "";
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return "ClusterScope{}";
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object obj) {
    return obj instanceof ClusterScope;
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return ClusterScope.class.hashCode();
  }
}
