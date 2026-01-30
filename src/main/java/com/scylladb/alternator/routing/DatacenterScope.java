package com.scylladb.alternator.routing;

/**
 * A routing scope that targets nodes in a specific datacenter.
 *
 * <p>DatacenterScope filters the node list to include only nodes from the specified datacenter.
 * When no nodes are available in the datacenter, requests can fall back to a broader scope.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * // Datacenter targeting with fallback to cluster
 * RoutingScope scope = DatacenterScope.of("dc1", ClusterScope.create());
 *
 * // Strict datacenter targeting (no fallback)
 * RoutingScope scope = DatacenterScope.of("dc1", null);
 * }</pre>
 *
 * @author dmitry.kropachev
 * @see RoutingScope
 * @see ClusterScope
 * @see RackScope
 * @since 1.0.5
 */
public final class DatacenterScope implements RoutingScope {

  private final String datacenter;
  private final RoutingScope fallback;

  private DatacenterScope(String datacenter, RoutingScope fallback) {
    if (datacenter == null || datacenter.isEmpty()) {
      throw new IllegalArgumentException("datacenter cannot be null or empty");
    }
    this.datacenter = datacenter;
    this.fallback = fallback;
  }

  /**
   * Creates a new DatacenterScope for the specified datacenter with an optional fallback.
   *
   * @param datacenter the datacenter name (must not be null or empty)
   * @param fallback the fallback scope to use when no nodes are available in this datacenter, or
   *     {@code null} for strict targeting
   * @return a new DatacenterScope instance
   * @throws IllegalArgumentException if datacenter is null or empty
   */
  public static DatacenterScope of(String datacenter, RoutingScope fallback) {
    return new DatacenterScope(datacenter, fallback);
  }

  /**
   * Returns the datacenter name this scope targets.
   *
   * @return the datacenter name
   */
  public String getDatacenter() {
    return datacenter;
  }

  /** {@inheritDoc} */
  @Override
  public String getName() {
    return "Datacenter";
  }

  /** {@inheritDoc} */
  @Override
  public String getDescription() {
    return "Datacenter " + datacenter;
  }

  /** {@inheritDoc} */
  @Override
  public RoutingScope getFallback() {
    return fallback;
  }

  /** {@inheritDoc} */
  @Override
  public String getLocalNodesQuery() {
    return "dc=" + datacenter;
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return "DatacenterScope{datacenter='" + datacenter + "', fallback=" + fallback + "}";
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof DatacenterScope)) {
      return false;
    }
    DatacenterScope other = (DatacenterScope) obj;
    if (!datacenter.equals(other.datacenter)) {
      return false;
    }
    if (fallback == null) {
      return other.fallback == null;
    }
    return fallback.equals(other.fallback);
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    int result = datacenter.hashCode();
    result = 31 * result + (fallback != null ? fallback.hashCode() : 0);
    return result;
  }
}
