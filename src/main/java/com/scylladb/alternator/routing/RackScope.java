package com.scylladb.alternator.routing;

/**
 * A routing scope that targets nodes in a specific rack within a datacenter.
 *
 * <p>RackScope is the most specific scope, filtering nodes to only those in a particular rack
 * within a datacenter. When no nodes are available in the rack, requests can fall back to a broader
 * scope such as DatacenterScope or ClusterScope.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * // Rack targeting with hierarchical fallback
 * RoutingScope scope = RackScope.of("dc1", "rack1",
 *     DatacenterScope.of("dc1",
 *         ClusterScope.create()));
 *
 * // Rack targeting with fallback to another rack
 * RoutingScope scope = RackScope.of("dc1", "rack1",
 *     RackScope.of("dc1", "rack2",
 *         DatacenterScope.of("dc1", ClusterScope.create())));
 *
 * // Strict rack targeting (no fallback)
 * RoutingScope scope = RackScope.of("dc1", "rack1", null);
 * }</pre>
 *
 * @author dmitry.kropachev
 * @see RoutingScope
 * @see ClusterScope
 * @see DatacenterScope
 * @since 2.0.0
 */
public final class RackScope implements RoutingScope {

  private final String datacenter;
  private final String rack;
  private final RoutingScope fallback;

  private RackScope(String datacenter, String rack, RoutingScope fallback) {
    if (datacenter == null || datacenter.isEmpty()) {
      throw new IllegalArgumentException("datacenter cannot be null or empty");
    }
    if (rack == null || rack.isEmpty()) {
      throw new IllegalArgumentException("rack cannot be null or empty");
    }
    this.datacenter = datacenter;
    this.rack = rack;
    this.fallback = fallback;
  }

  /**
   * Creates a new RackScope for the specified rack within a datacenter with an optional fallback.
   *
   * @param datacenter the datacenter name (must not be null or empty)
   * @param rack the rack name (must not be null or empty)
   * @param fallback the fallback scope to use when no nodes are available in this rack, or {@code
   *     null} for strict targeting
   * @return a new RackScope instance
   * @throws IllegalArgumentException if datacenter or rack is null or empty
   */
  public static RackScope of(String datacenter, String rack, RoutingScope fallback) {
    return new RackScope(datacenter, rack, fallback);
  }

  /**
   * Returns the datacenter name containing this rack.
   *
   * @return the datacenter name
   */
  public String getDatacenter() {
    return datacenter;
  }

  /**
   * Returns the rack name this scope targets.
   *
   * @return the rack name
   */
  public String getRack() {
    return rack;
  }

  /** {@inheritDoc} */
  @Override
  public String getName() {
    return "Rack";
  }

  /** {@inheritDoc} */
  @Override
  public String getDescription() {
    return "Rack " + rack + " in Datacenter " + datacenter;
  }

  /** {@inheritDoc} */
  @Override
  public RoutingScope getFallback() {
    return fallback;
  }

  /** {@inheritDoc} */
  @Override
  public String getLocalNodesQuery() {
    return "dc=" + datacenter + "&rack=" + rack;
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return "RackScope{datacenter='"
        + datacenter
        + "', rack='"
        + rack
        + "', fallback="
        + fallback
        + "}";
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof RackScope)) {
      return false;
    }
    RackScope other = (RackScope) obj;
    if (!datacenter.equals(other.datacenter)) {
      return false;
    }
    if (!rack.equals(other.rack)) {
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
    result = 31 * result + rack.hashCode();
    result = 31 * result + (fallback != null ? fallback.hashCode() : 0);
    return result;
  }
}
