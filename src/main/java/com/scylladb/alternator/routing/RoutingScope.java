package com.scylladb.alternator.routing;

/**
 * Defines a composable routing scope for targeting specific Alternator nodes. Routing scopes allow
 * users to specify which nodes should be used for load balancing, with optional fallback chains for
 * hierarchical targeting.
 *
 * <p>The scope system follows a hierarchical model where requests can be targeted at specific
 * levels:
 *
 * <ul>
 *   <li>{@link RackScope} - Target nodes in a specific rack within a datacenter
 *   <li>{@link DatacenterScope} - Target nodes in a specific datacenter
 *   <li>{@link ClusterScope} - Target all nodes in the cluster (no filtering)
 * </ul>
 *
 * <p>Scopes can be chained with fallbacks, allowing automatic degradation when the preferred scope
 * has no available nodes:
 *
 * <pre>{@code
 * // Target rack1 in dc1, fall back to dc1, then to entire cluster
 * RoutingScope scope = RackScope.of("dc1", "rack1",
 *     DatacenterScope.of("dc1",
 *         ClusterScope.create()));
 * }</pre>
 *
 * @author dmitry.kropachev
 * @see ClusterScope
 * @see DatacenterScope
 * @see RackScope
 * @since 2.0.0
 */
public interface RoutingScope {

  /**
   * Returns the short name of this scope type.
   *
   * <p>Examples: "Cluster", "Datacenter", "Rack"
   *
   * @return the scope type name
   */
  String getName();

  /**
   * Returns a human-readable description of this scope.
   *
   * <p>This description includes the scope type and any configuration details, such as the
   * datacenter or rack name. Used primarily for logging fallback transitions.
   *
   * <p>Examples:
   *
   * <ul>
   *   <li>"Cluster (all nodes)"
   *   <li>"Datacenter dc1"
   *   <li>"Rack rack1 in Datacenter dc1"
   * </ul>
   *
   * @return the scope description
   */
  String getDescription();

  /**
   * Returns the fallback scope to use when this scope has no available nodes.
   *
   * <p>The fallback chain allows automatic degradation from more specific to less specific scopes.
   * For example, a rack scope might fall back to its datacenter scope, which might then fall back
   * to the cluster scope.
   *
   * <p>Returning {@code null} indicates this is a terminal scope with no fallback. If no nodes are
   * available in a terminal scope, the client will fail rather than degrade further.
   *
   * @return the fallback scope, or {@code null} if this is a terminal scope
   */
  RoutingScope getFallback();

  /**
   * Returns the query string to append to the {@code /localnodes} endpoint URL.
   *
   * <p>This query string filters the nodes returned by the Alternator node discovery endpoint. The
   * query string should not include the leading "?" character.
   *
   * <p>Examples:
   *
   * <ul>
   *   <li>"" (empty string) for cluster-wide scope
   *   <li>"dc=dc1" for datacenter scope
   *   <li>"dc=dc1&amp;rack=rack1" for rack scope
   * </ul>
   *
   * @return the query string for filtering nodes, or empty string for no filtering
   */
  String getLocalNodesQuery();
}
