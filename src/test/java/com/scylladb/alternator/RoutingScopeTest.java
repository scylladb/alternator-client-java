package com.scylladb.alternator;

import static org.junit.Assert.*;

import com.scylladb.alternator.routing.ClusterScope;
import com.scylladb.alternator.routing.DatacenterScope;
import com.scylladb.alternator.routing.RackScope;
import com.scylladb.alternator.routing.RoutingScope;
import org.junit.Test;

/**
 * Unit tests for RoutingScope implementations.
 *
 * @author dmitry.kropachev
 */
public class RoutingScopeTest {

  // ============== ClusterScope Tests ==============

  @Test
  public void testClusterScopeCreate() {
    ClusterScope scope = ClusterScope.create();
    assertNotNull(scope);
  }

  @Test
  public void testClusterScopeSingleton() {
    ClusterScope scope1 = ClusterScope.create();
    ClusterScope scope2 = ClusterScope.create();
    assertSame("ClusterScope should return singleton instance", scope1, scope2);
  }

  @Test
  public void testClusterScopeGetName() {
    ClusterScope scope = ClusterScope.create();
    assertEquals("Cluster", scope.getName());
  }

  @Test
  public void testClusterScopeGetDescription() {
    ClusterScope scope = ClusterScope.create();
    assertEquals("Cluster (all nodes)", scope.getDescription());
  }

  @Test
  public void testClusterScopeGetFallback() {
    ClusterScope scope = ClusterScope.create();
    assertNull("ClusterScope should have no fallback", scope.getFallback());
  }

  @Test
  public void testClusterScopeGetLocalNodesQuery() {
    ClusterScope scope = ClusterScope.create();
    assertEquals("", scope.getLocalNodesQuery());
  }

  @Test
  public void testClusterScopeToString() {
    ClusterScope scope = ClusterScope.create();
    assertEquals("ClusterScope{}", scope.toString());
  }

  @Test
  public void testClusterScopeEquals() {
    ClusterScope scope1 = ClusterScope.create();
    ClusterScope scope2 = ClusterScope.create();
    assertEquals(scope1, scope2);
    assertEquals(scope1.hashCode(), scope2.hashCode());
  }

  // ============== DatacenterScope Tests ==============

  @Test
  public void testDatacenterScopeCreate() {
    DatacenterScope scope = DatacenterScope.of("dc1", null);
    assertNotNull(scope);
    assertEquals("dc1", scope.getDatacenter());
  }

  @Test
  public void testDatacenterScopeWithFallback() {
    ClusterScope fallback = ClusterScope.create();
    DatacenterScope scope = DatacenterScope.of("dc1", fallback);
    assertEquals(fallback, scope.getFallback());
  }

  @Test
  public void testDatacenterScopeWithoutFallback() {
    DatacenterScope scope = DatacenterScope.of("dc1", null);
    assertNull(scope.getFallback());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDatacenterScopeWithNullDatacenter() {
    DatacenterScope.of(null, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDatacenterScopeWithEmptyDatacenter() {
    DatacenterScope.of("", null);
  }

  @Test
  public void testDatacenterScopeGetName() {
    DatacenterScope scope = DatacenterScope.of("dc1", null);
    assertEquals("Datacenter", scope.getName());
  }

  @Test
  public void testDatacenterScopeGetDescription() {
    DatacenterScope scope = DatacenterScope.of("dc1", null);
    assertEquals("Datacenter dc1", scope.getDescription());
  }

  @Test
  public void testDatacenterScopeGetLocalNodesQuery() {
    DatacenterScope scope = DatacenterScope.of("dc1", null);
    assertEquals("dc=dc1", scope.getLocalNodesQuery());
  }

  @Test
  public void testDatacenterScopeToString() {
    DatacenterScope scope = DatacenterScope.of("dc1", ClusterScope.create());
    assertTrue(scope.toString().contains("dc1"));
    assertTrue(scope.toString().contains("ClusterScope"));
  }

  @Test
  public void testDatacenterScopeEquals() {
    DatacenterScope scope1 = DatacenterScope.of("dc1", ClusterScope.create());
    DatacenterScope scope2 = DatacenterScope.of("dc1", ClusterScope.create());
    DatacenterScope scope3 = DatacenterScope.of("dc2", ClusterScope.create());
    DatacenterScope scope4 = DatacenterScope.of("dc1", null);

    assertEquals(scope1, scope2);
    assertEquals(scope1.hashCode(), scope2.hashCode());
    assertNotEquals(scope1, scope3);
    assertNotEquals(scope1, scope4);
  }

  // ============== RackScope Tests ==============

  @Test
  public void testRackScopeCreate() {
    RackScope scope = RackScope.of("dc1", "rack1", null);
    assertNotNull(scope);
    assertEquals("dc1", scope.getDatacenter());
    assertEquals("rack1", scope.getRack());
  }

  @Test
  public void testRackScopeWithFallback() {
    DatacenterScope fallback = DatacenterScope.of("dc1", ClusterScope.create());
    RackScope scope = RackScope.of("dc1", "rack1", fallback);
    assertEquals(fallback, scope.getFallback());
  }

  @Test
  public void testRackScopeWithoutFallback() {
    RackScope scope = RackScope.of("dc1", "rack1", null);
    assertNull(scope.getFallback());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRackScopeWithNullDatacenter() {
    RackScope.of(null, "rack1", null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRackScopeWithEmptyDatacenter() {
    RackScope.of("", "rack1", null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRackScopeWithNullRack() {
    RackScope.of("dc1", null, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRackScopeWithEmptyRack() {
    RackScope.of("dc1", "", null);
  }

  @Test
  public void testRackScopeGetName() {
    RackScope scope = RackScope.of("dc1", "rack1", null);
    assertEquals("Rack", scope.getName());
  }

  @Test
  public void testRackScopeGetDescription() {
    RackScope scope = RackScope.of("dc1", "rack1", null);
    assertEquals("Rack rack1 in Datacenter dc1", scope.getDescription());
  }

  @Test
  public void testRackScopeGetLocalNodesQuery() {
    RackScope scope = RackScope.of("dc1", "rack1", null);
    assertEquals("dc=dc1&rack=rack1", scope.getLocalNodesQuery());
  }

  @Test
  public void testRackScopeToString() {
    RackScope scope = RackScope.of("dc1", "rack1", null);
    assertTrue(scope.toString().contains("dc1"));
    assertTrue(scope.toString().contains("rack1"));
  }

  @Test
  public void testRackScopeEquals() {
    RackScope scope1 = RackScope.of("dc1", "rack1", null);
    RackScope scope2 = RackScope.of("dc1", "rack1", null);
    RackScope scope3 = RackScope.of("dc1", "rack2", null);
    RackScope scope4 = RackScope.of("dc2", "rack1", null);

    assertEquals(scope1, scope2);
    assertEquals(scope1.hashCode(), scope2.hashCode());
    assertNotEquals(scope1, scope3);
    assertNotEquals(scope1, scope4);
  }

  // ============== Fallback Chain Tests ==============

  @Test
  public void testFallbackChainRackToDatacenterToCluster() {
    RoutingScope scope =
        RackScope.of("dc1", "rack1", DatacenterScope.of("dc1", ClusterScope.create()));

    // First level: Rack
    assertEquals("Rack", scope.getName());
    assertEquals("dc=dc1&rack=rack1", scope.getLocalNodesQuery());

    // Second level: Datacenter
    RoutingScope fallback1 = scope.getFallback();
    assertNotNull(fallback1);
    assertEquals("Datacenter", fallback1.getName());
    assertEquals("dc=dc1", fallback1.getLocalNodesQuery());

    // Third level: Cluster
    RoutingScope fallback2 = fallback1.getFallback();
    assertNotNull(fallback2);
    assertEquals("Cluster", fallback2.getName());
    assertEquals("", fallback2.getLocalNodesQuery());

    // No more fallbacks
    assertNull(fallback2.getFallback());
  }

  @Test
  public void testFallbackChainRackToRackToDatacenterToCluster() {
    RoutingScope scope =
        RackScope.of(
            "dc1",
            "rack1",
            RackScope.of("dc1", "rack2", DatacenterScope.of("dc1", ClusterScope.create())));

    // First level: Rack1
    assertEquals("Rack rack1 in Datacenter dc1", scope.getDescription());

    // Second level: Rack2
    RoutingScope fallback1 = scope.getFallback();
    assertNotNull(fallback1);
    assertEquals("Rack rack2 in Datacenter dc1", fallback1.getDescription());

    // Third level: Datacenter
    RoutingScope fallback2 = fallback1.getFallback();
    assertNotNull(fallback2);
    assertEquals("Datacenter dc1", fallback2.getDescription());

    // Fourth level: Cluster
    RoutingScope fallback3 = fallback2.getFallback();
    assertNotNull(fallback3);
    assertEquals("Cluster (all nodes)", fallback3.getDescription());

    // No more fallbacks
    assertNull(fallback3.getFallback());
  }

  @Test
  public void testStrictTargetingNoFallback() {
    DatacenterScope scope = DatacenterScope.of("dc1", null);
    assertNull("Strict targeting should have no fallback", scope.getFallback());
  }

  @Test
  public void testFallbackChainTraversal() {
    RoutingScope scope =
        RackScope.of("dc1", "rack1", DatacenterScope.of("dc1", ClusterScope.create()));

    // Count the number of scopes in the chain
    int count = 0;
    RoutingScope current = scope;
    while (current != null) {
      count++;
      current = current.getFallback();
    }
    assertEquals("Chain should have 3 scopes (rack -> dc -> cluster)", 3, count);
  }

  // ============== Cross-type Equality Tests ==============

  @Test
  public void testDifferentScopeTypesNotEqual() {
    ClusterScope clusterScope = ClusterScope.create();
    DatacenterScope datacenterScope = DatacenterScope.of("dc1", null);
    RackScope rackScope = RackScope.of("dc1", "rack1", null);

    assertNotEquals(clusterScope, datacenterScope);
    assertNotEquals(datacenterScope, rackScope);
    assertNotEquals(clusterScope, rackScope);
  }

  // ============== AlternatorConfig Integration Tests ==============

  @Test
  public void testAlternatorConfigWithRoutingScope() {
    RoutingScope scope =
        RackScope.of("dc1", "rack1", DatacenterScope.of("dc1", ClusterScope.create()));

    AlternatorConfig config = AlternatorConfig.builder().withRoutingScope(scope).build();

    assertEquals(scope, config.getRoutingScope());
  }

  @Test
  public void testAlternatorConfigWithoutRoutingScopeDefaultsToCluster() {
    AlternatorConfig config = AlternatorConfig.builder().build();
    RoutingScope scope = config.getRoutingScope();
    assertNotNull("RoutingScope should never be null", scope);
    assertTrue("Default scope should be ClusterScope", scope instanceof ClusterScope);
    assertEquals("", scope.getLocalNodesQuery());
  }

  @Test
  public void testAlternatorConfigWithNullRoutingScopeDefaultsToCluster() {
    AlternatorConfig config = AlternatorConfig.builder().withRoutingScope(null).build();
    RoutingScope scope = config.getRoutingScope();
    assertNotNull("RoutingScope should never be null even when explicitly set to null", scope);
    assertTrue("Null routing scope should default to ClusterScope", scope instanceof ClusterScope);
  }
}
