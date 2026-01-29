package com.scylladb.alternator;

import static org.junit.Assert.*;

import com.scylladb.alternator.internal.LazyQueryPlan;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for LazyQueryPlan.
 *
 * @author dmitry.kropachev
 */
public class LazyQueryPlanTest {

  private List<URI> activeNodes;
  private List<URI> quarantinedNodes;

  /** Set up test data before each test. */
  @Before
  public void setUp() throws URISyntaxException {
    activeNodes = createUriList("active", 5);
    quarantinedNodes = createUriList("quarantined", 3);
  }

  @Test
  public void testConstructorWithRandomSeed() {
    LazyQueryPlan plan = new LazyQueryPlan(activeNodes, quarantinedNodes);
    assertNotNull(plan);
    assertEquals(8, plan.size());
    assertEquals(8, plan.remaining());
  }

  @Test
  public void testConstructorWithFixedSeed() {
    long seed = 42L;
    LazyQueryPlan plan = new LazyQueryPlan(activeNodes, quarantinedNodes, seed);
    assertNotNull(plan);
    assertEquals(8, plan.size());
  }

  @Test
  public void testDeterministicBehaviorWithSameSeed() {
    long seed = 123L;
    LazyQueryPlan plan1 = new LazyQueryPlan(activeNodes, quarantinedNodes, seed);
    LazyQueryPlan plan2 = new LazyQueryPlan(activeNodes, quarantinedNodes, seed);

    List<URI> sequence1 = getAllNodes(plan1);
    List<URI> sequence2 = getAllNodes(plan2);

    assertEquals("Same seed should produce same sequence", sequence1, sequence2);
  }

  @Test
  public void testDifferentSeedsProduceDifferentSequences() {
    LazyQueryPlan plan1 = new LazyQueryPlan(activeNodes, quarantinedNodes, 100L);
    LazyQueryPlan plan2 = new LazyQueryPlan(activeNodes, quarantinedNodes, 200L);

    List<URI> sequence1 = getAllNodes(plan1);
    List<URI> sequence2 = getAllNodes(plan2);

    assertNotEquals("Different seeds should produce different sequences", sequence1, sequence2);
  }

  @Test
  public void testActiveNodesAppearBeforeQuarantined() {
    LazyQueryPlan plan = new LazyQueryPlan(activeNodes, quarantinedNodes, 999L);

    Set<String> activeHosts = new HashSet<>();
    for (URI uri : activeNodes) {
      activeHosts.add(uri.getHost());
    }

    int activeCount = 0;
    int quarantinedCount = 0;
    boolean foundQuarantined = false;

    while (plan.hasNext()) {
      URI node = plan.next();
      if (activeHosts.contains(node.getHost())) {
        activeCount++;
        assertFalse("Active nodes should appear before quarantined nodes", foundQuarantined);
      } else {
        quarantinedCount++;
        foundQuarantined = true;
      }
    }

    assertEquals("All active nodes should be present", activeNodes.size(), activeCount);
    assertEquals(
        "All quarantined nodes should be present", quarantinedNodes.size(), quarantinedCount);
  }

  @Test
  public void testHasNextAndNext() {
    LazyQueryPlan plan = new LazyQueryPlan(activeNodes, quarantinedNodes, 42L);

    assertTrue(plan.hasNext());
    URI first = plan.next();
    assertNotNull(first);
    assertEquals(7, plan.remaining());

    for (int i = 0; i < 6; i++) {
      assertTrue(plan.hasNext());
      plan.next();
    }

    assertTrue(plan.hasNext());
    URI last = plan.next();
    assertNotNull(last);

    assertFalse(plan.hasNext());
    assertEquals(0, plan.remaining());
  }

  @Test
  public void testSizeAndRemaining() {
    LazyQueryPlan plan = new LazyQueryPlan(activeNodes, quarantinedNodes);

    assertEquals(8, plan.size());
    assertEquals(8, plan.remaining());

    plan.next();
    assertEquals(8, plan.size());
    assertEquals(7, plan.remaining());

    plan.next();
    plan.next();
    assertEquals(8, plan.size());
    assertEquals(5, plan.remaining());
  }

  @Test
  public void testReset() {
    LazyQueryPlan plan = new LazyQueryPlan(activeNodes, quarantinedNodes, 42L);

    List<URI> firstPass = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      firstPass.add(plan.next());
    }
    assertEquals(5, plan.remaining());

    plan.reset();
    assertEquals(8, plan.remaining());
    assertTrue(plan.hasNext());

    List<URI> secondPass = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      secondPass.add(plan.next());
    }

    assertEquals("Reset should return to beginning", firstPass, secondPass);
  }

  @Test
  public void testEmptyActiveNodes() {
    LazyQueryPlan plan = new LazyQueryPlan(Collections.<URI>emptyList(), quarantinedNodes);

    assertEquals(3, plan.size());
    assertTrue(plan.hasNext());

    int count = 0;
    while (plan.hasNext()) {
      plan.next();
      count++;
    }
    assertEquals(3, count);
  }

  @Test
  public void testEmptyQuarantinedNodes() {
    LazyQueryPlan plan = new LazyQueryPlan(activeNodes, Collections.<URI>emptyList());

    assertEquals(5, plan.size());
    assertTrue(plan.hasNext());

    int count = 0;
    while (plan.hasNext()) {
      plan.next();
      count++;
    }
    assertEquals(5, count);
  }

  @Test
  public void testBothEmpty() {
    LazyQueryPlan plan =
        new LazyQueryPlan(Collections.<URI>emptyList(), Collections.<URI>emptyList());

    assertEquals(0, plan.size());
    assertEquals(0, plan.remaining());
    assertFalse(plan.hasNext());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNullActiveNodes() {
    new LazyQueryPlan(null, quarantinedNodes);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNullQuarantinedNodes() {
    new LazyQueryPlan(activeNodes, null);
  }

  @Test(expected = NoSuchElementException.class)
  public void testNextOnExhaustedIterator() {
    LazyQueryPlan plan =
        new LazyQueryPlan(Collections.<URI>emptyList(), Collections.<URI>emptyList());
    plan.next();
  }

  @Test
  public void testAllNodesAreReturned() {
    LazyQueryPlan plan = new LazyQueryPlan(activeNodes, quarantinedNodes, 42L);

    Set<URI> expectedNodes = new HashSet<>();
    expectedNodes.addAll(activeNodes);
    expectedNodes.addAll(quarantinedNodes);

    Set<URI> returnedNodes = new HashSet<>();
    while (plan.hasNext()) {
      returnedNodes.add(plan.next());
    }

    assertEquals(
        "All nodes should be returned exactly once", expectedNodes.size(), returnedNodes.size());
    assertEquals("All nodes should be returned", expectedNodes, returnedNodes);
  }

  @Test
  public void testPseudoRandomDistribution() {
    // Test that nodes are actually shuffled (not in original order)
    LazyQueryPlan plan = new LazyQueryPlan(activeNodes, quarantinedNodes, 42L);

    List<URI> returnedOrder = getAllNodes(plan);
    List<URI> originalOrder = new ArrayList<>();
    originalOrder.addAll(activeNodes);
    originalOrder.addAll(quarantinedNodes);

    // With a seed of 42 and 8 nodes, it's extremely unlikely to match original order
    boolean isDifferent = false;
    for (int i = 0; i < returnedOrder.size(); i++) {
      if (!returnedOrder.get(i).equals(originalOrder.get(i))) {
        isDifferent = true;
        break;
      }
    }

    assertTrue("Nodes should be shuffled, not in original order", isDifferent);
  }

  private List<URI> createUriList(String prefix, int count) throws URISyntaxException {
    List<URI> uris = new ArrayList<>();
    for (int i = 1; i <= count; i++) {
      uris.add(
          new URI("http", null, prefix + "-node" + i + ".example.com", 8000, null, null, null));
    }
    return uris;
  }

  @Test
  public void testIterableInterface() {
    LazyQueryPlan plan = new LazyQueryPlan(activeNodes, quarantinedNodes, 42L);

    List<URI> fromForEach = new ArrayList<>();
    for (URI uri : plan) {
      fromForEach.add(uri);
    }

    assertEquals(8, fromForEach.size());

    // Verify iterator() resets the plan
    List<URI> secondPass = new ArrayList<>();
    for (URI uri : plan) {
      secondPass.add(uri);
    }

    assertEquals("Iterator should reset on each call", fromForEach, secondPass);
  }

  @Test
  public void testGetNodesReturnsUnmodifiableList() {
    LazyQueryPlan plan = new LazyQueryPlan(activeNodes, quarantinedNodes, 42L);

    List<URI> nodes = plan.getNodes();
    assertEquals(8, nodes.size());

    try {
      nodes.add(activeNodes.get(0));
      fail("getNodes() should return unmodifiable list");
    } catch (UnsupportedOperationException e) {
      // Expected
    }
  }

  @Test
  public void testGetNodesDoesNotAffectIteration() {
    LazyQueryPlan plan = new LazyQueryPlan(activeNodes, quarantinedNodes, 42L);

    // Get nodes before iteration
    List<URI> allNodes = plan.getNodes();

    // Consume a few nodes
    plan.next();
    plan.next();

    // getNodes() should still return all nodes
    assertEquals(allNodes, plan.getNodes());
    assertEquals(6, plan.remaining());
  }

  @Test
  public void testSingleActiveNode() throws URISyntaxException {
    List<URI> singleNode = createUriList("single", 1);
    LazyQueryPlan plan = new LazyQueryPlan(singleNode, Collections.<URI>emptyList());

    assertEquals(1, plan.size());
    assertTrue(plan.hasNext());
    assertEquals(singleNode.get(0), plan.next());
    assertFalse(plan.hasNext());
  }

  @Test
  public void testSingleQuarantinedNode() throws URISyntaxException {
    List<URI> singleNode = createUriList("quarantined", 1);
    LazyQueryPlan plan = new LazyQueryPlan(Collections.<URI>emptyList(), singleNode);

    assertEquals(1, plan.size());
    assertTrue(plan.hasNext());
    assertEquals(singleNode.get(0), plan.next());
    assertFalse(plan.hasNext());
  }

  @Test
  public void testManyNodes() throws URISyntaxException {
    List<URI> manyActive = createUriList("active", 100);
    List<URI> manyQuarantined = createUriList("quarantined", 50);
    LazyQueryPlan plan = new LazyQueryPlan(manyActive, manyQuarantined, 12345L);

    assertEquals(150, plan.size());

    Set<URI> allExpected = new HashSet<>();
    allExpected.addAll(manyActive);
    allExpected.addAll(manyQuarantined);

    Set<URI> allReturned = new HashSet<>();
    while (plan.hasNext()) {
      allReturned.add(plan.next());
    }

    assertEquals(allExpected, allReturned);
  }

  @Test
  public void testResetAfterPartialIteration() {
    LazyQueryPlan plan = new LazyQueryPlan(activeNodes, quarantinedNodes, 42L);

    URI first = plan.next();
    URI second = plan.next();

    plan.reset();

    assertEquals("Reset should return to first node", first, plan.next());
    assertEquals("Second node should match", second, plan.next());
  }

  @Test
  public void testIteratorResetsToBeginning() {
    LazyQueryPlan plan = new LazyQueryPlan(activeNodes, quarantinedNodes, 42L);

    // Consume some nodes
    plan.next();
    plan.next();
    plan.next();

    // Call iterator() which should reset
    Iterator<URI> it = plan.iterator();
    assertEquals(8, plan.remaining());
    assertTrue(it.hasNext());
  }

  private List<URI> getAllNodes(LazyQueryPlan plan) {
    List<URI> nodes = new ArrayList<>();
    while (plan.hasNext()) {
      nodes.add(plan.next());
    }
    return nodes;
  }
}
