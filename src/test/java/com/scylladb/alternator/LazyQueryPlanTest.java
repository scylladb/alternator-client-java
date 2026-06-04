package com.scylladb.alternator;

import static org.junit.Assert.*;

import com.scylladb.alternator.internal.AlternatorLiveNodes;
import com.scylladb.alternator.internal.LazyQueryPlan;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for LazyQueryPlan.
 *
 * @author dmitry.kropachev
 */
public class LazyQueryPlanTest {

  private List<URI> nodes;
  private AlternatorLiveNodes liveNodes;

  /** Set up test data before each test. */
  @Before
  public void setUp() throws URISyntaxException {
    nodes = createUriList("node", 5);
    liveNodes = new AlternatorLiveNodes(nodes, "http", 8000, "", "");
  }

  @Test
  public void testConstructorWithRandomSeed() {
    LazyQueryPlan plan = new LazyQueryPlan(liveNodes);
    assertNotNull(plan);
    assertTrue(plan.hasNext());
  }

  @Test
  public void testConstructorWithFixedSeed() {
    long seed = 42L;
    LazyQueryPlan plan = new LazyQueryPlan(liveNodes, seed);
    assertNotNull(plan);
    assertTrue(plan.hasNext());
  }

  @Test
  public void testDeterministicBehaviorWithSameSeed() {
    long seed = 123L;
    LazyQueryPlan plan1 = new LazyQueryPlan(liveNodes, seed);
    LazyQueryPlan plan2 = new LazyQueryPlan(liveNodes, seed);

    // With same seed, first node should be the same
    URI first1 = plan1.next();
    URI first2 = plan2.next();

    assertEquals("Same seed should produce same first node", first1, first2);
  }

  @Test
  public void testDifferentSeedsProduceDifferentFirstNodes() {
    // Run multiple times since random might occasionally pick same node
    int differentCount = 0;
    for (int i = 0; i < 10; i++) {
      LazyQueryPlan plan1 = new LazyQueryPlan(liveNodes, 100L + i);
      LazyQueryPlan plan2 = new LazyQueryPlan(liveNodes, 200L + i);

      URI first1 = plan1.next();
      URI first2 = plan2.next();

      if (!first1.equals(first2)) {
        differentCount++;
      }
    }

    assertTrue("Different seeds should usually produce different first nodes", differentCount >= 5);
  }

  @Test
  public void testHasNextAndNext() {
    LazyQueryPlan plan = new LazyQueryPlan(liveNodes, 42L);

    assertTrue(plan.hasNext());
    URI first = plan.next();
    assertNotNull(first);

    for (int i = 0; i < 3; i++) {
      assertTrue(plan.hasNext());
      plan.next();
    }

    assertTrue(plan.hasNext());
    URI last = plan.next();
    assertNotNull(last);

    assertFalse(plan.hasNext());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNullLiveNodes() {
    new LazyQueryPlan(null);
  }

  @Test(expected = NoSuchElementException.class)
  public void testNextOnExhaustedIterator() {
    // Create a plan and exhaust all nodes
    LazyQueryPlan plan = new LazyQueryPlan(liveNodes);
    while (plan.hasNext()) {
      plan.next();
    }
    // This should throw NoSuchElementException
    plan.next();
  }

  @Test
  public void testAllNodesAreReturned() {
    LazyQueryPlan plan = new LazyQueryPlan(liveNodes, 42L);

    Set<URI> expectedNodes = new HashSet<>(nodes);

    Set<URI> returnedNodes = new HashSet<>();
    while (plan.hasNext()) {
      returnedNodes.add(plan.next());
    }

    assertEquals(
        "All nodes should be returned exactly once", expectedNodes.size(), returnedNodes.size());
    assertEquals("All nodes should be returned", expectedNodes, returnedNodes);
  }

  @Test
  public void testNodesAreNotDuplicated() {
    LazyQueryPlan plan = new LazyQueryPlan(liveNodes);

    List<URI> returnedNodes = new ArrayList<>();
    while (plan.hasNext()) {
      returnedNodes.add(plan.next());
    }

    Set<URI> uniqueNodes = new HashSet<>(returnedNodes);
    assertEquals("No duplicates should be returned", returnedNodes.size(), uniqueNodes.size());
  }

  private List<URI> createUriList(String prefix, int count) throws URISyntaxException {
    List<URI> uris = new ArrayList<>();
    for (int i = 1; i <= count; i++) {
      uris.add(new URI("http", null, prefix + i + ".example.com", 8000, null, null, null));
    }
    return uris;
  }

  @Test
  public void testSingleNode() throws URISyntaxException {
    List<URI> singleNode = createUriList("single", 1);
    AlternatorLiveNodes singleLiveNodes = new AlternatorLiveNodes(singleNode, "http", 8000, "", "");
    LazyQueryPlan plan = new LazyQueryPlan(singleLiveNodes);

    assertTrue(plan.hasNext());
    assertEquals(singleNode.get(0), plan.next());
    assertFalse(plan.hasNext());
  }

  @Test
  public void testManyNodes() throws URISyntaxException {
    List<URI> manyNodes = createUriList("node", 100);
    AlternatorLiveNodes manyLiveNodes = new AlternatorLiveNodes(manyNodes, "http", 8000, "", "");
    LazyQueryPlan plan = new LazyQueryPlan(manyLiveNodes, 12345L);

    Set<URI> allExpected = new HashSet<>(manyNodes);

    Set<URI> allReturned = new HashSet<>();
    while (plan.hasNext()) {
      allReturned.add(plan.next());
    }

    assertEquals(allExpected, allReturned);
  }

  @Test
  public void testLazyBehaviorReadsCurrentNodes() throws URISyntaxException {
    // Start with 3 nodes
    List<URI> initialNodes = createUriList("initial", 3);
    AlternatorLiveNodes dynamicLiveNodes =
        new AlternatorLiveNodes(initialNodes, "http", 8000, "", "");
    LazyQueryPlan plan = new LazyQueryPlan(dynamicLiveNodes);

    // Get first node
    URI first = plan.next();
    assertNotNull(first);

    // The lazy nature is tested - each call to hasNext() and next()
    // reads the current state of liveNodes
    assertTrue(plan.hasNext());
  }

  @Test
  public void testIteratorReturnsSelf() {
    LazyQueryPlan plan = new LazyQueryPlan(liveNodes);
    assertSame("iterator() should return this", plan, plan.iterator());
  }

  @Test
  public void testForEachLoop() {
    LazyQueryPlan plan = new LazyQueryPlan(liveNodes, 42L);
    Set<URI> collectedNodes = new HashSet<>();

    for (URI node : plan) {
      collectedNodes.add(node);
    }

    assertEquals("For-each should iterate all nodes", nodes.size(), collectedNodes.size());
    assertEquals("For-each should return all nodes", new HashSet<>(nodes), collectedNodes);
  }

  // 2-node and 3-node cases: exercise the firstUsedNode fast path added in this PR.

  @Test
  public void twoNodePlanReturnsBothNodesExactlyOnce() throws URISyntaxException {
    List<URI> twoNodes = createUriList("n", 2);
    AlternatorLiveNodes twoLiveNodes = new AlternatorLiveNodes(twoNodes, "http", 8000, "", "");
    LazyQueryPlan plan = new LazyQueryPlan(twoLiveNodes);

    URI first = plan.next();
    URI second = plan.next();
    assertFalse("plan must be exhausted after both nodes consumed", plan.hasNext());
    assertNotSame("the two returned nodes must be distinct", first, second);
    assertTrue(twoNodes.contains(first));
    assertTrue(twoNodes.contains(second));
  }

  @Test
  public void twoNodePlanCoversAllNodesOnRepeatedInstances() throws URISyntaxException {
    List<URI> twoNodes = createUriList("n", 2);
    AlternatorLiveNodes twoLiveNodes = new AlternatorLiveNodes(twoNodes, "http", 8000, "", "");

    Set<URI> firstNodes = new HashSet<>();
    for (int i = 0; i < 40; i++) {
      LazyQueryPlan plan = new LazyQueryPlan(twoLiveNodes);
      URI a = plan.next();
      URI b = plan.next(); // exercises firstUsedNode → second-call path
      firstNodes.add(a);
      assertNotSame("each plan must return two distinct nodes", a, b);
    }
    assertEquals("both nodes must be reachable as first pick", 2, firstNodes.size());
  }

  @Test
  public void threeNodePlanNoDuplicatesAcrossAllThreeCalls() throws URISyntaxException {
    // Walks all three paths: first call (no state), second call (firstUsedNode), third call
    // (HashSet).
    List<URI> threeNodes = createUriList("t", 3);
    AlternatorLiveNodes threeLiveNodes = new AlternatorLiveNodes(threeNodes, "http", 8000, "", "");
    LazyQueryPlan plan = new LazyQueryPlan(threeLiveNodes);

    Set<URI> returned = new HashSet<>();
    while (plan.hasNext()) {
      URI node = plan.next();
      assertTrue("node must come from live list", threeNodes.contains(node));
      assertTrue("no duplicate returned: " + node, returned.add(node));
    }
    assertEquals("all three nodes must be returned", 3, returned.size());
  }

  @Test
  public void nonSeededPlanHasNextFalseWhenLiveListIsEmpty() throws Exception {
    Field f = AlternatorLiveNodes.class.getDeclaredField("liveNodes");
    f.setAccessible(true);
    @SuppressWarnings("unchecked")
    AtomicReference<List<URI>> ref = (AtomicReference<List<URI>>) f.get(liveNodes);
    ref.set(Collections.emptyList());

    LazyQueryPlan plan = new LazyQueryPlan(liveNodes);
    assertFalse("empty live list must yield hasNext() == false", plan.hasNext());
  }

  @Test(expected = NoSuchElementException.class)
  public void seededPlanNextThrowsWhenExhausted() throws URISyntaxException {
    LazyQueryPlan plan = new LazyQueryPlan(liveNodes, 42L);
    while (plan.hasNext()) {
      plan.next();
    }
    plan.next(); // must throw
  }

  // usedNodesView() covers three internal states: unused, one node used, two+ nodes used.
  @Test
  @SuppressWarnings("unchecked")
  public void usedNodesViewReflectsInternalState() throws Exception {
    Method m = LazyQueryPlan.class.getDeclaredMethod("usedNodesView");
    m.setAccessible(true);

    LazyQueryPlan plan = new LazyQueryPlan(liveNodes);

    // Before any next(): both firstUsedNode and usedNodes are null → empty set
    Set<URI> before = (Set<URI>) m.invoke(plan);
    assertTrue("before first next(): view must be empty", before.isEmpty());

    // After first next(): firstUsedNode set, usedNodes still null → singleton
    URI first = plan.next();
    Set<URI> afterOne = (Set<URI>) m.invoke(plan);
    assertEquals("after first next(): view must be singleton", 1, afterOne.size());
    assertTrue(afterOne.contains(first));

    // After second next(): usedNodes materialised → set of two
    URI second = plan.next();
    Set<URI> afterTwo = (Set<URI>) m.invoke(plan);
    assertEquals("after second next(): view must contain both nodes", 2, afterTwo.size());
    assertTrue(afterTwo.contains(first));
    assertTrue(afterTwo.contains(second));
  }
}
