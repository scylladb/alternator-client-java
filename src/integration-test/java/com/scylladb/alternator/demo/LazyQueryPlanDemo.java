package com.scylladb.alternator.demo;

import com.scylladb.alternator.internal.AlternatorLiveNodes;
import com.scylladb.alternator.internal.LazyQueryPlan;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

/**
 * Demonstration of LazyQueryPlan functionality.
 *
 * <p>LazyQueryPlan is truly lazy - it pulls nodes one at a time from AlternatorLiveNodes only when
 * needed, and tracks which nodes have been returned to avoid duplicates.
 *
 * @author dmitry.kropachev
 */
public class LazyQueryPlanDemo {

  /**
   * Main method to run demonstrations of LazyQueryPlan.
   *
   * @param args command line arguments (not used)
   * @throws java.net.URISyntaxException if URI construction fails
   */
  public static void main(String[] args) throws URISyntaxException {
    System.out.println("=== LazyQueryPlan Test and Demonstration ===\n");

    // Create sample URIs
    List<URI> nodes = createUriList("node", 5);
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(nodes, "http", 8000, "", "");

    System.out.println("Available nodes: " + nodes);
    System.out.println();

    // Test 1: Random seed (non-deterministic)
    System.out.println("--- Test 1: Random seed (two runs may differ) ---");
    LazyQueryPlan plan1 = new LazyQueryPlan(liveNodes);
    System.out.print("Run 1: ");
    printPlan(plan1);

    LazyQueryPlan plan2 = new LazyQueryPlan(liveNodes);
    System.out.print("Run 2: ");
    printPlan(plan2);
    System.out.println();

    // Test 2: Fixed seed (deterministic/reproducible first node)
    System.out.println("--- Test 2: Fixed seed (first node should be identical) ---");
    long seed = 42L;
    LazyQueryPlan plan3 = new LazyQueryPlan(liveNodes, seed);
    System.out.print("Run 1 (seed=" + seed + "): ");
    URI first1 = plan3.next();
    System.out.println("First node: " + first1.getHost());

    LazyQueryPlan plan4 = new LazyQueryPlan(liveNodes, seed);
    System.out.print("Run 2 (seed=" + seed + "): ");
    URI first2 = plan4.next();
    System.out.println("First node: " + first2.getHost());
    System.out.println("First nodes match: " + first1.equals(first2));
    System.out.println();

    // Test 3: Different seeds produce different first nodes
    System.out.println("--- Test 3: Different seeds produce different first nodes ---");
    LazyQueryPlan plan5 = new LazyQueryPlan(liveNodes, 100L);
    System.out.print("Seed 100: ");
    printPlan(plan5);

    LazyQueryPlan plan6 = new LazyQueryPlan(liveNodes, 200L);
    System.out.print("Seed 200: ");
    printPlan(plan6);
    System.out.println();

    // Test 4: Iterator operations
    System.out.println("--- Test 4: Iterator operations ---");
    LazyQueryPlan plan7 = new LazyQueryPlan(liveNodes, 123L);
    System.out.println("hasNext before iteration: " + plan7.hasNext());

    System.out.println("Iterating through first 3 nodes:");
    for (int i = 0; i < 3 && plan7.hasNext(); i++) {
      URI node = plan7.next();
      System.out.println("  Node " + (i + 1) + ": " + node.getHost());
    }
    System.out.println("hasNext after 3 iterations: " + plan7.hasNext());
    System.out.println();

    // Test 5: Lazy behavior - no node duplication
    System.out.println("--- Test 5: Verify no duplicate nodes returned ---");
    LazyQueryPlan plan8 = new LazyQueryPlan(liveNodes, 999L);
    Set<URI> seenNodes = new HashSet<>();
    boolean duplicateFound = false;

    while (plan8.hasNext()) {
      URI node = plan8.next();
      if (seenNodes.contains(node)) {
        System.out.println("ERROR: Duplicate node found: " + node);
        duplicateFound = true;
      }
      seenNodes.add(node);
    }

    System.out.println("Total unique nodes returned: " + seenNodes.size());
    System.out.println("No duplicates: " + !duplicateFound);
    System.out.println();

    // Test 6: Empty node list
    System.out.println("--- Test 6: Edge cases ---");
    AlternatorLiveNodes emptyLiveNodes =
        new AlternatorLiveNodes(Collections.<URI>emptyList(), "http", 8000, "", "");
    LazyQueryPlan emptyPlan = new LazyQueryPlan(emptyLiveNodes);
    System.out.println("Empty node list hasNext: " + emptyPlan.hasNext());
    System.out.println();

    // Test 7: Exception handling
    System.out.println("--- Test 7: Exception handling ---");
    try {
      new LazyQueryPlan(null);
      System.out.println("ERROR: Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      System.out.println(
          "Correctly threw IllegalArgumentException for null liveNodes: " + e.getMessage());
    }

    try {
      LazyQueryPlan exhausted = new LazyQueryPlan(emptyLiveNodes);
      exhausted.next();
      System.out.println("ERROR: Should have thrown NoSuchElementException");
    } catch (NoSuchElementException e) {
      System.out.println(
          "Correctly threw NoSuchElementException when exhausted: " + e.getMessage());
    }
    System.out.println();

    // Test 8: Iterable interface (for-each loop)
    System.out.println("--- Test 8: Iterable interface (for-each loop) ---");
    LazyQueryPlan iterablePlan = new LazyQueryPlan(liveNodes, 42L);
    System.out.print("Using for-each: ");
    List<String> forEachHosts = new ArrayList<>();
    for (URI node : iterablePlan) {
      forEachHosts.add(node.getHost().split("\\.")[0]);
    }
    System.out.println(forEachHosts);
    System.out.println();

    // Test 9: Iterator exhaustion
    System.out.println("--- Test 9: Iterator exhaustion ---");
    LazyQueryPlan trackPlan = new LazyQueryPlan(liveNodes, 42L);
    trackPlan.next();
    trackPlan.next();
    System.out.println("hasNext after 2 calls: " + trackPlan.hasNext());
    int remainingCount = 0;
    while (trackPlan.hasNext()) {
      trackPlan.next();
      remainingCount++;
    }
    System.out.println("Remaining nodes after 2 calls: " + remainingCount);

    System.out.println("\n=== All tests completed ===");
  }

  private static List<URI> createUriList(String prefix, int count) throws URISyntaxException {
    List<URI> uris = new ArrayList<>();
    for (int i = 1; i <= count; i++) {
      uris.add(
          new URI("http", null, prefix + i + ".example.com", 8000, null, null, null));
    }
    return uris;
  }

  private static void printPlan(LazyQueryPlan plan) {
    List<String> hosts = new ArrayList<>();
    while (plan.hasNext()) {
      URI node = plan.next();
      String host = node.getHost();
      // Extract just the prefix and number for readability
      hosts.add(host.split("\\.")[0]);
    }
    System.out.println(hosts);
  }
}
