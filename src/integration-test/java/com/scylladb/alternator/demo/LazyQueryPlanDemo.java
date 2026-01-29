package com.scylladb.alternator.demo;

import com.scylladb.alternator.internal.LazyQueryPlan;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

/**
 * Demonstration of LazyQueryPlan functionality.
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
    List<URI> activeNodes = createUriList("active", 5);
    List<URI> quarantinedNodes = createUriList("quarantined", 3);

    System.out.println("Active nodes: " + activeNodes);
    System.out.println("Quarantined nodes: " + quarantinedNodes);
    System.out.println();

    // Test 1: Random seed (non-deterministic)
    System.out.println("--- Test 1: Random seed (two runs should differ) ---");
    LazyQueryPlan plan1 = new LazyQueryPlan(activeNodes, quarantinedNodes);
    System.out.print("Run 1: ");
    printPlan(plan1);

    LazyQueryPlan plan2 = new LazyQueryPlan(activeNodes, quarantinedNodes);
    System.out.print("Run 2: ");
    printPlan(plan2);
    System.out.println();

    // Test 2: Fixed seed (deterministic/reproducible)
    System.out.println("--- Test 2: Fixed seed (two runs should be identical) ---");
    long seed = 42L;
    LazyQueryPlan plan3 = new LazyQueryPlan(activeNodes, quarantinedNodes, seed);
    System.out.print("Run 1 (seed=" + seed + "): ");
    printPlan(plan3);

    LazyQueryPlan plan4 = new LazyQueryPlan(activeNodes, quarantinedNodes, seed);
    System.out.print("Run 2 (seed=" + seed + "): ");
    printPlan(plan4);
    System.out.println();

    // Test 3: Different seeds produce different orders
    System.out.println("--- Test 3: Different seeds produce different orders ---");
    LazyQueryPlan plan5 = new LazyQueryPlan(activeNodes, quarantinedNodes, 100L);
    System.out.print("Seed 100: ");
    printPlan(plan5);

    LazyQueryPlan plan6 = new LazyQueryPlan(activeNodes, quarantinedNodes, 200L);
    System.out.print("Seed 200: ");
    printPlan(plan6);
    System.out.println();

    // Test 4: Iterator operations
    System.out.println("--- Test 4: Iterator operations ---");
    LazyQueryPlan plan7 = new LazyQueryPlan(activeNodes, quarantinedNodes, 123L);
    System.out.println("Total size: " + plan7.size());
    System.out.println("Remaining before iteration: " + plan7.remaining());

    System.out.println("Iterating through first 3 nodes:");
    for (int i = 0; i < 3 && plan7.hasNext(); i++) {
      URI node = plan7.next();
      System.out.println("  Node " + (i + 1) + ": " + node.getHost());
    }
    System.out.println("Remaining after 3 iterations: " + plan7.remaining());

    System.out.println("Resetting iterator...");
    plan7.reset();
    System.out.println("Remaining after reset: " + plan7.remaining());
    System.out.println();

    // Test 5: Active nodes appear before quarantined
    System.out.println("--- Test 5: Verify active nodes come before quarantined ---");
    LazyQueryPlan plan8 = new LazyQueryPlan(activeNodes, quarantinedNodes, 999L);
    Set<String> activeHosts = new HashSet<>();
    for (URI uri : activeNodes) {
      activeHosts.add(uri.getHost());
    }

    int activeCount = 0;
    int quarantinedCount = 0;
    boolean foundQuarantined = false;

    while (plan8.hasNext()) {
      URI node = plan8.next();
      if (activeHosts.contains(node.getHost())) {
        activeCount++;
        if (foundQuarantined) {
          System.out.println("ERROR: Found active node after quarantined node!");
        }
      } else {
        quarantinedCount++;
        foundQuarantined = true;
      }
    }

    System.out.println("Active nodes in first part: " + activeCount);
    System.out.println("Quarantined nodes in second part: " + quarantinedCount);
    System.out.println("Order is correct: " + (activeCount == activeNodes.size()));
    System.out.println();

    // Test 6: Empty lists
    System.out.println("--- Test 6: Edge cases ---");
    LazyQueryPlan emptyActive = new LazyQueryPlan(Collections.emptyList(), quarantinedNodes);
    System.out.println("Empty active list size: " + emptyActive.size());

    LazyQueryPlan emptyQuarantined = new LazyQueryPlan(activeNodes, Collections.emptyList());
    System.out.println("Empty quarantined list size: " + emptyQuarantined.size());

    LazyQueryPlan bothEmpty = new LazyQueryPlan(Collections.emptyList(), Collections.emptyList());
    System.out.println("Both empty size: " + bothEmpty.size());
    System.out.println("Both empty hasNext: " + bothEmpty.hasNext());
    System.out.println();

    // Test 7: Exception handling
    System.out.println("--- Test 7: Exception handling ---");
    try {
      LazyQueryPlan nullActive = new LazyQueryPlan(null, quarantinedNodes);
      System.out.println("ERROR: Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      System.out.println(
          "Correctly threw IllegalArgumentException for null activeNodes: " + e.getMessage());
    }

    try {
      LazyQueryPlan exhausted = new LazyQueryPlan(Collections.emptyList(), Collections.emptyList());
      exhausted.next();
      System.out.println("ERROR: Should have thrown NoSuchElementException");
    } catch (NoSuchElementException e) {
      System.out.println(
          "Correctly threw NoSuchElementException when exhausted: " + e.getMessage());
    }
    System.out.println();

    // Test 8: Iterable interface (for-each loop)
    System.out.println("--- Test 8: Iterable interface (for-each loop) ---");
    LazyQueryPlan iterablePlan = new LazyQueryPlan(activeNodes, quarantinedNodes, 42L);
    System.out.print("Using for-each: ");
    List<String> forEachHosts = new ArrayList<>();
    for (URI node : iterablePlan) {
      forEachHosts.add(node.getHost().split("\\.")[0]);
    }
    System.out.println(forEachHosts);
    System.out.println();

    // Test 9: getNodes() method
    System.out.println("--- Test 9: getNodes() returns all nodes ---");
    LazyQueryPlan nodesPlan = new LazyQueryPlan(activeNodes, quarantinedNodes, 42L);
    System.out.println("getNodes() size: " + nodesPlan.getNodes().size());
    System.out.println("First 3 nodes from getNodes(): ");
    for (int i = 0; i < 3; i++) {
      System.out.println("  " + nodesPlan.getNodes().get(i).getHost());
    }
    System.out.println("Remaining (not consumed by getNodes): " + nodesPlan.remaining());

    System.out.println("\n=== All tests completed ===");
  }

  private static List<URI> createUriList(String prefix, int count) throws URISyntaxException {
    List<URI> uris = new ArrayList<>();
    for (int i = 1; i <= count; i++) {
      uris.add(
          new URI("http", null, prefix + "-node" + i + ".example.com", 8000, null, null, null));
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
