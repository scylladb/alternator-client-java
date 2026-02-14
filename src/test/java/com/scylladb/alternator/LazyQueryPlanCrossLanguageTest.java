package com.scylladb.alternator;

import static org.junit.Assert.*;

import com.scylladb.alternator.internal.AlternatorLiveNodes;
import com.scylladb.alternator.internal.LazyQueryPlan;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import org.junit.Test;

/**
 * Cross-language compatibility tests for LazyQueryPlan.
 *
 * <p>These tests verify that LazyQueryPlan produces identical node selection sequences to the Go
 * implementation for the same seed values. This is critical for key route affinity, where all
 * client implementations must route the same partition key to the same coordinator node.
 *
 * <p>Test vectors are generated from the Go implementation using Go's math/rand PRNG with
 * pick-and-remove selection.
 *
 * <p>Node naming convention: active nodes are node1.example.com:8043 through
 * node{N}.example.com:8043, quarantined nodes are quarantined1.example.com:8043 through
 * quarantined{N}.example.com:8043.
 *
 * @author dmitry.kropachev
 * @since 2.0.0
 */
public class LazyQueryPlanCrossLanguageTest {

  private static final int PORT = 8043;
  private static final String SCHEME = "http";

  private static List<URI> createNodes(String prefix, int count) throws URISyntaxException {
    List<URI> nodes = new ArrayList<>();
    for (int i = 1; i <= count; i++) {
      nodes.add(new URI(SCHEME, null, prefix + i + ".example.com", PORT, null, null, null));
    }
    return nodes;
  }

  private static AlternatorLiveNodes createLiveNodes(int activeCount, int quarantinedCount)
      throws URISyntaxException {
    // Only active nodes are in the live nodes list; quarantined nodes are excluded
    List<URI> active = createNodes("node", activeCount);
    return new AlternatorLiveNodes(active, SCHEME, PORT, "", "");
  }

  private static String nodeShortName(URI uri) {
    // Extract "node6" from "node6.example.com"
    String host = uri.getHost();
    return host.substring(0, host.indexOf(".example.com"));
  }

  private static List<String> getSequence(LazyQueryPlan plan, int count) {
    List<String> result = new ArrayList<>();
    for (int i = 0; i < count && plan.hasNext(); i++) {
      result.add(nodeShortName(plan.next()));
    }
    return result;
  }

  @Test
  public void testSeed42_10Nodes() throws URISyntaxException {
    AlternatorLiveNodes liveNodes = createLiveNodes(10, 0);
    LazyQueryPlan plan = new LazyQueryPlan(liveNodes, 42L);
    List<String> actual = getSequence(plan, 6);
    assertEquals(Arrays.asList("node6", "node9", "node5", "node2", "node7", "node1"), actual);
  }

  @Test
  public void testSeed123_10Nodes() throws URISyntaxException {
    AlternatorLiveNodes liveNodes = createLiveNodes(10, 0);
    LazyQueryPlan plan = new LazyQueryPlan(liveNodes, 123L);
    List<String> actual = getSequence(plan, 6);
    assertEquals(Arrays.asList("node6", "node1", "node4", "node3", "node10", "node5"), actual);
  }

  @Test
  public void testSeed999_10Nodes() throws URISyntaxException {
    AlternatorLiveNodes liveNodes = createLiveNodes(10, 0);
    LazyQueryPlan plan = new LazyQueryPlan(liveNodes, 999L);
    List<String> actual = getSequence(plan, 6);
    assertEquals(Arrays.asList("node5", "node10", "node4", "node1", "node2", "node3"), actual);
  }

  @Test
  public void testSeed0_10Nodes() throws URISyntaxException {
    AlternatorLiveNodes liveNodes = createLiveNodes(10, 0);
    LazyQueryPlan plan = new LazyQueryPlan(liveNodes, 0L);
    List<String> actual = getSequence(plan, 6);
    assertEquals(Arrays.asList("node5", "node1", "node2", "node10", "node6", "node8"), actual);
  }

  @Test
  public void testSeedNeg1_10Nodes() throws URISyntaxException {
    AlternatorLiveNodes liveNodes = createLiveNodes(10, 0);
    LazyQueryPlan plan = new LazyQueryPlan(liveNodes, -1L);
    List<String> actual = getSequence(plan, 6);
    assertEquals(Arrays.asList("node2", "node5", "node1", "node3", "node6", "node10"), actual);
  }

  @Test
  public void testSeed42_6ActiveNodes_4Quarantined() throws URISyntaxException {
    AlternatorLiveNodes liveNodes = createLiveNodes(6, 4);
    LazyQueryPlan plan = new LazyQueryPlan(liveNodes, 42L);
    List<String> actual = getSequence(plan, 6);
    assertEquals(Arrays.asList("node6", "node3", "node1", "node4", "node2", "node5"), actual);
  }

  @Test
  public void testSeed12345_10Nodes() throws URISyntaxException {
    AlternatorLiveNodes liveNodes = createLiveNodes(10, 0);
    LazyQueryPlan plan = new LazyQueryPlan(liveNodes, 12345L);
    List<String> actual = getSequence(plan, 6);
    assertEquals(Arrays.asList("node4", "node5", "node1", "node7", "node6", "node8"), actual);
  }

  @Test
  public void testSeedMaxInt64_10Nodes() throws URISyntaxException {
    AlternatorLiveNodes liveNodes = createLiveNodes(10, 0);
    LazyQueryPlan plan = new LazyQueryPlan(liveNodes, Long.MAX_VALUE);
    List<String> actual = getSequence(plan, 6);
    assertEquals(Arrays.asList("node2", "node7", "node8", "node1", "node10", "node4"), actual);
  }
}
