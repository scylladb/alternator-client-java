package com.scylladb.alternator;

import static org.junit.Assert.*;

import com.scylladb.alternator.internal.AlternatorLiveNodes;
import com.scylladb.alternator.internal.LazyQueryPlan;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import org.junit.Test;

/**
 * Unit tests for AlternatorLiveNodes functionality that doesn't require a live server.
 *
 * @author dmitry.kropachev
 */
public class AlternatorLiveNodesTest {

  @Test
  public void testConstructorWithSingleNode() throws URISyntaxException {
    URI node = new URI("http://localhost:8000");
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(node, "", "");
    assertNotNull(liveNodes);
  }

  @Test
  public void testConstructorWithMultipleNodes() throws URISyntaxException {
    List<URI> nodes =
        Arrays.asList(
            new URI("http://node1.example.com:8000"),
            new URI("http://node2.example.com:8000"),
            new URI("http://node3.example.com:8000"));
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(nodes, "http", 8000, "", "");
    assertNotNull(liveNodes);
  }

  @Test(expected = RuntimeException.class)
  public void testConstructorWithNullNodes() {
    new AlternatorLiveNodes(null, "http", 8000, "", "");
  }

  @Test(expected = RuntimeException.class)
  public void testConstructorWithEmptyNodes() {
    new AlternatorLiveNodes(Collections.<URI>emptyList(), "http", 8000, "", "");
  }

  @Test
  public void testNextAsURIRoundRobin() throws URISyntaxException {
    List<URI> nodes =
        Arrays.asList(
            new URI("http://node1.example.com:8000"),
            new URI("http://node2.example.com:8000"),
            new URI("http://node3.example.com:8000"));
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(nodes, "http", 8000, "", "");

    Set<URI> returnedNodes = new HashSet<>();
    for (int i = 0; i < 6; i++) {
      returnedNodes.add(liveNodes.nextAsURI());
    }

    assertEquals("Round-robin should return all nodes", 3, returnedNodes.size());
  }

  @Test
  public void testNextAsURIWithPathAndQuery() throws URISyntaxException {
    URI node = new URI("http://localhost:8000");
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(node, "", "");

    URI result = liveNodes.nextAsURI("/test/path", "key=value");

    assertEquals("/test/path", result.getPath());
    assertEquals("key=value", result.getQuery());
    assertEquals("localhost", result.getHost());
    assertEquals(8000, result.getPort());
  }

  @Test
  public void testGetLiveNodes() throws URISyntaxException {
    List<URI> nodes =
        Arrays.asList(
            new URI("http://node1.example.com:8000"), new URI("http://node2.example.com:8000"));
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(nodes, "http", 8000, "", "");

    List<URI> result = liveNodes.getLiveNodes();

    assertEquals(2, result.size());
    assertTrue(result.containsAll(nodes));
  }

  @Test
  public void testGetLiveNodesReturnsUnmodifiableList() throws URISyntaxException {
    URI node = new URI("http://localhost:8000");
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(node, "", "");

    List<URI> result = liveNodes.getLiveNodes();

    try {
      result.add(new URI("http://newnode:8000"));
      fail("getLiveNodes() should return unmodifiable list");
    } catch (UnsupportedOperationException e) {
      // Expected
    }
  }

  @Test
  public void testNewQueryPlan() throws URISyntaxException {
    List<URI> nodes =
        Arrays.asList(
            new URI("http://node1.example.com:8000"),
            new URI("http://node2.example.com:8000"),
            new URI("http://node3.example.com:8000"));
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(nodes, "http", 8000, "", "");

    LazyQueryPlan plan = new LazyQueryPlan(liveNodes);

    assertNotNull(plan);
    assertTrue(plan.hasNext());

    Set<URI> returnedNodes = new HashSet<>();
    while (plan.hasNext()) {
      returnedNodes.add(plan.next());
    }

    assertEquals(new HashSet<>(nodes), returnedNodes);
  }

  @Test
  public void testNewQueryPlanWithSeed() throws URISyntaxException {
    List<URI> nodes =
        Arrays.asList(
            new URI("http://node1.example.com:8000"),
            new URI("http://node2.example.com:8000"),
            new URI("http://node3.example.com:8000"));
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(nodes, "http", 8000, "", "");

    long seed = 42L;
    LazyQueryPlan plan1 = new LazyQueryPlan(liveNodes, seed);
    LazyQueryPlan plan2 = new LazyQueryPlan(liveNodes, seed);

    // Same seed should produce same first node
    URI first1 = plan1.next();
    URI first2 = plan2.next();

    assertEquals("Same seed should produce same first node", first1, first2);
  }

  @Test
  public void testValidateURI() throws Exception {
    URI validUri = new URI("http://localhost:8000");
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(validUri, "", "");

    // Should not throw
    liveNodes.validateURI(validUri);
  }

  @Test
  public void testValidateURIWithInvalidThrowsException() throws Exception {
    URI validUri = new URI("http://localhost:8000");
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(validUri, "", "");

    // Create a URI that is valid as a URI but not as a URL (relative URI)
    URI invalidUri = new URI(null, null, "/relative/path", null);

    // The method should throw some exception for invalid URIs
    // The actual exception type depends on the kind of invalid URI
    try {
      liveNodes.validateURI(invalidUri);
      fail("Should throw an exception for invalid URI");
    } catch (AlternatorLiveNodes.ValidationError e) {
      // Expected for MalformedURLException
    } catch (IllegalArgumentException e) {
      // Also acceptable - thrown for relative URIs
    }
  }

  @Test
  public void testHttpsScheme() throws URISyntaxException {
    URI node = new URI("https://localhost:8043");
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(node, "", "");

    URI result = liveNodes.nextAsURI();

    assertEquals("https", result.getScheme());
    assertEquals(8043, result.getPort());
  }
}
