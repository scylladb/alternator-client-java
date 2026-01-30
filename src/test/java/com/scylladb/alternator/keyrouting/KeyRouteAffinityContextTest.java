package com.scylladb.alternator.keyrouting;

import static org.junit.Assert.*;

import java.net.URI;
import java.net.URISyntaxException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for KeyRouteAffinityContext.
 *
 * @author dmitry.kropachev
 */
public class KeyRouteAffinityContextTest {

  @Before
  public void setUp() {
    KeyRouteAffinityContext.clear();
  }

  @After
  public void tearDown() {
    KeyRouteAffinityContext.clear();
  }

  @Test
  public void testInitiallyEmpty() {
    assertNull(KeyRouteAffinityContext.getTargetNode());
    assertFalse(KeyRouteAffinityContext.hasTargetNode());
  }

  @Test
  public void testSetAndGetTargetNode() throws URISyntaxException {
    URI uri = new URI("http://node1.example.com:8000");
    KeyRouteAffinityContext.setTargetNode(uri);

    assertEquals(uri, KeyRouteAffinityContext.getTargetNode());
    assertTrue(KeyRouteAffinityContext.hasTargetNode());
  }

  @Test
  public void testClear() throws URISyntaxException {
    URI uri = new URI("http://node1.example.com:8000");
    KeyRouteAffinityContext.setTargetNode(uri);
    assertTrue(KeyRouteAffinityContext.hasTargetNode());

    KeyRouteAffinityContext.clear();

    assertNull(KeyRouteAffinityContext.getTargetNode());
    assertFalse(KeyRouteAffinityContext.hasTargetNode());
  }

  @Test
  public void testSetNullClears() throws URISyntaxException {
    URI uri = new URI("http://node1.example.com:8000");
    KeyRouteAffinityContext.setTargetNode(uri);
    assertTrue(KeyRouteAffinityContext.hasTargetNode());

    KeyRouteAffinityContext.setTargetNode(null);

    assertNull(KeyRouteAffinityContext.getTargetNode());
    assertFalse(KeyRouteAffinityContext.hasTargetNode());
  }

  @Test
  public void testOverwriteTargetNode() throws URISyntaxException {
    URI uri1 = new URI("http://node1.example.com:8000");
    URI uri2 = new URI("http://node2.example.com:8000");

    KeyRouteAffinityContext.setTargetNode(uri1);
    assertEquals(uri1, KeyRouteAffinityContext.getTargetNode());

    KeyRouteAffinityContext.setTargetNode(uri2);
    assertEquals(uri2, KeyRouteAffinityContext.getTargetNode());
  }

  @Test
  public void testThreadIsolation() throws URISyntaxException, InterruptedException {
    final URI uri1 = new URI("http://node1.example.com:8000");
    final URI uri2 = new URI("http://node2.example.com:8000");
    final URI[] threadResult = new URI[1];

    // Set in main thread
    KeyRouteAffinityContext.setTargetNode(uri1);

    // Create a new thread and set a different value
    Thread thread =
        new Thread(
            () -> {
              KeyRouteAffinityContext.setTargetNode(uri2);
              threadResult[0] = KeyRouteAffinityContext.getTargetNode();
              KeyRouteAffinityContext.clear();
            });
    thread.start();
    thread.join();

    // Main thread should still have its value
    assertEquals(uri1, KeyRouteAffinityContext.getTargetNode());
    // Other thread should have had its own value
    assertEquals(uri2, threadResult[0]);
  }
}
