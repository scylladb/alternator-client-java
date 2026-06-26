package com.scylladb.alternator.internal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.scylladb.alternator.AlternatorConfig;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Before;
import org.junit.Test;

/**
 * Regression test for the {@code Math.floorMod} fix in {@link AlternatorLiveNodes#nextAsURI()}.
 * {@code Math.abs(Integer.MIN_VALUE) == Integer.MIN_VALUE} (still negative), so the old {@code
 * Math.abs(index % size)} implementation threw {@link IndexOutOfBoundsException} once every
 * ~2<sup>31</sup> calls when the counter wrapped.
 */
public class NextAsURIOverflowTest {

  private static final int NODE_COUNT = 3;
  private AlternatorLiveNodes liveNodes;

  @Before
  public void setUp() {
    liveNodes =
        new AlternatorLiveNodes(
            AlternatorConfig.builder()
                .withSeedHosts(Arrays.asList("10.0.0.1", "10.0.0.2", "10.0.0.3"))
                .withScheme("http")
                .withPort(8000)
                .build());
  }

  @Test
  public void nextAsURICoversAllNodesAcrossCounterWrapAround() throws Exception {
    Field field = AlternatorLiveNodes.class.getDeclaredField("nextLiveNodeIndex");
    field.setAccessible(true);
    // Start one before MAX_VALUE so the first three calls hit MAX_VALUE-1, MAX_VALUE, MIN_VALUE.
    ((AtomicInteger) field.get(liveNodes)).set(Integer.MAX_VALUE - 1);

    Set<URI> seen = new HashSet<>();
    for (int i = 0; i < NODE_COUNT * 4; i++) {
      URI uri = liveNodes.nextAsURI();
      assertTrue("URI must be from the live list", liveNodes.getLiveNodes().contains(uri));
      seen.add(uri);
    }
    assertEquals("all nodes must be reachable after wrap-around", NODE_COUNT, seen.size());
  }
}
