package com.scylladb.alternator;

import static org.junit.Assert.*;

import org.junit.Test;

/**
 * Unit tests for RequestCompressionAlgorithm enum.
 *
 * @author dmitry.kropachev
 */
public class RequestCompressionAlgorithmTest {

  @Test
  public void testNoneIsNotEnabled() {
    assertFalse(RequestCompressionAlgorithm.NONE.isEnabled());
  }

  @Test
  public void testGzipIsEnabled() {
    assertTrue(RequestCompressionAlgorithm.GZIP.isEnabled());
  }

  @Test
  public void testAllValuesExist() {
    // Verify the expected enum values exist
    RequestCompressionAlgorithm none = RequestCompressionAlgorithm.NONE;
    RequestCompressionAlgorithm gzip = RequestCompressionAlgorithm.GZIP;

    assertNotNull(none);
    assertNotNull(gzip);

    // Verify we have exactly 2 values
    assertEquals(2, RequestCompressionAlgorithm.values().length);
  }

  @Test
  public void testValueOf() {
    assertEquals(RequestCompressionAlgorithm.NONE, RequestCompressionAlgorithm.valueOf("NONE"));
    assertEquals(RequestCompressionAlgorithm.GZIP, RequestCompressionAlgorithm.valueOf("GZIP"));
  }

  @Test
  public void testIsEnabledConsistency() {
    // All algorithms except NONE should be enabled
    for (RequestCompressionAlgorithm algorithm : RequestCompressionAlgorithm.values()) {
      if (algorithm == RequestCompressionAlgorithm.NONE) {
        assertFalse(algorithm.name() + " should not be enabled", algorithm.isEnabled());
      } else {
        assertTrue(algorithm.name() + " should be enabled", algorithm.isEnabled());
      }
    }
  }
}
