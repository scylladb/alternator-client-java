package com.scylladb.alternator.internal;

import static org.junit.Assert.*;

import org.junit.Test;

/**
 * Unit tests for {@link AsyncClientDetector}.
 *
 * <p>Verifies classpath detection for async HTTP client implementations.
 */
public class AsyncClientDetectorTest {

  @Test
  public void testDetectFindsImplementation() {
    // Both Netty and CRT are on the test classpath, so detection should succeed
    AsyncClientDetector.AsyncClientType type = AsyncClientDetector.detect();
    assertNotNull("Should detect an async client type", type);
  }

  @Test
  public void testDetectPrefersNettyOverCrt() {
    // Both are on classpath; Netty should win by priority
    AsyncClientDetector.AsyncClientType type = AsyncClientDetector.detect();
    assertEquals(
        "Netty should be preferred when both are available",
        AsyncClientDetector.AsyncClientType.NETTY,
        type);
  }

  @Test
  public void testAsyncClientTypeValues() {
    AsyncClientDetector.AsyncClientType[] values = AsyncClientDetector.AsyncClientType.values();
    assertEquals("Should have exactly 2 async client types", 2, values.length);
    assertEquals(AsyncClientDetector.AsyncClientType.NETTY, values[0]);
    assertEquals(AsyncClientDetector.AsyncClientType.CRT, values[1]);
  }
}
