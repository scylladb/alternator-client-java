package com.scylladb.alternator;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.junit.Test;

/**
 * Unit tests for AlternatorConfig headers optimization configuration.
 *
 * @author dmitry.kropachev
 */
public class AlternatorConfigHeadersTest {

  @Test
  public void testDefaultHeadersOptimizationSettings() {
    AlternatorConfig config = AlternatorConfig.builder().build();

    assertFalse(config.isOptimizeHeaders());
    assertEquals(AlternatorConfig.DEFAULT_HEADERS_WHITELIST, config.getHeadersWhitelist());
  }

  @Test
  public void testHeadersOptimizationEnabled() {
    AlternatorConfig config = AlternatorConfig.builder().withOptimizeHeaders(true).build();

    assertTrue(config.isOptimizeHeaders());
    assertEquals(AlternatorConfig.DEFAULT_HEADERS_WHITELIST, config.getHeadersWhitelist());
  }

  @Test
  public void testCustomHeadersWhitelist() {
    Set<String> customHeaders = new HashSet<>(Arrays.asList("Host", "Authorization", "X-Custom"));

    AlternatorConfig config =
        AlternatorConfig.builder()
            .withOptimizeHeaders(true)
            .withHeadersWhitelist(customHeaders)
            .build();

    assertTrue(config.isOptimizeHeaders());
    assertEquals(customHeaders, config.getHeadersWhitelist());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNullHeadersWhitelistThrowsException() {
    AlternatorConfig.builder().withHeadersWhitelist(null).build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyHeadersWhitelistThrowsException() {
    AlternatorConfig.builder().withHeadersWhitelist(Arrays.asList()).build();
  }

  @Test
  public void testHeadersWhitelistIsImmutable() {
    Set<String> customHeaders = new HashSet<>(Arrays.asList("Host", "Authorization"));

    AlternatorConfig config =
        AlternatorConfig.builder()
            .withOptimizeHeaders(true)
            .withHeadersWhitelist(customHeaders)
            .build();

    // Modifying the original set should not affect the config
    customHeaders.add("X-Modified");
    assertFalse(config.getHeadersWhitelist().contains("X-Modified"));
  }

  @Test
  public void testHeadersOptimizationWithDatacenterAndRack() {
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withDatacenter("us-east")
            .withRack("rack1")
            .withOptimizeHeaders(true)
            .build();

    assertEquals("us-east", config.getDatacenter());
    assertEquals("rack1", config.getRack());
    assertTrue(config.isOptimizeHeaders());
    assertEquals(AlternatorConfig.DEFAULT_HEADERS_WHITELIST, config.getHeadersWhitelist());
  }

  @Test
  public void testHeadersOptimizationWithCompression() {
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withCompressionAlgorithm(RequestCompressionAlgorithm.GZIP)
            .withOptimizeHeaders(true)
            .build();

    assertEquals(RequestCompressionAlgorithm.GZIP, config.getCompressionAlgorithm());
    assertTrue(config.isOptimizeHeaders());
  }

  @Test
  public void testBackwardCompatibilityWithoutHeadersOptimization() {
    // Existing code without headers settings should still work
    AlternatorConfig config =
        AlternatorConfig.builder().withDatacenter("dc1").withRack("rack1").build();

    assertEquals("dc1", config.getDatacenter());
    assertEquals("rack1", config.getRack());
    // Headers optimization defaults to disabled
    assertFalse(config.isOptimizeHeaders());
    assertEquals(AlternatorConfig.DEFAULT_HEADERS_WHITELIST, config.getHeadersWhitelist());
  }

  @Test
  public void testExplicitlyDisableHeadersOptimization() {
    AlternatorConfig config = AlternatorConfig.builder().withOptimizeHeaders(false).build();

    assertFalse(config.isOptimizeHeaders());
  }

  @Test
  public void testDefaultHeadersWhitelistContents() {
    // Verify the default whitelist contains expected headers
    Set<String> defaultHeaders = AlternatorConfig.DEFAULT_HEADERS_WHITELIST;

    assertTrue(defaultHeaders.contains("Host"));
    assertTrue(defaultHeaders.contains("X-Amz-Target"));
    assertTrue(defaultHeaders.contains("Content-Type"));
    assertTrue(defaultHeaders.contains("Content-Length"));
    assertTrue(defaultHeaders.contains("Accept-Encoding"));
    assertTrue(defaultHeaders.contains("Content-Encoding"));
    assertTrue(defaultHeaders.contains("Authorization"));
    assertTrue(defaultHeaders.contains("X-Amz-Date"));
    assertTrue(defaultHeaders.contains("X-Amz-Content-Sha256"));
    assertEquals(9, defaultHeaders.size());
  }

  @Test
  public void testDefaultHeadersWhitelistIsImmutable() {
    try {
      AlternatorConfig.DEFAULT_HEADERS_WHITELIST.add("X-Should-Fail");
      fail("DEFAULT_HEADERS_WHITELIST should be immutable");
    } catch (UnsupportedOperationException e) {
      // Expected
    }
  }
}
