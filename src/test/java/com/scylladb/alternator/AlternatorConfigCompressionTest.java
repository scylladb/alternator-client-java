package com.scylladb.alternator;

import static org.junit.Assert.*;

import org.junit.Test;

/**
 * Unit tests for AlternatorConfig compression configuration.
 *
 * @author dmitry.kropachev
 */
public class AlternatorConfigCompressionTest {

  @Test
  public void testDefaultCompressionSettings() {
    AlternatorConfig config = AlternatorConfig.builder().build();

    assertEquals(RequestCompressionAlgorithm.NONE, config.getCompressionAlgorithm());
    assertEquals(
        AlternatorConfig.DEFAULT_MIN_COMPRESSION_SIZE_BYTES, config.getMinCompressionSizeBytes());
    assertFalse(config.getCompressionAlgorithm().isEnabled());
  }

  @Test
  public void testGzipCompressionEnabled() {
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withCompressionAlgorithm(RequestCompressionAlgorithm.GZIP)
            .build();

    assertEquals(RequestCompressionAlgorithm.GZIP, config.getCompressionAlgorithm());
    assertTrue(config.getCompressionAlgorithm().isEnabled());
  }

  @Test
  public void testCustomMinCompressionSize() {
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withCompressionAlgorithm(RequestCompressionAlgorithm.GZIP)
            .withMinCompressionSizeBytes(2048)
            .build();

    assertEquals(RequestCompressionAlgorithm.GZIP, config.getCompressionAlgorithm());
    assertEquals(2048, config.getMinCompressionSizeBytes());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNegativeMinCompressionSizeThrowsException() {
    AlternatorConfig.builder().withMinCompressionSizeBytes(-1).build();
  }

  @Test
  public void testZeroMinCompressionSizeIsValid() {
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withCompressionAlgorithm(RequestCompressionAlgorithm.GZIP)
            .withMinCompressionSizeBytes(0)
            .build();

    assertEquals(0, config.getMinCompressionSizeBytes());
  }

  @Test
  public void testNullCompressionAlgorithmDefaultsToNone() {
    AlternatorConfig config = AlternatorConfig.builder().withCompressionAlgorithm(null).build();

    assertEquals(RequestCompressionAlgorithm.NONE, config.getCompressionAlgorithm());
    assertFalse(config.getCompressionAlgorithm().isEnabled());
  }

  @Test
  public void testCompressionWithDatacenterAndRack() {
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withDatacenter("us-east")
            .withRack("rack1")
            .withCompressionAlgorithm(RequestCompressionAlgorithm.GZIP)
            .withMinCompressionSizeBytes(512)
            .build();

    assertEquals("us-east", config.getDatacenter());
    assertEquals("rack1", config.getRack());
    assertEquals(RequestCompressionAlgorithm.GZIP, config.getCompressionAlgorithm());
    assertEquals(512, config.getMinCompressionSizeBytes());
  }

  @Test
  public void testBackwardCompatibilityWithoutCompression() {
    // Existing code without compression settings should still work
    AlternatorConfig config =
        AlternatorConfig.builder().withDatacenter("dc1").withRack("rack1").build();

    assertEquals("dc1", config.getDatacenter());
    assertEquals("rack1", config.getRack());
    // Compression defaults to disabled
    assertEquals(RequestCompressionAlgorithm.NONE, config.getCompressionAlgorithm());
    assertEquals(
        AlternatorConfig.DEFAULT_MIN_COMPRESSION_SIZE_BYTES, config.getMinCompressionSizeBytes());
    assertFalse(config.getCompressionAlgorithm().isEnabled());
  }

  @Test
  public void testExplicitlyDisableCompression() {
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withCompressionAlgorithm(RequestCompressionAlgorithm.NONE)
            .build();

    assertEquals(RequestCompressionAlgorithm.NONE, config.getCompressionAlgorithm());
    assertFalse(config.getCompressionAlgorithm().isEnabled());
  }

  @Test
  public void testDefaultMinCompressionSizeValue() {
    // Verify the default constant value is 1024 bytes (1 KB)
    assertEquals(1024, AlternatorConfig.DEFAULT_MIN_COMPRESSION_SIZE_BYTES);
  }
}
