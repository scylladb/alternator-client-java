package com.scylladb.alternator;

import static org.junit.Assert.*;

import com.scylladb.alternator.routing.ClusterScope;
import com.scylladb.alternator.routing.DatacenterScope;
import com.scylladb.alternator.routing.RackScope;
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
  public void testCompressionWithRoutingScope() {
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withRoutingScope(
                RackScope.of(
                    "us-east", "rack1", DatacenterScope.of("us-east", ClusterScope.create())))
            .withCompressionAlgorithm(RequestCompressionAlgorithm.GZIP)
            .withMinCompressionSizeBytes(512)
            .build();

    assertEquals("Rack", config.getRoutingScope().getName());
    assertEquals(RequestCompressionAlgorithm.GZIP, config.getCompressionAlgorithm());
    assertEquals(512, config.getMinCompressionSizeBytes());
  }

  @Test
  public void testDefaultWithoutCompression() {
    // Existing code without compression settings should still work
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withRoutingScope(
                RackScope.of("dc1", "rack1", DatacenterScope.of("dc1", ClusterScope.create())))
            .build();

    assertEquals("Rack", config.getRoutingScope().getName());
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
