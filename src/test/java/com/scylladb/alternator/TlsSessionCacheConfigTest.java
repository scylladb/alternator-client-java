package com.scylladb.alternator;

import static org.junit.Assert.*;

import org.junit.Test;

/**
 * Unit tests for TlsSessionCacheConfig.
 *
 * @author dmitry.kropachev
 */
public class TlsSessionCacheConfigTest {

  @Test
  public void testDefaultValues() {
    TlsSessionCacheConfig config = TlsSessionCacheConfig.getDefault();

    assertTrue("TLS session cache should be enabled by default", config.isEnabled());
    assertEquals("Default session cache size should be 1024", 1024, config.getSessionCacheSize());
    assertEquals(
        "Default session timeout should be 86400 seconds (24 hours)",
        86400,
        config.getSessionTimeoutSeconds());
  }

  @Test
  public void testDisabled() {
    TlsSessionCacheConfig config = TlsSessionCacheConfig.disabled();

    assertFalse("TLS session cache should be disabled", config.isEnabled());
  }

  @Test
  public void testBuilderWithCustomValues() {
    TlsSessionCacheConfig config =
        TlsSessionCacheConfig.builder()
            .withEnabled(true)
            .withSessionCacheSize(200)
            .withSessionTimeoutSeconds(3600)
            .build();

    assertTrue(config.isEnabled());
    assertEquals(200, config.getSessionCacheSize());
    assertEquals(3600, config.getSessionTimeoutSeconds());
  }

  @Test
  public void testBuilderWithDisabled() {
    TlsSessionCacheConfig config = TlsSessionCacheConfig.builder().withEnabled(false).build();

    assertFalse(config.isEnabled());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuilderRejectsNegativeCacheSize() {
    TlsSessionCacheConfig.builder().withSessionCacheSize(-1).build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuilderRejectsZeroCacheSize() {
    TlsSessionCacheConfig.builder().withSessionCacheSize(0).build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuilderRejectsNegativeTimeout() {
    TlsSessionCacheConfig.builder().withSessionTimeoutSeconds(-1).build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuilderRejectsZeroTimeout() {
    TlsSessionCacheConfig.builder().withSessionTimeoutSeconds(0).build();
  }

  @Test
  public void testDefaultConfigIsImmutable() {
    TlsSessionCacheConfig config1 = TlsSessionCacheConfig.getDefault();
    TlsSessionCacheConfig config2 = TlsSessionCacheConfig.getDefault();

    // Should return the same instance
    assertSame(config1, config2);
  }

  @Test
  public void testDisabledConfigIsImmutable() {
    TlsSessionCacheConfig config1 = TlsSessionCacheConfig.disabled();
    TlsSessionCacheConfig config2 = TlsSessionCacheConfig.disabled();

    // Should return the same instance
    assertSame(config1, config2);
  }

  @Test
  public void testToString() {
    TlsSessionCacheConfig config =
        TlsSessionCacheConfig.builder()
            .withEnabled(true)
            .withSessionCacheSize(50)
            .withSessionTimeoutSeconds(1800)
            .build();

    String str = config.toString();
    assertTrue(str.contains("enabled=true"));
    assertTrue(str.contains("sessionCacheSize=50"));
    assertTrue(str.contains("sessionTimeoutSeconds=1800"));
  }

  @Test
  public void testEqualsAndHashCode() {
    TlsSessionCacheConfig config1 =
        TlsSessionCacheConfig.builder()
            .withEnabled(true)
            .withSessionCacheSize(100)
            .withSessionTimeoutSeconds(3600)
            .build();

    TlsSessionCacheConfig config2 =
        TlsSessionCacheConfig.builder()
            .withEnabled(true)
            .withSessionCacheSize(100)
            .withSessionTimeoutSeconds(3600)
            .build();

    TlsSessionCacheConfig config3 =
        TlsSessionCacheConfig.builder()
            .withEnabled(true)
            .withSessionCacheSize(200)
            .withSessionTimeoutSeconds(3600)
            .build();

    assertEquals(config1, config2);
    assertEquals(config1.hashCode(), config2.hashCode());
    assertNotEquals(config1, config3);
  }
}
