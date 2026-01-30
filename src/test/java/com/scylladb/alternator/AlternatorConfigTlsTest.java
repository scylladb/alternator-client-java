package com.scylladb.alternator;

import static org.junit.Assert.*;

import java.net.URI;
import org.junit.Test;

/**
 * Unit tests for TLS session cache configuration in AlternatorConfig.
 *
 * @author dmitry.kropachev
 */
public class AlternatorConfigTlsTest {

  @Test
  public void testDefaultTlsSessionCacheConfig() {
    AlternatorConfig config =
        AlternatorConfig.builder().withSeedNode(URI.create("https://localhost:8043")).build();

    TlsSessionCacheConfig tlsConfig = config.getTlsSessionCacheConfig();
    assertNotNull("TLS session cache config should not be null", tlsConfig);
    assertTrue("TLS session cache should be enabled by default", tlsConfig.isEnabled());
  }

  @Test
  public void testCustomTlsSessionCacheConfig() {
    TlsSessionCacheConfig customTlsConfig =
        TlsSessionCacheConfig.builder()
            .withEnabled(true)
            .withSessionCacheSize(500)
            .withSessionTimeoutSeconds(7200)
            .build();

    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedNode(URI.create("https://localhost:8043"))
            .withTlsSessionCacheConfig(customTlsConfig)
            .build();

    TlsSessionCacheConfig tlsConfig = config.getTlsSessionCacheConfig();
    assertEquals(customTlsConfig, tlsConfig);
    assertEquals(500, tlsConfig.getSessionCacheSize());
    assertEquals(7200, tlsConfig.getSessionTimeoutSeconds());
  }

  @Test
  public void testDisabledTlsSessionCache() {
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedNode(URI.create("https://localhost:8043"))
            .withTlsSessionCacheConfig(TlsSessionCacheConfig.disabled())
            .build();

    TlsSessionCacheConfig tlsConfig = config.getTlsSessionCacheConfig();
    assertFalse("TLS session cache should be disabled", tlsConfig.isEnabled());
  }

  @Test
  public void testHttpSchemeShouldStillHaveTlsConfig() {
    // Even for HTTP, the TLS config should be present (though not used)
    AlternatorConfig config =
        AlternatorConfig.builder().withSeedNode(URI.create("http://localhost:8000")).build();

    TlsSessionCacheConfig tlsConfig = config.getTlsSessionCacheConfig();
    assertNotNull("TLS config should be present even for HTTP", tlsConfig);
  }

  @Test
  public void testTlsSessionCacheWithCompression() {
    // TLS session cache should work alongside compression
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedNode(URI.create("https://localhost:8043"))
            .withCompressionAlgorithm(RequestCompressionAlgorithm.GZIP)
            .withTlsSessionCacheConfig(TlsSessionCacheConfig.getDefault())
            .build();

    assertTrue(config.getTlsSessionCacheConfig().isEnabled());
    assertEquals(RequestCompressionAlgorithm.GZIP, config.getCompressionAlgorithm());
  }

  @Test
  public void testNullTlsConfigUsesDefault() {
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedNode(URI.create("https://localhost:8043"))
            .withTlsSessionCacheConfig(null)
            .build();

    // Should use default when null is passed
    TlsSessionCacheConfig tlsConfig = config.getTlsSessionCacheConfig();
    assertNotNull(tlsConfig);
    assertTrue(tlsConfig.isEnabled());
  }
}
