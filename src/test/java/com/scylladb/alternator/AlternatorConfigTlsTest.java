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

  // Tests for TlsConfig (new CA certificate support)

  @Test
  public void testDefaultTlsConfig() {
    AlternatorConfig config =
        AlternatorConfig.builder().withSeedNode(URI.create("https://localhost:8043")).build();

    TlsConfig tlsConfig = config.getTlsConfig();
    assertNotNull("TlsConfig should not be null", tlsConfig);
    // Default should be trust-all for backwards compatibility
    assertTrue("Default should trust all certificates", tlsConfig.isTrustAllCertificates());
  }

  @Test
  public void testTlsConfigTrustAll() {
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedNode(URI.create("https://localhost:8043"))
            .withTlsConfig(TlsConfig.trustAll())
            .build();

    TlsConfig tlsConfig = config.getTlsConfig();
    assertTrue("Should trust all certificates", tlsConfig.isTrustAllCertificates());
    assertFalse("Should not verify hostname", tlsConfig.isVerifyHostname());
  }

  @Test
  public void testTlsConfigSystemDefault() {
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedNode(URI.create("https://localhost:8043"))
            .withTlsConfig(TlsConfig.systemDefault())
            .build();

    TlsConfig tlsConfig = config.getTlsConfig();
    assertFalse("Should not trust all certificates", tlsConfig.isTrustAllCertificates());
    assertTrue("Should trust system CAs", tlsConfig.isTrustSystemCaCerts());
    assertTrue("Should verify hostname", tlsConfig.isVerifyHostname());
  }

  @Test
  public void testTlsConfigWithSessionCache() {
    TlsSessionCacheConfig sessionConfig =
        TlsSessionCacheConfig.builder()
            .withSessionCacheSize(256)
            .withSessionTimeoutSeconds(1800)
            .build();

    TlsConfig tlsConfig =
        TlsConfig.builder()
            .withTrustAllCertificates(true)
            .withSessionCacheConfig(sessionConfig)
            .build();

    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedNode(URI.create("https://localhost:8043"))
            .withTlsConfig(tlsConfig)
            .build();

    assertEquals(sessionConfig, config.getTlsConfig().getSessionCacheConfig());
    assertEquals(256, config.getTlsConfig().getSessionCacheConfig().getSessionCacheSize());
  }

  @Test
  public void testTlsConfigNullUsesDefault() {
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedNode(URI.create("https://localhost:8043"))
            .withTlsConfig(null)
            .build();

    TlsConfig tlsConfig = config.getTlsConfig();
    assertNotNull("TlsConfig should not be null even when set to null", tlsConfig);
    // Default should be trust-all for backwards compatibility
    assertTrue("Default should trust all certificates", tlsConfig.isTrustAllCertificates());
  }

  @Test
  public void testLegacyTlsSessionCacheConfigCreatesTlsConfig() {
    // When only TlsSessionCacheConfig is set (legacy way), TlsConfig should be created
    TlsSessionCacheConfig sessionConfig =
        TlsSessionCacheConfig.builder()
            .withSessionCacheSize(512)
            .withSessionTimeoutSeconds(3600)
            .build();

    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedNode(URI.create("https://localhost:8043"))
            .withTlsSessionCacheConfig(sessionConfig)
            .build();

    TlsConfig tlsConfig = config.getTlsConfig();
    assertNotNull("TlsConfig should be created from TlsSessionCacheConfig", tlsConfig);
    assertTrue(
        "Should trust all certificates for backwards compatibility",
        tlsConfig.isTrustAllCertificates());
    assertEquals(512, tlsConfig.getSessionCacheConfig().getSessionCacheSize());
  }

  @Test
  public void testTlsConfigOverridesTlsSessionCacheConfig() {
    // When both are set, TlsConfig should take precedence
    TlsSessionCacheConfig legacyConfig =
        TlsSessionCacheConfig.builder().withSessionCacheSize(100).build();

    TlsConfig tlsConfig =
        TlsConfig.builder()
            .withTrustSystemCaCerts(true)
            .withSessionCacheConfig(
                TlsSessionCacheConfig.builder().withSessionCacheSize(200).build())
            .build();

    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedNode(URI.create("https://localhost:8043"))
            .withTlsSessionCacheConfig(legacyConfig)
            .withTlsConfig(tlsConfig)
            .build();

    // TlsConfig should be used
    assertEquals(200, config.getTlsConfig().getSessionCacheConfig().getSessionCacheSize());
  }
}
