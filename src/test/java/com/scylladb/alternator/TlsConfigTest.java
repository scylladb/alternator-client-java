package com.scylladb.alternator;

import static org.junit.Assert.*;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

/**
 * Unit tests for TlsConfig.
 *
 * @author dmitry.kropachev
 */
public class TlsConfigTest {

  @Test
  public void testTrustAllFactoryMethod() {
    TlsConfig config = TlsConfig.trustAll();

    assertTrue("trustAllCertificates should be true", config.isTrustAllCertificates());
    assertFalse("trustSystemCaCerts should be false", config.isTrustSystemCaCerts());
    assertFalse("verifyHostname should be false", config.isVerifyHostname());
    assertTrue("customCaCertPaths should be empty", config.getCustomCaCertPaths().isEmpty());
    assertNotNull("sessionCacheConfig should not be null", config.getSessionCacheConfig());
  }

  @Test
  public void testSystemDefaultFactoryMethod() {
    TlsConfig config = TlsConfig.systemDefault();

    assertFalse("trustAllCertificates should be false", config.isTrustAllCertificates());
    assertTrue("trustSystemCaCerts should be true", config.isTrustSystemCaCerts());
    assertTrue("verifyHostname should be true", config.isVerifyHostname());
    assertTrue("customCaCertPaths should be empty", config.getCustomCaCertPaths().isEmpty());
    assertNotNull("sessionCacheConfig should not be null", config.getSessionCacheConfig());
  }

  @Test
  public void testTrustAllIsSingletonInstance() {
    TlsConfig config1 = TlsConfig.trustAll();
    TlsConfig config2 = TlsConfig.trustAll();

    assertSame("trustAll() should return the same instance", config1, config2);
  }

  @Test
  public void testSystemDefaultIsSingletonInstance() {
    TlsConfig config1 = TlsConfig.systemDefault();
    TlsConfig config2 = TlsConfig.systemDefault();

    assertSame("systemDefault() should return the same instance", config1, config2);
  }

  @Test
  public void testBuilderWithCustomCaCertPath() {
    Path certPath = Paths.get("/path/to/ca.pem");
    TlsConfig config =
        TlsConfig.builder().withCaCertPath(certPath).withTrustSystemCaCerts(false).build();

    assertFalse("trustAllCertificates should be false", config.isTrustAllCertificates());
    assertFalse("trustSystemCaCerts should be false", config.isTrustSystemCaCerts());
    assertTrue("verifyHostname should be true", config.isVerifyHostname());
    assertEquals("Should have one CA cert path", 1, config.getCustomCaCertPaths().size());
    assertEquals("CA cert path should match", certPath, config.getCustomCaCertPaths().get(0));
  }

  @Test
  public void testBuilderWithMultipleCaCertPaths() {
    List<Path> certPaths =
        Arrays.asList(Paths.get("/path/to/ca1.pem"), Paths.get("/path/to/ca2.pem"));
    TlsConfig config =
        TlsConfig.builder().withCaCertPaths(certPaths).withTrustSystemCaCerts(false).build();

    assertEquals("Should have two CA cert paths", 2, config.getCustomCaCertPaths().size());
  }

  @Test
  public void testBuilderWithTrustAllCertificates() {
    TlsConfig config = TlsConfig.builder().withTrustAllCertificates(true).build();

    assertTrue("trustAllCertificates should be true", config.isTrustAllCertificates());
    // When trust-all is enabled, hostname verification is automatically disabled
    assertFalse(
        "verifyHostname should be false when trust-all is enabled", config.isVerifyHostname());
  }

  @Test
  public void testBuilderWithSystemCasAndCustomCas() {
    Path certPath = Paths.get("/path/to/ca.pem");
    TlsConfig config =
        TlsConfig.builder().withCaCertPath(certPath).withTrustSystemCaCerts(true).build();

    assertFalse("trustAllCertificates should be false", config.isTrustAllCertificates());
    assertTrue("trustSystemCaCerts should be true", config.isTrustSystemCaCerts());
    assertEquals("Should have one CA cert path", 1, config.getCustomCaCertPaths().size());
  }

  @Test
  public void testBuilderWithSessionCacheConfig() {
    TlsSessionCacheConfig sessionConfig =
        TlsSessionCacheConfig.builder()
            .withSessionCacheSize(200)
            .withSessionTimeoutSeconds(3600)
            .build();

    TlsConfig config =
        TlsConfig.builder()
            .withTrustAllCertificates(true)
            .withSessionCacheConfig(sessionConfig)
            .build();

    assertEquals("sessionCacheConfig should match", sessionConfig, config.getSessionCacheConfig());
  }

  @Test
  public void testBuilderWithVerifyHostnameDisabled() {
    TlsConfig config =
        TlsConfig.builder().withTrustSystemCaCerts(true).withVerifyHostname(false).build();

    assertFalse("verifyHostname should be false", config.isVerifyHostname());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuilderRejectsNullCaCertPath() {
    TlsConfig.builder().withCaCertPath(null).build();
  }

  @Test(expected = IllegalStateException.class)
  public void testBuilderRejectsNoTrustSource() {
    // When trustSystemCaCerts is false, no custom CAs, and trustAll is false
    TlsConfig.builder().withTrustSystemCaCerts(false).withTrustAllCertificates(false).build();
  }

  @Test
  public void testBuilderDefaultsToSystemCas() {
    // By default, system CAs should be trusted
    TlsConfig config = TlsConfig.builder().withCaCertPath(Paths.get("/path/to/ca.pem")).build();

    assertTrue("trustSystemCaCerts should be true by default", config.isTrustSystemCaCerts());
  }

  @Test
  public void testBuilderDefaultsToHostnameVerification() {
    TlsConfig config = TlsConfig.builder().withTrustSystemCaCerts(true).build();

    assertTrue("verifyHostname should be true by default", config.isVerifyHostname());
  }

  @Test
  public void testCustomCaCertPathsAreImmutable() {
    Path certPath = Paths.get("/path/to/ca.pem");
    TlsConfig config =
        TlsConfig.builder().withCaCertPath(certPath).withTrustSystemCaCerts(false).build();

    List<Path> paths = config.getCustomCaCertPaths();
    try {
      paths.add(Paths.get("/another/path.pem"));
      fail("Should not be able to modify the CA cert paths list");
    } catch (UnsupportedOperationException e) {
      // Expected
    }
  }

  @Test
  public void testToString() {
    TlsConfig config =
        TlsConfig.builder()
            .withTrustSystemCaCerts(true)
            .withCaCertPath(Paths.get("/path/to/ca.pem"))
            .build();

    String str = config.toString();
    assertTrue("toString should contain customCaCertPaths", str.contains("customCaCertPaths"));
    assertTrue("toString should contain trustSystemCaCerts", str.contains("trustSystemCaCerts"));
    assertTrue(
        "toString should contain trustAllCertificates", str.contains("trustAllCertificates"));
    assertTrue("toString should contain verifyHostname", str.contains("verifyHostname"));
    assertTrue("toString should contain sessionCacheConfig", str.contains("sessionCacheConfig"));
  }

  @Test
  public void testEqualsAndHashCode() {
    Path certPath = Paths.get("/path/to/ca.pem");

    TlsConfig config1 =
        TlsConfig.builder().withCaCertPath(certPath).withTrustSystemCaCerts(false).build();

    TlsConfig config2 =
        TlsConfig.builder().withCaCertPath(certPath).withTrustSystemCaCerts(false).build();

    TlsConfig config3 =
        TlsConfig.builder().withCaCertPath(certPath).withTrustSystemCaCerts(true).build();

    assertEquals(config1, config2);
    assertEquals(config1.hashCode(), config2.hashCode());
    assertNotEquals(config1, config3);
  }

  @Test
  public void testEqualsWithNull() {
    TlsConfig config = TlsConfig.trustAll();
    assertNotEquals(config, null);
  }

  @Test
  public void testEqualsWithDifferentType() {
    TlsConfig config = TlsConfig.trustAll();
    assertNotEquals(config, "string");
  }

  @Test
  public void testEqualsSameInstance() {
    TlsConfig config = TlsConfig.trustAll();
    assertEquals(config, config);
  }
}
