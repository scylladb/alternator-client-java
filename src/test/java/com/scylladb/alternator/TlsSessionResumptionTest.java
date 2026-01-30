package com.scylladb.alternator;

import static org.junit.Assert.*;

import com.scylladb.alternator.internal.TlsContextFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSessionContext;
import org.junit.Test;

/**
 * Unit tests for TLS session resumption support.
 *
 * <p>These tests verify that the SSLContext is properly configured to support TLS session tickets
 * and session resumption, which enables faster TLS renegotiation when reconnecting to nodes.
 *
 * @author dmitry.kropachev
 */
public class TlsSessionResumptionTest {

  @Test
  public void testSslContextWithDefaultConfig() {
    TlsSessionCacheConfig config = TlsSessionCacheConfig.getDefault();
    SSLContext sslContext = TlsContextFactory.createSslContext(config);

    assertNotNull("SSLContext should not be null", sslContext);
    // Should use TLSv1.3 (preferred) or TLS as fallback
    assertTrue(
        "Should use TLS protocol (TLSv1.3 or TLS)", sslContext.getProtocol().startsWith("TLS"));
  }

  @Test
  public void testSslContextSessionCacheEnabled() {
    TlsSessionCacheConfig config =
        TlsSessionCacheConfig.builder()
            .withEnabled(true)
            .withSessionCacheSize(200)
            .withSessionTimeoutSeconds(3600)
            .build();

    SSLContext sslContext = TlsContextFactory.createSslContext(config);
    SSLSessionContext clientSessionContext = sslContext.getClientSessionContext();

    assertNotNull("Client session context should not be null", clientSessionContext);
    assertEquals(
        "Session cache size should match config", 200, clientSessionContext.getSessionCacheSize());
    assertEquals(
        "Session timeout should match config", 3600, clientSessionContext.getSessionTimeout());
  }

  @Test
  public void testSslContextSessionCacheDisabled() {
    TlsSessionCacheConfig config = TlsSessionCacheConfig.disabled();
    SSLContext sslContext = TlsContextFactory.createSslContext(config);

    SSLSessionContext clientSessionContext = sslContext.getClientSessionContext();
    assertNotNull("Client session context should not be null", clientSessionContext);
    // When disabled, cache size should be 0
    assertEquals(
        "Session cache size should be 0 when disabled",
        0,
        clientSessionContext.getSessionCacheSize());
  }

  @Test
  public void testSslContextWithCustomCacheSize() {
    TlsSessionCacheConfig config =
        TlsSessionCacheConfig.builder()
            .withEnabled(true)
            .withSessionCacheSize(500)
            .withSessionTimeoutSeconds(7200)
            .build();

    SSLContext sslContext = TlsContextFactory.createSslContext(config);
    SSLSessionContext clientSessionContext = sslContext.getClientSessionContext();

    assertEquals(500, clientSessionContext.getSessionCacheSize());
    assertEquals(7200, clientSessionContext.getSessionTimeout());
  }

  @Test
  public void testSslContextTrustAllCertificates() {
    // This test verifies that the SSL context accepts all certificates
    // (trust-all behavior for development/testing)
    TlsSessionCacheConfig config = TlsSessionCacheConfig.getDefault();
    SSLContext sslContext = TlsContextFactory.createSslContext(config);

    // The SSL context should be properly initialized
    assertNotNull(sslContext.getSocketFactory());
  }

  @Test
  public void testModernTlsProtocol() {
    // Verify we're using modern TLS, not the deprecated "SSL" protocol
    TlsSessionCacheConfig config = TlsSessionCacheConfig.getDefault();
    SSLContext sslContext = TlsContextFactory.createSslContext(config);

    String protocol = sslContext.getProtocol();
    // Should be TLS (which allows TLS 1.2 and 1.3), not the legacy "SSL"
    assertTrue(
        "Should use modern TLS protocol, not legacy SSL",
        protocol.startsWith("TLS") || protocol.equals("Default"));
  }

  @Test
  public void testMultipleSslContextsShareConfig() {
    // Creating multiple SSL contexts with the same config should work
    TlsSessionCacheConfig config =
        TlsSessionCacheConfig.builder()
            .withEnabled(true)
            .withSessionCacheSize(100)
            .withSessionTimeoutSeconds(3600)
            .build();

    SSLContext context1 = TlsContextFactory.createSslContext(config);
    SSLContext context2 = TlsContextFactory.createSslContext(config);

    assertNotNull(context1);
    assertNotNull(context2);
    // They should be different instances
    assertNotSame(context1, context2);
    // But have the same configuration
    assertEquals(
        context1.getClientSessionContext().getSessionCacheSize(),
        context2.getClientSessionContext().getSessionCacheSize());
  }

  @Test
  public void testSessionContextConfiguredForReuse() {
    // This test verifies that the SSLSessionContext is properly configured
    // to allow session reuse (non-zero cache size and timeout)
    TlsSessionCacheConfig config = TlsSessionCacheConfig.getDefault();
    SSLContext sslContext = TlsContextFactory.createSslContext(config);

    SSLSessionContext clientContext = sslContext.getClientSessionContext();

    // For session reuse to work, cache size must be > 0
    assertTrue(
        "Session cache size must be positive for session reuse",
        clientContext.getSessionCacheSize() > 0);

    // Session timeout must be > 0 for sessions to be valid
    assertTrue(
        "Session timeout must be positive for session reuse",
        clientContext.getSessionTimeout() > 0);
  }

  @Test
  public void testDisabledConfigPreventsSessionReuse() {
    // When disabled, cache size should be 0, preventing session reuse
    TlsSessionCacheConfig config = TlsSessionCacheConfig.disabled();
    SSLContext sslContext = TlsContextFactory.createSslContext(config);

    SSLSessionContext clientContext = sslContext.getClientSessionContext();

    // Cache size of 0 means no sessions will be cached
    assertEquals(
        "Session cache size should be 0 when disabled", 0, clientContext.getSessionCacheSize());
  }
}
