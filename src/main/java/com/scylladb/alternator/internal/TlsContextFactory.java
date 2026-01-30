package com.scylladb.alternator.internal;

import com.scylladb.alternator.TlsSessionCacheConfig;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSessionContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

/**
 * Factory for creating SSLContext instances with TLS session ticket support.
 *
 * <p>This factory creates SSL contexts configured for:
 *
 * <ul>
 *   <li>Modern TLS protocols (TLS 1.2+) that support session tickets
 *   <li>Configurable session caching for TLS session resumption
 *   <li>Trust-all certificate validation (for development/testing with self-signed certs)
 * </ul>
 *
 * <p><strong>Security Note:</strong> The created SSL contexts accept all certificates including
 * self-signed ones. This is intentional for development and testing scenarios but should be
 * carefully considered for production deployments.
 *
 * @author dmitry.kropachev
 * @since 1.0.5
 */
public final class TlsContextFactory {

  private TlsContextFactory() {
    // Utility class
  }

  /**
   * Creates an SSLContext configured according to the provided TLS session cache configuration.
   *
   * <p>The returned context:
   *
   * <ul>
   *   <li>Uses TLSv1.3 protocol (preferred) or TLS (fallback) for session ticket support
   *   <li>Has session caching configured per the provided config
   *   <li>Trusts all certificates (including self-signed)
   * </ul>
   *
   * <p><b>Note:</b> TLS 1.3 session tickets require server-side support. In ScyllaDB, enable
   * session tickets via {@code alternator_encryption_options.enable_session_tickets: true} in
   * scylla.yaml.
   *
   * @param config the TLS session cache configuration
   * @return a configured SSLContext
   * @throws RuntimeException if the SSL context cannot be created
   */
  public static SSLContext createSslContext(TlsSessionCacheConfig config) {
    TrustManager[] trustAllCertificates = createTrustAllManagers();

    try {
      // Prefer TLSv1.3 for session ticket support, fall back to TLS if not available
      SSLContext sslContext;
      try {
        sslContext = SSLContext.getInstance("TLSv1.3");
      } catch (NoSuchAlgorithmException e) {
        // Fall back to generic TLS (will use highest available version)
        sslContext = SSLContext.getInstance("TLS");
      }
      sslContext.init(null, trustAllCertificates, new java.security.SecureRandom());

      // Configure session caching
      configureSessionCache(sslContext, config);

      return sslContext;
    } catch (NoSuchAlgorithmException | KeyManagementException e) {
      throw new RuntimeException("Failed to create SSL context", e);
    }
  }

  /**
   * Creates trust managers that accept all certificates.
   *
   * <p><strong>Security Note:</strong> This is intended for development and testing with
   * self-signed certificates. For production, consider using proper certificate validation.
   *
   * @return an array containing a trust-all X509TrustManager
   */
  private static TrustManager[] createTrustAllManagers() {
    return new TrustManager[] {
      new X509TrustManager() {
        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType) {
          // Accept all client certificates
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType) {
          // Accept all server certificates
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
          return new X509Certificate[0];
        }
      }
    };
  }

  /**
   * Configures the SSL session cache based on the provided configuration.
   *
   * @param sslContext the SSL context to configure
   * @param config the TLS session cache configuration
   */
  private static void configureSessionCache(SSLContext sslContext, TlsSessionCacheConfig config) {
    SSLSessionContext clientSessionContext = sslContext.getClientSessionContext();
    if (clientSessionContext != null) {
      if (config.isEnabled()) {
        // Enable session caching with configured size and timeout
        clientSessionContext.setSessionCacheSize(config.getSessionCacheSize());
        clientSessionContext.setSessionTimeout(config.getSessionTimeoutSeconds());
      } else {
        // Disable session caching by setting cache size to 0
        clientSessionContext.setSessionCacheSize(0);
      }
    }
  }
}
