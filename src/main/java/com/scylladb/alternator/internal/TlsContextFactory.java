package com.scylladb.alternator.internal;

import com.scylladb.alternator.TlsConfig;
import com.scylladb.alternator.TlsSessionCacheConfig;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSessionContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

/**
 * Factory for creating SSLContext instances with TLS session ticket support.
 *
 * <p>This factory creates SSL contexts configured for:
 *
 * <ul>
 *   <li>Modern TLS protocols (TLS 1.2+) that support session tickets
 *   <li>Configurable session caching for TLS session resumption
 *   <li>Custom CA certificate support for production deployments
 *   <li>Trust-all certificate validation (for development/testing with self-signed certs)
 * </ul>
 *
 * <p><strong>Security Note:</strong> For production deployments, use {@link
 * #createSslContext(TlsConfig)} with properly configured CA certificates. The legacy {@link
 * #createSslContext(TlsSessionCacheConfig)} method trusts all certificates and should only be used
 * for development/testing.
 *
 * @author dmitry.kropachev
 * @since 2.0.0
 */
public final class TlsContextFactory {

  private TlsContextFactory() {
    // Utility class
  }

  /**
   * Creates an SSLContext configured according to the provided TLS configuration.
   *
   * <p>This method supports:
   *
   * <ul>
   *   <li>Custom CA certificates from PEM files
   *   <li>System CA certificates (JVM default trust store)
   *   <li>Combination of custom and system CAs
   *   <li>Trust-all mode for development/testing
   * </ul>
   *
   * @param config the TLS configuration
   * @return a configured SSLContext
   * @throws RuntimeException if the SSL context cannot be created or certificates cannot be loaded
   * @since 1.0.9
   */
  public static SSLContext createSslContext(TlsConfig config) {
    if (config == null) {
      config = TlsConfig.trustAll();
    }

    TrustManager[] trustManagers;
    if (config.isTrustAllCertificates()) {
      trustManagers = createTrustAllManagers();
    } else {
      trustManagers = createTrustManagers(config);
    }

    try {
      SSLContext sslContext = createBaseContext();
      sslContext.init(null, trustManagers, new java.security.SecureRandom());
      configureSessionCache(sslContext, config.getSessionCacheConfig());
      return sslContext;
    } catch (NoSuchAlgorithmException | KeyManagementException e) {
      throw new RuntimeException("Failed to create SSL context", e);
    }
  }

  /**
   * Creates trust managers based on the TLS configuration.
   *
   * @param config the TLS configuration
   * @return an array of TrustManagers
   */
  static TrustManager[] createTrustManagers(TlsConfig config) {
    try {
      List<TrustManager> allTrustManagers = new ArrayList<>();

      // Add custom CA certificates if provided
      if (!config.getCustomCaCertPaths().isEmpty()) {
        KeyStore customKeyStore = loadCertificates(config.getCustomCaCertPaths());
        TrustManagerFactory customTmf =
            TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        customTmf.init(customKeyStore);
        for (TrustManager tm : customTmf.getTrustManagers()) {
          allTrustManagers.add(tm);
        }
      }

      // Add system CA certificates if requested
      if (config.isTrustSystemCaCerts()) {
        TrustManagerFactory systemTmf =
            TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        systemTmf.init((KeyStore) null); // null loads the default trust store
        for (TrustManager tm : systemTmf.getTrustManagers()) {
          allTrustManagers.add(tm);
        }
      }

      // If we have multiple trust managers, combine them
      if (allTrustManagers.size() > 1) {
        return new TrustManager[] {new CompositeX509TrustManager(allTrustManagers)};
      } else if (allTrustManagers.size() == 1) {
        return new TrustManager[] {allTrustManagers.get(0)};
      } else {
        throw new RuntimeException(
            "No trust managers configured. Enable system CAs or provide custom CA certificates.");
      }
    } catch (NoSuchAlgorithmException | KeyStoreException e) {
      throw new RuntimeException("Failed to create trust managers", e);
    }
  }

  /**
   * Loads certificates from the specified PEM files into a KeyStore.
   *
   * @param certPaths the paths to PEM-encoded certificate files
   * @return a KeyStore containing the loaded certificates
   */
  private static KeyStore loadCertificates(List<Path> certPaths) {
    try {
      KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
      keyStore.load(null, null); // Initialize empty keystore

      CertificateFactory certFactory = CertificateFactory.getInstance("X.509");
      int certIndex = 0;

      for (Path certPath : certPaths) {
        try (InputStream is = new BufferedInputStream(Files.newInputStream(certPath))) {
          Collection<? extends Certificate> certs = certFactory.generateCertificates(is);
          for (Certificate cert : certs) {
            String alias = "custom-ca-" + certIndex++;
            keyStore.setCertificateEntry(alias, cert);
          }
        }
      }

      return keyStore;
    } catch (KeyStoreException | CertificateException | NoSuchAlgorithmException | IOException e) {
      throw new RuntimeException("Failed to load CA certificates", e);
    }
  }

  /**
   * Creates the base SSLContext with appropriate TLS version.
   *
   * @return an uninitialized SSLContext
   * @throws NoSuchAlgorithmException if TLS is not available
   */
  private static SSLContext createBaseContext() throws NoSuchAlgorithmException {
    // Prefer TLSv1.3 for session ticket support, fall back to TLS if not available
    try {
      return SSLContext.getInstance("TLSv1.3");
    } catch (NoSuchAlgorithmException e) {
      // Fall back to generic TLS (will use highest available version)
      return SSLContext.getInstance("TLS");
    }
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
   * <p><strong>Security Note:</strong> This method trusts all certificates including self-signed
   * ones. For production deployments, use {@link #createSslContext(TlsConfig)} with properly
   * configured CA certificates instead.
   *
   * @param config the TLS session cache configuration
   * @return a configured SSLContext
   * @throws RuntimeException if the SSL context cannot be created
   * @deprecated Use {@link #createSslContext(TlsConfig)} for better security configuration
   */
  @Deprecated
  public static SSLContext createSslContext(TlsSessionCacheConfig config) {
    TlsConfig tlsConfig =
        TlsConfig.builder().withTrustAllCertificates(true).withSessionCacheConfig(config).build();
    return createSslContext(tlsConfig);
  }

  /**
   * Creates trust managers that accept all certificates.
   *
   * <p><strong>Security Note:</strong> This is intended for development and testing with
   * self-signed certificates. For production, consider using proper certificate validation.
   *
   * @return an array containing a trust-all X509TrustManager
   */
  static TrustManager[] createTrustAllManagers() {
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
    if (clientSessionContext != null && config != null) {
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

  /**
   * A composite X509TrustManager that delegates to multiple trust managers.
   *
   * <p>This allows combining custom CA certificates with system CA certificates. A certificate is
   * trusted if any of the underlying trust managers trusts it.
   */
  private static class CompositeX509TrustManager implements X509TrustManager {
    private final List<X509TrustManager> trustManagers;

    CompositeX509TrustManager(List<TrustManager> managers) {
      this.trustManagers = new ArrayList<>();
      for (TrustManager tm : managers) {
        if (tm instanceof X509TrustManager) {
          this.trustManagers.add((X509TrustManager) tm);
        }
      }
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType)
        throws CertificateException {
      CertificateException lastException = null;
      for (X509TrustManager tm : trustManagers) {
        try {
          tm.checkClientTrusted(chain, authType);
          return; // Success
        } catch (CertificateException e) {
          lastException = e;
        }
      }
      if (lastException != null) {
        throw lastException;
      }
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType)
        throws CertificateException {
      CertificateException lastException = null;
      for (X509TrustManager tm : trustManagers) {
        try {
          tm.checkServerTrusted(chain, authType);
          return; // Success
        } catch (CertificateException e) {
          lastException = e;
        }
      }
      if (lastException != null) {
        throw lastException;
      }
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
      List<X509Certificate> issuers = new ArrayList<>();
      for (X509TrustManager tm : trustManagers) {
        X509Certificate[] certs = tm.getAcceptedIssuers();
        if (certs != null) {
          for (X509Certificate cert : certs) {
            issuers.add(cert);
          }
        }
      }
      return issuers.toArray(new X509Certificate[0]);
    }
  }
}
