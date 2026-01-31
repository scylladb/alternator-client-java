package com.scylladb.alternator;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Configuration for TLS/SSL settings including custom CA certificate support.
 *
 * <p>This class consolidates all TLS-related settings for the Alternator client:
 *
 * <ul>
 *   <li>Custom CA certificates from file paths (PEM format)
 *   <li>Option to trust system CA certificates
 *   <li>Trust-all mode for development/testing
 *   <li>Hostname verification control
 *   <li>Session cache configuration for TLS resumption
 * </ul>
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * // Trust all certificates (development/testing)
 * TlsConfig config = TlsConfig.trustAll();
 *
 * // Use system CA certificates only
 * TlsConfig config = TlsConfig.systemDefault();
 *
 * // Use custom CA certificate
 * TlsConfig config = TlsConfig.builder()
 *     .withCaCertPath(Paths.get("/path/to/ca.pem"))
 *     .withTrustSystemCaCerts(false)  // only trust the custom CA
 *     .build();
 *
 * // Combine custom CA with system CAs
 * TlsConfig config = TlsConfig.builder()
 *     .withCaCertPath(Paths.get("/path/to/ca.pem"))
 *     .withTrustSystemCaCerts(true)
 *     .build();
 * }</pre>
 *
 * @author dmitry.kropachev
 * @since 1.0.9
 */
public final class TlsConfig {

  private static final TlsConfig TRUST_ALL_INSTANCE =
      new TlsConfig(
          Collections.<Path>emptyList(), false, true, false, TlsSessionCacheConfig.getDefault());

  private static final TlsConfig SYSTEM_DEFAULT_INSTANCE =
      new TlsConfig(
          Collections.<Path>emptyList(), true, false, true, TlsSessionCacheConfig.getDefault());

  private final List<Path> customCaCertPaths;
  private final boolean trustSystemCaCerts;
  private final boolean trustAllCertificates;
  private final boolean verifyHostname;
  private final TlsSessionCacheConfig sessionCacheConfig;

  private TlsConfig(
      List<Path> customCaCertPaths,
      boolean trustSystemCaCerts,
      boolean trustAllCertificates,
      boolean verifyHostname,
      TlsSessionCacheConfig sessionCacheConfig) {
    this.customCaCertPaths =
        customCaCertPaths != null
            ? Collections.unmodifiableList(new ArrayList<>(customCaCertPaths))
            : Collections.<Path>emptyList();
    this.trustSystemCaCerts = trustSystemCaCerts;
    this.trustAllCertificates = trustAllCertificates;
    this.verifyHostname = verifyHostname;
    this.sessionCacheConfig =
        sessionCacheConfig != null ? sessionCacheConfig : TlsSessionCacheConfig.getDefault();
  }

  /**
   * Returns a TlsConfig that trusts all certificates (including self-signed).
   *
   * <p><strong>Security Warning:</strong> This configuration should only be used for
   * development/testing purposes. It disables certificate validation entirely, making connections
   * vulnerable to man-in-the-middle attacks.
   *
   * <p>This configuration:
   *
   * <ul>
   *   <li>Trusts ALL certificates (no validation)
   *   <li>Disables hostname verification
   *   <li>Uses default session cache settings
   * </ul>
   *
   * @return a TlsConfig that trusts all certificates
   */
  public static TlsConfig trustAll() {
    return TRUST_ALL_INSTANCE;
  }

  /**
   * Returns a TlsConfig that uses the system's default CA certificates.
   *
   * <p>This configuration uses the JVM's default trust store (typically located at
   * $JAVA_HOME/lib/security/cacerts) for certificate validation.
   *
   * <p>This configuration:
   *
   * <ul>
   *   <li>Trusts system CA certificates
   *   <li>Enables hostname verification
   *   <li>Uses default session cache settings
   * </ul>
   *
   * @return a TlsConfig that uses system CA certificates
   */
  public static TlsConfig systemDefault() {
    return SYSTEM_DEFAULT_INSTANCE;
  }

  /**
   * Creates a new builder for TlsConfig.
   *
   * @return a new {@link Builder}
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns the list of custom CA certificate file paths.
   *
   * <p>These are paths to PEM-encoded certificate files that will be used for server certificate
   * validation.
   *
   * @return an unmodifiable list of CA certificate paths, never null but may be empty
   */
  public List<Path> getCustomCaCertPaths() {
    return customCaCertPaths;
  }

  /**
   * Returns whether system CA certificates should be trusted.
   *
   * <p>When true, the JVM's default trust store is included in certificate validation along with
   * any custom CA certificates.
   *
   * @return true if system CA certificates should be trusted
   */
  public boolean isTrustSystemCaCerts() {
    return trustSystemCaCerts;
  }

  /**
   * Returns whether all certificates should be trusted (no validation).
   *
   * <p><strong>Security Warning:</strong> When true, certificate validation is completely disabled.
   * This should only be used for development/testing.
   *
   * @return true if all certificates should be trusted
   */
  public boolean isTrustAllCertificates() {
    return trustAllCertificates;
  }

  /**
   * Returns whether hostname verification is enabled.
   *
   * <p>When true, the server's hostname is verified against the certificate's Subject Alternative
   * Names (SANs) or Common Name (CN).
   *
   * @return true if hostname verification is enabled
   */
  public boolean isVerifyHostname() {
    return verifyHostname;
  }

  /**
   * Returns the TLS session cache configuration.
   *
   * @return the session cache configuration, never null
   */
  public TlsSessionCacheConfig getSessionCacheConfig() {
    return sessionCacheConfig;
  }

  @Override
  public String toString() {
    return "TlsConfig{"
        + "customCaCertPaths="
        + customCaCertPaths
        + ", trustSystemCaCerts="
        + trustSystemCaCerts
        + ", trustAllCertificates="
        + trustAllCertificates
        + ", verifyHostname="
        + verifyHostname
        + ", sessionCacheConfig="
        + sessionCacheConfig
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TlsConfig tlsConfig = (TlsConfig) o;
    return trustSystemCaCerts == tlsConfig.trustSystemCaCerts
        && trustAllCertificates == tlsConfig.trustAllCertificates
        && verifyHostname == tlsConfig.verifyHostname
        && Objects.equals(customCaCertPaths, tlsConfig.customCaCertPaths)
        && Objects.equals(sessionCacheConfig, tlsConfig.sessionCacheConfig);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        customCaCertPaths,
        trustSystemCaCerts,
        trustAllCertificates,
        verifyHostname,
        sessionCacheConfig);
  }

  /** Builder for creating {@link TlsConfig} instances. */
  public static class Builder {
    private List<Path> customCaCertPaths = new ArrayList<>();
    private boolean trustSystemCaCerts = true;
    private boolean trustAllCertificates = false;
    private boolean verifyHostname = true;
    private TlsSessionCacheConfig sessionCacheConfig = null;

    Builder() {}

    /**
     * Adds a custom CA certificate path.
     *
     * <p>The file should be PEM-encoded and can contain one or more certificates. Multiple
     * certificate files can be added by calling this method multiple times.
     *
     * @param path the path to a PEM-encoded CA certificate file
     * @return this builder instance
     * @throws IllegalArgumentException if path is null
     */
    public Builder withCaCertPath(Path path) {
      if (path == null) {
        throw new IllegalArgumentException("CA certificate path cannot be null");
      }
      this.customCaCertPaths.add(path);
      return this;
    }

    /**
     * Sets multiple custom CA certificate paths.
     *
     * <p>This replaces any previously added paths. Each file should be PEM-encoded and can contain
     * one or more certificates.
     *
     * @param paths the paths to PEM-encoded CA certificate files
     * @return this builder instance
     */
    public Builder withCaCertPaths(Collection<Path> paths) {
      this.customCaCertPaths = paths != null ? new ArrayList<>(paths) : new ArrayList<Path>();
      return this;
    }

    /**
     * Sets whether to trust system CA certificates.
     *
     * <p>When true (default), the JVM's default trust store is included in certificate validation.
     * Set to false to only trust explicitly configured custom CA certificates.
     *
     * @param trustSystemCaCerts true to include system CAs in trust evaluation
     * @return this builder instance
     */
    public Builder withTrustSystemCaCerts(boolean trustSystemCaCerts) {
      this.trustSystemCaCerts = trustSystemCaCerts;
      return this;
    }

    /**
     * Sets whether to trust all certificates (disable certificate validation).
     *
     * <p><strong>Security Warning:</strong> Setting this to true disables all certificate
     * validation and should only be used for development/testing. This also disables hostname
     * verification.
     *
     * @param trustAllCertificates true to trust all certificates
     * @return this builder instance
     */
    public Builder withTrustAllCertificates(boolean trustAllCertificates) {
      this.trustAllCertificates = trustAllCertificates;
      return this;
    }

    /**
     * Sets whether to verify hostnames in server certificates.
     *
     * <p>When true (default), the server's hostname is verified against the certificate's Subject
     * Alternative Names (SANs) or Common Name (CN). Set to false to skip hostname verification.
     *
     * <p><strong>Security Warning:</strong> Disabling hostname verification makes connections
     * vulnerable to man-in-the-middle attacks where an attacker could present a valid certificate
     * for a different hostname.
     *
     * @param verifyHostname true to enable hostname verification
     * @return this builder instance
     */
    public Builder withVerifyHostname(boolean verifyHostname) {
      this.verifyHostname = verifyHostname;
      return this;
    }

    /**
     * Sets the TLS session cache configuration.
     *
     * <p>TLS session caching enables quick TLS renegotiation by caching session tickets.
     *
     * @param sessionCacheConfig the session cache configuration, or null to use default
     * @return this builder instance
     */
    public Builder withSessionCacheConfig(TlsSessionCacheConfig sessionCacheConfig) {
      this.sessionCacheConfig = sessionCacheConfig;
      return this;
    }

    /**
     * Builds and returns a {@link TlsConfig} instance.
     *
     * <p>Validation rules:
     *
     * <ul>
     *   <li>If trustAllCertificates is true, other trust settings are ignored
     *   <li>If trustAllCertificates is false and no custom CAs are provided and trustSystemCaCerts
     *       is false, an exception is thrown
     * </ul>
     *
     * @return a new TlsConfig instance
     * @throws IllegalStateException if the configuration is invalid
     */
    public TlsConfig build() {
      // If trust-all is enabled, hostname verification should be disabled
      boolean effectiveVerifyHostname = trustAllCertificates ? false : verifyHostname;

      // Validate that we have at least some way to validate certificates
      if (!trustAllCertificates && !trustSystemCaCerts && customCaCertPaths.isEmpty()) {
        throw new IllegalStateException(
            "Invalid TLS configuration: no trust source configured. "
                + "Either enable trustSystemCaCerts, add custom CA certificates, "
                + "or enable trustAllCertificates (for development only).");
      }

      return new TlsConfig(
          customCaCertPaths,
          trustSystemCaCerts,
          trustAllCertificates,
          effectiveVerifyHostname,
          sessionCacheConfig);
    }
  }
}
