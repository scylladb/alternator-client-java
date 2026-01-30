package com.scylladb.alternator;

import java.util.Objects;

/**
 * Configuration for TLS session caching to enable quick TLS renegotiation.
 *
 * <p>TLS session tickets (RFC 5077) allow clients to resume TLS sessions without performing a full
 * handshake. This significantly reduces latency when reconnecting to Alternator nodes, which is
 * especially beneficial in load-balanced scenarios where connections may be distributed across
 * different nodes.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * // Use default settings (enabled, 1024 sessions, 24h timeout)
 * TlsSessionCacheConfig config = TlsSessionCacheConfig.getDefault();
 *
 * // Custom configuration
 * TlsSessionCacheConfig config = TlsSessionCacheConfig.builder()
 *     .withEnabled(true)
 *     .withSessionCacheSize(200)
 *     .withSessionTimeoutSeconds(3600)  // 1 hour
 *     .build();
 *
 * // Disable session caching
 * TlsSessionCacheConfig config = TlsSessionCacheConfig.disabled();
 * }</pre>
 *
 * @author dmitry.kropachev
 * @since 1.0.5
 */
public final class TlsSessionCacheConfig {
  /** Default session cache size (number of sessions to cache). */
  public static final int DEFAULT_SESSION_CACHE_SIZE = 1024;

  /** Default session timeout in seconds (24 hours). */
  public static final int DEFAULT_SESSION_TIMEOUT_SECONDS = 86400;

  private static final TlsSessionCacheConfig DEFAULT_INSTANCE =
      new TlsSessionCacheConfig(true, DEFAULT_SESSION_CACHE_SIZE, DEFAULT_SESSION_TIMEOUT_SECONDS);

  private static final TlsSessionCacheConfig DISABLED_INSTANCE =
      new TlsSessionCacheConfig(false, 0, 0);

  private final boolean enabled;
  private final int sessionCacheSize;
  private final int sessionTimeoutSeconds;

  private TlsSessionCacheConfig(boolean enabled, int sessionCacheSize, int sessionTimeoutSeconds) {
    this.enabled = enabled;
    this.sessionCacheSize = sessionCacheSize;
    this.sessionTimeoutSeconds = sessionTimeoutSeconds;
  }

  /**
   * Returns the default TLS session cache configuration.
   *
   * <p>The default configuration has:
   *
   * <ul>
   *   <li>Session caching enabled
   *   <li>Cache size of 1024 sessions
   *   <li>Session timeout of 24 hours (86400 seconds)
   * </ul>
   *
   * @return the default configuration instance
   */
  public static TlsSessionCacheConfig getDefault() {
    return DEFAULT_INSTANCE;
  }

  /**
   * Returns a disabled TLS session cache configuration.
   *
   * <p>Use this when TLS session caching should be turned off entirely.
   *
   * @return a configuration with session caching disabled
   */
  public static TlsSessionCacheConfig disabled() {
    return DISABLED_INSTANCE;
  }

  /**
   * Creates a new builder for TlsSessionCacheConfig.
   *
   * @return a new {@link Builder}
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns whether TLS session caching is enabled.
   *
   * @return true if session caching is enabled, false otherwise
   */
  public boolean isEnabled() {
    return enabled;
  }

  /**
   * Returns the maximum number of TLS sessions to cache.
   *
   * <p>This controls how many session tickets can be stored for resumption. A larger value allows
   * more connections to benefit from session resumption but uses more memory.
   *
   * @return the session cache size
   */
  public int getSessionCacheSize() {
    return sessionCacheSize;
  }

  /**
   * Returns the session timeout in seconds.
   *
   * <p>Cached sessions older than this value will be invalidated. The timeout should be set based
   * on security requirements and expected connection patterns.
   *
   * @return the session timeout in seconds
   */
  public int getSessionTimeoutSeconds() {
    return sessionTimeoutSeconds;
  }

  @Override
  public String toString() {
    return "TlsSessionCacheConfig{"
        + "enabled="
        + enabled
        + ", sessionCacheSize="
        + sessionCacheSize
        + ", sessionTimeoutSeconds="
        + sessionTimeoutSeconds
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
    TlsSessionCacheConfig that = (TlsSessionCacheConfig) o;
    return enabled == that.enabled
        && sessionCacheSize == that.sessionCacheSize
        && sessionTimeoutSeconds == that.sessionTimeoutSeconds;
  }

  @Override
  public int hashCode() {
    return Objects.hash(enabled, sessionCacheSize, sessionTimeoutSeconds);
  }

  /** Builder for creating {@link TlsSessionCacheConfig} instances. */
  public static class Builder {
    private boolean enabled = true;
    private int sessionCacheSize = DEFAULT_SESSION_CACHE_SIZE;
    private int sessionTimeoutSeconds = DEFAULT_SESSION_TIMEOUT_SECONDS;

    Builder() {}

    /**
     * Enables or disables TLS session caching.
     *
     * @param enabled true to enable session caching, false to disable
     * @return this builder instance
     */
    public Builder withEnabled(boolean enabled) {
      this.enabled = enabled;
      return this;
    }

    /**
     * Sets the maximum number of TLS sessions to cache.
     *
     * @param sessionCacheSize the cache size (must be positive)
     * @return this builder instance
     */
    public Builder withSessionCacheSize(int sessionCacheSize) {
      this.sessionCacheSize = sessionCacheSize;
      return this;
    }

    /**
     * Sets the session timeout in seconds.
     *
     * @param sessionTimeoutSeconds the timeout in seconds (must be positive)
     * @return this builder instance
     */
    public Builder withSessionTimeoutSeconds(int sessionTimeoutSeconds) {
      this.sessionTimeoutSeconds = sessionTimeoutSeconds;
      return this;
    }

    /**
     * Builds and returns a {@link TlsSessionCacheConfig} instance.
     *
     * @return a new TlsSessionCacheConfig
     * @throws IllegalArgumentException if sessionCacheSize or sessionTimeoutSeconds is not positive
     *     when enabled
     */
    public TlsSessionCacheConfig build() {
      if (enabled) {
        if (sessionCacheSize <= 0) {
          throw new IllegalArgumentException(
              "sessionCacheSize must be positive when TLS session cache is enabled, but was: "
                  + sessionCacheSize);
        }
        if (sessionTimeoutSeconds <= 0) {
          throw new IllegalArgumentException(
              "sessionTimeoutSeconds must be positive when TLS session cache is enabled, but was: "
                  + sessionTimeoutSeconds);
        }
      }
      return new TlsSessionCacheConfig(enabled, sessionCacheSize, sessionTimeoutSeconds);
    }
  }
}
