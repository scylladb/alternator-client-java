package com.scylladb.alternator.internal;

import com.scylladb.alternator.AlternatorConfig;
import com.scylladb.alternator.TlsConfig;
import java.time.Duration;
import java.util.function.Consumer;
import java.util.logging.Logger;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpConfigurationOption;
import software.amazon.awssdk.http.crt.AwsCrtHttpClient;
import software.amazon.awssdk.http.crt.TcpKeepAliveConfiguration;
import software.amazon.awssdk.utils.AttributeMap;

/**
 * Factory for creating sync HTTP clients using AWS CRT.
 *
 * <p>This factory creates an {@link AwsCrtHttpClient} with Alternator-optimized defaults and
 * applies an optional user customizer callback.
 *
 * @since 2.1.0
 */
public final class CrtSyncClientFactory {

  private static final Logger logger = Logger.getLogger(CrtSyncClientFactory.class.getName());

  private CrtSyncClientFactory() {}

  /**
   * Creates a sync HTTP client using AWS CRT.
   *
   * @param customizer optional consumer to customize the builder (may be null)
   * @param config the Alternator configuration
   * @param tlsConfig the TLS configuration
   * @return a configured SdkHttpClient
   */
  public static SdkHttpClient create(
      Consumer<AwsCrtHttpClient.Builder> customizer, AlternatorConfig config, TlsConfig tlsConfig) {
    AwsCrtHttpClient.Builder builder = AwsCrtHttpClient.builder();
    builder.tcpKeepAliveConfiguration(
        TcpKeepAliveConfiguration.builder()
            .keepAliveInterval(Duration.ofSeconds(30))
            .keepAliveTimeout(Duration.ofSeconds(30))
            .build());

    // Apply Alternator-optimized defaults from config
    if (config != null) {
      builder.maxConcurrency(config.getMaxConnections());
      if (config.getConnectionMaxIdleTimeMs() > 0) {
        builder.connectionMaxIdleTime(Duration.ofMillis(config.getConnectionMaxIdleTimeMs()));
      } else if (config.getConnectionMaxIdleTimeMs() == 0) {
        logger.info(
            "connectionMaxIdleTimeMs=0 is not supported by CRT HTTP client;"
                + " falling back to SDK default.");
      }
      // CRT SDK requires positive durations for these timeouts; skip when 0 (use SDK default)
      if (config.getConnectionAcquisitionTimeoutMs() > 0) {
        builder.connectionAcquisitionTimeout(
            Duration.ofMillis(config.getConnectionAcquisitionTimeoutMs()));
      } else if (config.getConnectionAcquisitionTimeoutMs() == 0) {
        logger.info(
            "connectionAcquisitionTimeoutMs=0 is not supported by CRT HTTP client;"
                + " falling back to SDK default.");
      }
      if (config.getConnectionTimeoutMs() > 0) {
        builder.connectionTimeout(Duration.ofMillis(config.getConnectionTimeoutMs()));
      } else if (config.getConnectionTimeoutMs() == 0) {
        logger.info(
            "connectionTimeoutMs=0 is not supported by CRT HTTP client;"
                + " falling back to SDK default.");
      }
      if (config.getConnectionTimeToLiveMs() > 0) {
        logger.warning(
            "connectionTimeToLiveMs is not supported by the CRT HTTP client and will be ignored. "
                + "Use Apache HTTP client if connection TTL is required.");
      }
    }

    // Validate CRT limitations before customizer
    if (tlsConfig != null && !tlsConfig.getCustomCaCertPaths().isEmpty()) {
      throw new UnsupportedOperationException(
          "Custom CA certificates are not supported with the CRT HTTP client. "
              + "Use Apache or Netty HTTP client instead, or use TlsConfig.trustAll() for testing.");
    }

    // Apply user customizer last — allows overriding any defaults
    if (customizer != null) {
      customizer.accept(builder);
    }

    return buildWithTls(builder, tlsConfig);
  }

  /**
   * Creates a small sync HTTP client for LiveNodes polling using CRT.
   *
   * @param tlsConfig the TLS configuration
   * @return a configured SdkHttpClient with small pool size
   */
  public static SdkHttpClient createPollingClient(TlsConfig tlsConfig) {
    if (tlsConfig != null && !tlsConfig.getCustomCaCertPaths().isEmpty()) {
      throw new UnsupportedOperationException(
          "Custom CA certificates are not supported with the CRT HTTP client. "
              + "Use Apache or Netty HTTP client instead, or use TlsConfig.trustAll() for testing.");
    }
    AwsCrtHttpClient.Builder builder = AwsCrtHttpClient.builder();
    builder.tcpKeepAliveConfiguration(
        TcpKeepAliveConfiguration.builder()
            .keepAliveInterval(Duration.ofSeconds(30))
            .keepAliveTimeout(Duration.ofSeconds(30))
            .build());
    builder.maxConcurrency(4);

    return buildWithTls(builder, tlsConfig);
  }

  private static SdkHttpClient buildWithTls(AwsCrtHttpClient.Builder builder, TlsConfig tlsConfig) {
    if (tlsConfig != null) {
      if (!tlsConfig.getCustomCaCertPaths().isEmpty()) {
        throw new UnsupportedOperationException(
            "Custom CA certificates are not supported with the CRT HTTP client. "
                + "Use Apache or Netty HTTP client instead, or use TlsConfig.trustAll() for testing.");
      }
      if (tlsConfig.isTrustAllCertificates()) {
        return builder.buildWithDefaults(
            AttributeMap.builder()
                .put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, true)
                .build());
      }
    }
    return builder.build();
  }
}
