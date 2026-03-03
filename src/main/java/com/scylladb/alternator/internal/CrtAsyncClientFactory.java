package com.scylladb.alternator.internal;

import com.scylladb.alternator.AlternatorConfig;
import com.scylladb.alternator.TlsConfig;
import java.time.Duration;
import java.util.function.Consumer;
import java.util.logging.Logger;
import software.amazon.awssdk.http.SdkHttpConfigurationOption;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient;
import software.amazon.awssdk.http.crt.TcpKeepAliveConfiguration;
import software.amazon.awssdk.utils.AttributeMap;

/**
 * Factory for creating async HTTP clients using AWS CRT.
 *
 * <p>This factory creates an {@link AwsCrtAsyncHttpClient} with Alternator-optimized defaults and
 * applies an optional user customizer callback.
 *
 * @since 2.1.0
 */
public final class CrtAsyncClientFactory {

  private static final Logger logger = Logger.getLogger(CrtAsyncClientFactory.class.getName());

  private CrtAsyncClientFactory() {}

  /**
   * Creates an async HTTP client using AWS CRT.
   *
   * @param customizer optional consumer to customize the builder (may be null)
   * @param config the Alternator configuration
   * @param tlsConfig the TLS configuration
   * @return a configured SdkAsyncHttpClient
   */
  public static SdkAsyncHttpClient create(
      Consumer<AwsCrtAsyncHttpClient.Builder> customizer,
      AlternatorConfig config,
      TlsConfig tlsConfig) {
    AwsCrtAsyncHttpClient.Builder builder = AwsCrtAsyncHttpClient.builder();
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
            "connectionMaxIdleTimeMs=0 is not supported by CRT async HTTP client;"
                + " falling back to SDK default.");
      }
      // CRT SDK requires positive durations for these timeouts; skip when 0 (use SDK default)
      if (config.getConnectionAcquisitionTimeoutMs() > 0) {
        builder.connectionAcquisitionTimeout(
            Duration.ofMillis(config.getConnectionAcquisitionTimeoutMs()));
      } else if (config.getConnectionAcquisitionTimeoutMs() == 0) {
        logger.info(
            "connectionAcquisitionTimeoutMs=0 is not supported by CRT async HTTP client;"
                + " falling back to SDK default.");
      }
      if (config.getConnectionTimeoutMs() > 0) {
        builder.connectionTimeout(Duration.ofMillis(config.getConnectionTimeoutMs()));
      } else if (config.getConnectionTimeoutMs() == 0) {
        logger.info(
            "connectionTimeoutMs=0 is not supported by CRT async HTTP client;"
                + " falling back to SDK default.");
      }
      if (config.getConnectionTimeToLiveMs() > 0) {
        logger.warning(
            "connectionTimeToLiveMs is not supported by the CRT async HTTP client and will be"
                + " ignored. Use Netty HTTP client if connection TTL is required.");
      }
    }

    // Validate CRT limitations
    if (tlsConfig != null && !tlsConfig.getCustomCaCertPaths().isEmpty()) {
      throw new UnsupportedOperationException(
          "Custom CA certificates are not supported with the CRT async HTTP client. "
              + "Use Netty HTTP client instead, or use TlsConfig.trustAll() for testing.");
    }

    // Apply user customizer last — allows overriding any defaults
    if (customizer != null) {
      customizer.accept(builder);
    }

    if (tlsConfig != null && tlsConfig.isTrustAllCertificates()) {
      return builder.buildWithDefaults(
          AttributeMap.builder()
              .put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, true)
              .build());
    }
    return builder.build();
  }
}
