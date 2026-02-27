package com.scylladb.alternator.internal;

import com.scylladb.alternator.AlternatorConfig;
import com.scylladb.alternator.TlsConfig;
import java.time.Duration;
import java.util.function.Consumer;
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
      if (config.getMaxConnections() > 0) {
        builder.maxConcurrency(config.getMaxConnections());
      }
      if (config.getConnectionMaxIdleTimeMs() > 0) {
        builder.connectionMaxIdleTime(Duration.ofMillis(config.getConnectionMaxIdleTimeMs()));
      }
    }

    // Apply user customizer
    if (customizer != null) {
      customizer.accept(builder);
    }

    // Apply TLS settings
    if (tlsConfig != null) {
      if (!tlsConfig.getCustomCaCertPaths().isEmpty()) {
        throw new UnsupportedOperationException(
            "Custom CA certificates are not supported with the CRT async HTTP client. "
                + "Use Netty HTTP client instead, or use TlsConfig.trustAll() for testing.");
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
