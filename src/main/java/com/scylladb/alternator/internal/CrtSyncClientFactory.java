package com.scylladb.alternator.internal;

import com.scylladb.alternator.AlternatorConfig;
import com.scylladb.alternator.TlsConfig;
import java.time.Duration;
import java.util.function.Consumer;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpConfigurationOption;
import software.amazon.awssdk.http.crt.AwsCrtHttpClient;
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
    return buildWithTls(builder, tlsConfig);
  }

  /**
   * Creates a small sync HTTP client for LiveNodes polling using CRT.
   *
   * @param tlsConfig the TLS configuration
   * @return a configured SdkHttpClient with small pool size
   */
  public static SdkHttpClient createPollingClient(TlsConfig tlsConfig) {
    AwsCrtHttpClient.Builder builder = AwsCrtHttpClient.builder();
    builder.maxConcurrency(4);

    return buildWithTls(builder, tlsConfig);
  }

  private static SdkHttpClient buildWithTls(AwsCrtHttpClient.Builder builder, TlsConfig tlsConfig) {
    if (tlsConfig != null) {
      // Validate custom TLS config eagerly to fail fast on invalid certs
      if (!tlsConfig.getCustomCaCertPaths().isEmpty()) {
        TlsContextFactory.createTrustManagers(tlsConfig);
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
