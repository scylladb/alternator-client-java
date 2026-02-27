package com.scylladb.alternator.internal;

import com.scylladb.alternator.AlternatorConfig;
import com.scylladb.alternator.TlsConfig;
import java.time.Duration;
import java.util.function.Consumer;
import javax.net.ssl.TrustManager;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpConfigurationOption;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.utils.AttributeMap;

/**
 * Factory for creating sync HTTP clients using Apache HttpClient.
 *
 * <p>This factory creates an {@link ApacheHttpClient} with Alternator-optimized defaults and
 * applies an optional user customizer callback.
 *
 * @since 2.1.0
 */
public final class ApacheSyncClientFactory {

  private ApacheSyncClientFactory() {}

  /**
   * Creates a sync HTTP client using Apache HttpClient.
   *
   * @param customizer optional consumer to customize the builder (may be null)
   * @param config the Alternator configuration
   * @param tlsConfig the TLS configuration
   * @return a configured SdkHttpClient
   */
  public static SdkHttpClient create(
      Consumer<ApacheHttpClient.Builder> customizer, AlternatorConfig config, TlsConfig tlsConfig) {
    ApacheHttpClient.Builder builder = ApacheHttpClient.builder();
    builder.tcpKeepAlive(true);

    // Apply Alternator-optimized defaults from config
    if (config != null) {
      if (config.getMaxConnections() > 0) {
        builder.maxConnections(config.getMaxConnections());
      }
      if (config.getConnectionMaxIdleTimeMs() > 0) {
        builder.connectionMaxIdleTime(Duration.ofMillis(config.getConnectionMaxIdleTimeMs()));
      }
      if (config.getConnectionTimeToLiveMs() > 0) {
        builder.connectionTimeToLive(Duration.ofMillis(config.getConnectionTimeToLiveMs()));
      }
    }

    // Apply TLS trust manager settings
    if (tlsConfig != null
        && !tlsConfig.isTrustAllCertificates()
        && (!tlsConfig.getCustomCaCertPaths().isEmpty() || !tlsConfig.isTrustSystemCaCerts())) {
      TrustManager[] trustManagers = TlsContextFactory.createTrustManagers(tlsConfig);
      builder.tlsTrustManagersProvider(() -> trustManagers);
    }

    // Apply user customizer last — allows overriding any defaults including TLS
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

  /**
   * Creates a small sync HTTP client for LiveNodes polling.
   *
   * @param tlsConfig the TLS configuration
   * @return a configured SdkHttpClient with small pool size
   */
  public static SdkHttpClient createPollingClient(TlsConfig tlsConfig) {
    ApacheHttpClient.Builder builder = ApacheHttpClient.builder();
    builder.tcpKeepAlive(true);
    builder.maxConnections(4);

    return buildWithTls(builder, tlsConfig);
  }

  private static SdkHttpClient buildWithTls(ApacheHttpClient.Builder builder, TlsConfig tlsConfig) {
    if (tlsConfig != null) {
      if (tlsConfig.isTrustAllCertificates()) {
        return builder.buildWithDefaults(
            AttributeMap.builder()
                .put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, true)
                .build());
      }
      if (!tlsConfig.getCustomCaCertPaths().isEmpty() || !tlsConfig.isTrustSystemCaCerts()) {
        // Eagerly validate to fail fast on invalid cert paths
        TrustManager[] trustManagers = TlsContextFactory.createTrustManagers(tlsConfig);
        builder.tlsTrustManagersProvider(() -> trustManagers);
      }
    }
    return builder.build();
  }
}
