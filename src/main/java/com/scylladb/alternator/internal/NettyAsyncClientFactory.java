package com.scylladb.alternator.internal;

import com.scylladb.alternator.AlternatorConfig;
import com.scylladb.alternator.TlsConfig;
import java.time.Duration;
import java.util.function.Consumer;
import javax.net.ssl.TrustManager;
import software.amazon.awssdk.http.SdkHttpConfigurationOption;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.utils.AttributeMap;

/**
 * Factory for creating async HTTP clients using Netty NIO.
 *
 * <p>This factory creates a {@link NettyNioAsyncHttpClient} with Alternator-optimized defaults and
 * applies an optional user customizer callback.
 *
 * @since 2.1.0
 */
public final class NettyAsyncClientFactory {

  private NettyAsyncClientFactory() {}

  /**
   * Creates an async HTTP client using Netty NIO.
   *
   * @param customizer optional consumer to customize the builder (may be null)
   * @param config the Alternator configuration
   * @param tlsConfig the TLS configuration
   * @return a configured SdkAsyncHttpClient
   */
  public static SdkAsyncHttpClient create(
      Consumer<NettyNioAsyncHttpClient.Builder> customizer,
      AlternatorConfig config,
      TlsConfig tlsConfig) {
    NettyNioAsyncHttpClient.Builder builder = NettyNioAsyncHttpClient.builder();
    builder.tcpKeepAlive(true);

    // Apply Alternator-optimized defaults from config
    if (config != null) {
      if (config.getMaxConnections() > 0) {
        builder.maxConcurrency(config.getMaxConnections());
      }
      if (config.getConnectionMaxIdleTimeMs() > 0) {
        builder.connectionMaxIdleTime(Duration.ofMillis(config.getConnectionMaxIdleTimeMs()));
      }
      if (config.getConnectionTimeToLiveMs() > 0) {
        builder.connectionTimeToLive(Duration.ofMillis(config.getConnectionTimeToLiveMs()));
      }
    }

    // Apply TLS trust manager settings (not buildWithDefaults — that happens at build time)
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

    // Build with trust-all if configured, otherwise normal build
    if (tlsConfig != null && tlsConfig.isTrustAllCertificates()) {
      return builder.buildWithDefaults(
          AttributeMap.builder()
              .put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, true)
              .build());
    }
    return builder.build();
  }
}
