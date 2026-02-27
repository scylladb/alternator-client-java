package com.scylladb.alternator.internal;

import com.scylladb.alternator.AlternatorConfig;
import com.scylladb.alternator.TlsConfig;
import java.time.Duration;
import java.util.function.Consumer;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;

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

    // Apply user customizer
    if (customizer != null) {
      customizer.accept(builder);
    }

    // Apply TLS settings
    if (tlsConfig != null) {
      if (tlsConfig.isTrustAllCertificates()) {
        builder.tlsTrustManagersProvider(() -> TlsContextFactory.createTrustAllManagers());
      } else if (!tlsConfig.getCustomCaCertPaths().isEmpty()
          || !tlsConfig.isTrustSystemCaCerts()) {
        // Eagerly validate to fail fast on invalid cert paths
        javax.net.ssl.TrustManager[] trustManagers =
            TlsContextFactory.createTrustManagers(tlsConfig);
        builder.tlsTrustManagersProvider(() -> trustManagers);
      }
    }
    return builder.build();
  }
}
