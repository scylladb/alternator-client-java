package com.scylladb.alternator;

import com.scylladb.alternator.internal.AlternatorLiveNodes;
import com.scylladb.alternator.internal.ApacheSyncClientFactory;
import com.scylladb.alternator.internal.CrtSyncClientFactory;
import com.scylladb.alternator.internal.SyncClientDetector;
import com.scylladb.alternator.keyrouting.KeyRouteAffinity;
import com.scylladb.alternator.keyrouting.KeyRouteAffinityConfig;
import com.scylladb.alternator.queryplan.AffinityQueryPlanInterceptor;
import com.scylladb.alternator.queryplan.BasicQueryPlanInterceptor;
import com.scylladb.alternator.routing.RoutingScope;
import java.net.URI;
import java.util.Collection;
import java.util.function.Consumer;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.awscore.endpoints.AccountIdEndpointMode;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.crt.AwsCrtHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;
import software.amazon.awssdk.services.dynamodb.endpoints.DynamoDbEndpointProvider;

/**
 * Factory class for creating DynamoDB clients with Alternator load balancing support.
 *
 * <p>This class provides a builder that simplifies the construction of a DynamoDB client (AWS SDK
 * v2) that automatically distributes requests across all nodes in an Alternator cluster. It
 * provides a fluent API compatible with {@link DynamoDbClient#builder()} while automatically
 * integrating query plan interceptors for client-side load balancing.
 *
 * <p>The builder implements {@link DynamoDbClientBuilder}, ensuring compatibility with standard AWS
 * SDK v2 patterns while adding Alternator-specific configuration via {@link
 * AlternatorDynamoDbClientBuilder#withRoutingScope}.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * // Standard usage - returns DynamoDbClient
 * DynamoDbClient client = AlternatorDynamoDbClient.builder()
 *     .endpointOverride(URI.create("https://localhost:8043"))
 *     .credentialsProvider(credentialsProvider)
 *     .build();
 *
 * // With Alternator-specific APIs - returns AlternatorDynamoDbClientWrapper
 * AlternatorDynamoDbClientWrapper client = AlternatorDynamoDbClient.builder()
 *     .endpointOverride(URI.create("https://localhost:8043"))
 *     .credentialsProvider(credentialsProvider)
 *     .buildWithAlternatorAPI();
 *
 * // Access Alternator-specific functionality
 * List<URI> nodes = client.getLiveNodes();
 * URI nextNode = client.nextAsURI();
 * }</pre>
 *
 * @author dmitry.kropachev
 * @since 2.0.0
 */
public class AlternatorDynamoDbClient {

  /**
   * Creates a new builder instance using the standard builder pattern, similar to {@link
   * DynamoDbClient#builder()}.
   *
   * @return a new {@link AlternatorDynamoDbClientBuilder} configured for Alternator load balancing
   */
  public static AlternatorDynamoDbClientBuilder builder() {
    return new AlternatorDynamoDbClientBuilder();
  }

  /**
   * Builder implementation for constructing DynamoDB clients with Alternator load balancing.
   *
   * <p>This builder implements {@link DynamoDbClientBuilder} and delegates most configuration to
   * the standard AWS SDK {@link DynamoDbClient} builder while automatically integrating query plan
   * interceptors for node discovery and load balancing.
   *
   * <p>The builder tracks the seed URI (via {@link #endpointOverride(URI)}) and optional
   * datacenter/rack configuration, then creates the appropriate query plan interceptor during the
   * {@link #build()} phase.
   *
   * <p>Note: Some AWS-specific features are not supported by Alternator and will throw {@link
   * UnsupportedOperationException}, including endpoint discovery, FIPS mode, and dual-stack
   * networking.
   */
  public static class AlternatorDynamoDbClientBuilder implements DynamoDbClientBuilder {
    private final DynamoDbClientBuilder delegate;
    private final AlternatorConfig.Builder configBuilder;
    private URI seedUri;
    private Region region;
    private boolean disableCertificateChecks = false;
    private boolean httpClientSet = false;
    private boolean credentialsProviderSet = false;
    private Consumer<ApacheHttpClient.Builder> apacheCustomizer;
    private Consumer<AwsCrtHttpClient.Builder> crtCustomizer;

    private AlternatorDynamoDbClientBuilder() {
      this.delegate = DynamoDbClient.builder();
      this.configBuilder = AlternatorConfig.builder();
    }

    /**
     * Sets the routing scope for node targeting with fallback support.
     *
     * <p>The routing scope defines which nodes should be used for load balancing and provides
     * hierarchical fallback capabilities. Common usage patterns:
     *
     * <pre>{@code
     * // Target rack with fallback to datacenter and cluster
     * builder.withRoutingScope(RackScope.of("dc1", "rack1",
     *     DatacenterScope.of("dc1",
     *         ClusterScope.create())));
     *
     * // Target datacenter with fallback to cluster
     * builder.withRoutingScope(DatacenterScope.of("dc1", ClusterScope.create()));
     *
     * // Strict datacenter targeting (no fallback)
     * builder.withRoutingScope(DatacenterScope.of("dc1", null));
     * }</pre>
     *
     * @param routingScope the routing scope, or {@code null} to use all nodes
     * @return this builder instance
     * @since 2.0.0
     */
    public AlternatorDynamoDbClientBuilder withRoutingScope(RoutingScope routingScope) {
      configBuilder.withRoutingScope(routingScope);
      return this;
    }

    /**
     * Sets the request compression algorithm.
     *
     * <p>When a compression algorithm other than {@link RequestCompressionAlgorithm#NONE} is
     * specified, request bodies exceeding the minimum size threshold will be compressed.
     *
     * @param algorithm the compression algorithm to use, or null to disable compression
     * @return this builder instance
     */
    public AlternatorDynamoDbClientBuilder withCompressionAlgorithm(
        RequestCompressionAlgorithm algorithm) {
      configBuilder.withCompressionAlgorithm(algorithm);
      return this;
    }

    /**
     * Sets the minimum request body size (in bytes) that triggers compression.
     *
     * <p>Requests smaller than this threshold will not be compressed.
     *
     * @param minCompressionSizeBytes minimum request size in bytes, must be non-negative
     * @return this builder instance
     * @throws IllegalArgumentException if minCompressionSizeBytes is negative
     */
    public AlternatorDynamoDbClientBuilder withMinCompressionSizeBytes(
        int minCompressionSizeBytes) {
      configBuilder.withMinCompressionSizeBytes(minCompressionSizeBytes);
      return this;
    }

    /**
     * Enables or disables HTTP header optimization.
     *
     * <p>When enabled, outgoing requests will have their HTTP headers filtered to include only
     * those in the configured whitelist, reducing network traffic overhead.
     *
     * @param optimizeHeaders true to enable header filtering, false to disable
     * @return this builder instance
     */
    public AlternatorDynamoDbClientBuilder withOptimizeHeaders(boolean optimizeHeaders) {
      configBuilder.withOptimizeHeaders(optimizeHeaders);
      return this;
    }

    /**
     * Sets a custom whitelist of HTTP headers to preserve when optimization is enabled.
     *
     * @param headers collection of header names to preserve (case-insensitive)
     * @return this builder instance
     * @throws IllegalArgumentException if headers is null or empty
     */
    public AlternatorDynamoDbClientBuilder withHeadersWhitelist(Collection<String> headers) {
      configBuilder.withHeadersWhitelist(headers);
      return this;
    }

    /**
     * Sets the TLS session cache configuration for quick TLS renegotiation.
     *
     * <p>TLS session tickets (RFC 5077) allow clients to resume TLS sessions without performing a
     * full handshake. This significantly reduces latency when reconnecting to Alternator nodes.
     *
     * <p>Default: {@link TlsSessionCacheConfig#getDefault()} (enabled with 1024 sessions, 24h
     * timeout)
     *
     * @param tlsSessionCacheConfig the TLS session cache configuration, or null to use default
     * @return this builder instance
     * @since 2.0.0
     * @deprecated Use {@link #withTlsConfig(TlsConfig)} instead
     */
    @Deprecated
    public AlternatorDynamoDbClientBuilder withTlsSessionCacheConfig(
        TlsSessionCacheConfig tlsSessionCacheConfig) {
      configBuilder.withTlsSessionCacheConfig(tlsSessionCacheConfig);
      return this;
    }

    /**
     * Sets the TLS configuration for secure connections.
     *
     * <p>The TLS configuration controls certificate validation, custom CA certificates, hostname
     * verification, and session caching settings.
     *
     * @param tlsConfig the TLS configuration, or null to use default (trust-all)
     * @return this builder instance
     * @since 1.0.9
     */
    public AlternatorDynamoDbClientBuilder withTlsConfig(TlsConfig tlsConfig) {
      configBuilder.withTlsConfig(tlsConfig);
      return this;
    }

    /**
     * Sets the key route affinity configuration.
     *
     * @param keyRouteAffinityConfig the key route affinity configuration
     * @return this builder instance
     * @since 2.0.0
     */
    public AlternatorDynamoDbClientBuilder withKeyRouteAffinity(
        KeyRouteAffinityConfig keyRouteAffinityConfig) {
      configBuilder.withKeyRouteAffinity(keyRouteAffinityConfig);
      return this;
    }

    /**
     * Sets the key route affinity type with no pre-configured PK info.
     *
     * @param type the key route affinity type
     * @return this builder instance
     * @since 1.0.7
     */
    public AlternatorDynamoDbClientBuilder withKeyRouteAffinity(KeyRouteAffinity type) {
      configBuilder.withKeyRouteAffinity(type);
      return this;
    }

    /**
     * Sets the refresh interval for updating the node list when there are active requests.
     *
     * <p>Default: {@link AlternatorConfig#DEFAULT_ACTIVE_REFRESH_INTERVAL_MS} (1000ms / 1 second)
     *
     * @param intervalMs the refresh interval in milliseconds, must be positive
     * @return this builder instance
     * @since 1.0.8
     */
    public AlternatorDynamoDbClientBuilder withActiveRefreshIntervalMs(long intervalMs) {
      configBuilder.withActiveRefreshIntervalMs(intervalMs);
      return this;
    }

    /**
     * Sets the refresh interval for updating the node list when the client is idle.
     *
     * <p>Default: {@link AlternatorConfig#DEFAULT_IDLE_REFRESH_INTERVAL_MS} (60000ms / 1 minute)
     *
     * @param intervalMs the refresh interval in milliseconds, must be positive
     * @return this builder instance
     * @since 1.0.8
     */
    public AlternatorDynamoDbClientBuilder withIdleRefreshIntervalMs(long intervalMs) {
      configBuilder.withIdleRefreshIntervalMs(intervalMs);
      return this;
    }

    /**
     * Sets the maximum number of connections in the HTTP client connection pool.
     *
     * @param maxConnections the maximum number of connections, or 0 to use SDK default
     * @return this builder instance
     * @since 2.0.2
     */
    public AlternatorDynamoDbClientBuilder withMaxConnections(int maxConnections) {
      configBuilder.withMaxConnections(maxConnections);
      return this;
    }

    /**
     * Sets the maximum idle time for pooled connections in milliseconds.
     *
     * @param connectionMaxIdleTimeMs the maximum idle time in milliseconds, or 0 to use SDK default
     * @return this builder instance
     * @since 2.0.2
     */
    public AlternatorDynamoDbClientBuilder withConnectionMaxIdleTimeMs(
        long connectionMaxIdleTimeMs) {
      configBuilder.withConnectionMaxIdleTimeMs(connectionMaxIdleTimeMs);
      return this;
    }

    /**
     * Sets the maximum lifetime for pooled connections in milliseconds.
     *
     * @param connectionTimeToLiveMs the connection time-to-live in milliseconds, or 0 to use SDK
     *     default
     * @return this builder instance
     * @since 2.0.2
     */
    public AlternatorDynamoDbClientBuilder withConnectionTimeToLiveMs(long connectionTimeToLiveMs) {
      configBuilder.withConnectionTimeToLiveMs(connectionTimeToLiveMs);
      return this;
    }

    /**
     * Sets the complete Alternator configuration.
     *
     * @param config the Alternator configuration
     * @return this builder instance
     * @since 2.0.0
     * @deprecated Use individual builder methods instead (e.g., {@link #withRoutingScope}, {@link
     *     #withCompressionAlgorithm}, {@link #withTlsConfig}, etc.)
     */
    @Deprecated
    public AlternatorDynamoDbClientBuilder withAlternatorConfig(AlternatorConfig config) {
      if (config != null) {
        configBuilder.withRoutingScope(config.getRoutingScope());
        configBuilder.withCompressionAlgorithm(config.getCompressionAlgorithm());
        configBuilder.withMinCompressionSizeBytes(config.getMinCompressionSizeBytes());
        configBuilder.withOptimizeHeaders(config.isOptimizeHeaders());
        configBuilder.withHeadersWhitelist(config.getHeadersWhitelist());
        configBuilder.withTlsConfig(config.getTlsConfig());
        configBuilder.withTlsSessionCacheConfig(config.getTlsSessionCacheConfig());
        configBuilder.withKeyRouteAffinity(config.getKeyRouteAffinityConfig());
        configBuilder.withActiveRefreshIntervalMs(config.getActiveRefreshIntervalMs());
        configBuilder.withIdleRefreshIntervalMs(config.getIdleRefreshIntervalMs());
        configBuilder.withMaxConnections(config.getMaxConnections());
        configBuilder.withConnectionMaxIdleTimeMs(config.getConnectionMaxIdleTimeMs());
        configBuilder.withConnectionTimeToLiveMs(config.getConnectionTimeToLiveMs());
      }
      return this;
    }

    /**
     * Disables SSL certificate validation for testing purposes.
     *
     * <p><strong>WARNING:</strong> This should only be used for testing with self-signed
     * certificates. Never use this in production.
     *
     * @return this builder instance
     */
    public AlternatorDynamoDbClientBuilder withDisableCertificateChecks() {
      this.disableCertificateChecks = true;
      return this;
    }

    /**
     * Sets a customizer for the Apache HTTP client builder.
     *
     * <p>The customizer is called after Alternator-optimized defaults are applied, allowing users
     * to override specific settings. This is mutually exclusive with {@link
     * #withCrtHttpClientCustomizer} and {@link #httpClient(SdkHttpClient)}.
     *
     * <p>Example:
     *
     * <pre>{@code
     * DynamoDbClient client = AlternatorDynamoDbClient.builder()
     *     .endpointOverride(URI.create("https://localhost:8043"))
     *     .withApacheHttpClientCustomizer(builder -> builder
     *         .maxConnections(200)
     *         .connectionTimeout(Duration.ofSeconds(5)))
     *     .build();
     * }</pre>
     *
     * @param customizer a consumer that customizes the Apache HTTP client builder
     * @return this builder instance
     * @since 2.1.0
     */
    public AlternatorDynamoDbClientBuilder withApacheHttpClientCustomizer(
        Consumer<ApacheHttpClient.Builder> customizer) {
      this.apacheCustomizer = customizer;
      return this;
    }

    /**
     * Sets a customizer for the AWS CRT HTTP client builder.
     *
     * <p>The customizer is called after Alternator-optimized defaults are applied, allowing users
     * to override specific settings. This is mutually exclusive with {@link
     * #withApacheHttpClientCustomizer} and {@link #httpClient(SdkHttpClient)}.
     *
     * @param customizer a consumer that customizes the CRT HTTP client builder
     * @return this builder instance
     * @since 2.1.0
     */
    public AlternatorDynamoDbClientBuilder withCrtHttpClientCustomizer(
        Consumer<AwsCrtHttpClient.Builder> customizer) {
      this.crtCustomizer = customizer;
      return this;
    }

    /** {@inheritDoc} */
    @Override
    public AlternatorDynamoDbClientBuilder region(Region region) {
      this.region = region;
      delegate.region(region);
      return this;
    }

    /** {@inheritDoc} */
    @Override
    public AlternatorDynamoDbClientBuilder credentialsProvider(
        AwsCredentialsProvider credentialsProvider) {
      if (credentialsProvider != null) {
        this.credentialsProviderSet = true;
        configBuilder.authenticationEnabled(true);
      }
      delegate.credentialsProvider(credentialsProvider);
      return this;
    }

    /** {@inheritDoc} */
    @Override
    public AlternatorDynamoDbClientBuilder endpointOverride(URI endpointOverride) {
      this.seedUri = endpointOverride;
      return this;
    }

    /** {@inheritDoc} */
    @Override
    public AlternatorDynamoDbClientBuilder httpClient(SdkHttpClient httpClient) {
      this.httpClientSet = true;
      delegate.httpClient(httpClient);
      return this;
    }

    /** {@inheritDoc} */
    @Override
    public AlternatorDynamoDbClientBuilder httpClientBuilder(
        SdkHttpClient.Builder httpClientBuilder) {
      this.httpClientSet = true;
      delegate.httpClientBuilder(httpClientBuilder);
      return this;
    }

    /** {@inheritDoc} */
    @Override
    public ClientOverrideConfiguration overrideConfiguration() {
      return delegate.overrideConfiguration();
    }

    /** {@inheritDoc} */
    @Override
    public AlternatorDynamoDbClientBuilder overrideConfiguration(
        ClientOverrideConfiguration overrideConfiguration) {
      delegate.overrideConfiguration(overrideConfiguration);
      return this;
    }

    /** {@inheritDoc} */
    @Override
    public AlternatorDynamoDbClientBuilder overrideConfiguration(
        Consumer<ClientOverrideConfiguration.Builder> builderConsumer) {
      delegate.overrideConfiguration(builderConsumer);
      return this;
    }

    /** {@inheritDoc} */
    @Override
    public AlternatorDynamoDbClientBuilder endpointProvider(
        DynamoDbEndpointProvider endpointProvider) {
      throw new UnsupportedOperationException(
          "AlternatorDynamoDbClient does not support custom endpoint providers. "
              + "Use endpointOverride(URI) to specify the seed endpoint instead.");
    }

    /** {@inheritDoc} */
    @Override
    public AlternatorDynamoDbClientBuilder endpointDiscoveryEnabled(
        boolean endpointDiscoveryEnabled) {
      throw new UnsupportedOperationException(
          "AlternatorDynamoDbClient does not support AWS endpoint discovery. "
              + "Node discovery is handled automatically via the /localnodes API.");
    }

    /** {@inheritDoc} */
    @Override
    public AlternatorDynamoDbClientBuilder enableEndpointDiscovery() {
      throw new UnsupportedOperationException(
          "AlternatorDynamoDbClient does not support AWS endpoint discovery. "
              + "Node discovery is handled automatically via the /localnodes API.");
    }

    /** {@inheritDoc} */
    @Override
    public AlternatorDynamoDbClientBuilder fipsEnabled(Boolean fipsEnabled) {
      throw new UnsupportedOperationException(
          "AlternatorDynamoDbClient does not support FIPS mode (AWS-specific feature).");
    }

    /** {@inheritDoc} */
    @Override
    public AlternatorDynamoDbClientBuilder dualstackEnabled(Boolean dualstackEnabled) {
      throw new UnsupportedOperationException(
          "AlternatorDynamoDbClient does not support dual-stack networking "
              + "(AWS-specific feature).");
    }

    /** {@inheritDoc} */
    @Override
    public AlternatorDynamoDbClientBuilder accountIdEndpointMode(
        AccountIdEndpointMode accountIdEndpointMode) {
      delegate.accountIdEndpointMode(accountIdEndpointMode);
      return this;
    }

    /** {@inheritDoc} */
    @Override
    public DynamoDbClient build() {
      return buildWithAlternatorAPI().getClient();
    }

    /**
     * Builds and returns a DynamoDB client wrapper with Alternator load balancing and access to
     * Alternator-specific APIs.
     *
     * @return an {@link AlternatorDynamoDbClientWrapper} instance
     * @throws IllegalStateException if {@link #endpointOverride(URI)} was not called, or if
     *     conflicting HTTP client options are set
     */
    public AlternatorDynamoDbClientWrapper buildWithAlternatorAPI() {
      if (seedUri == null) {
        throw new IllegalStateException(
            "endpointOverride must be set when using AlternatorDynamoDbClientBuilder. "
                + "Call endpointOverride(URI) with the seed Alternator node URI.");
      }

      // Validate mutually exclusive options
      validateHttpClientOptions();

      // Use anonymous credentials if no credentials were provided
      if (!credentialsProviderSet) {
        configBuilder.authenticationEnabled(false);
        delegate.credentialsProvider(AnonymousCredentialsProvider.create());
      }

      // Set the seed node on the config
      configBuilder.withSeedNode(seedUri);

      // If disableCertificateChecks was called, set trust-all TLS config
      if (disableCertificateChecks) {
        configBuilder.withTlsConfig(TlsConfig.trustAll());
      }

      // Build the AlternatorConfig from the internal builder
      AlternatorConfig alternatorConfig = configBuilder.build();

      // Apply SDK-level interceptors from config (compression only)
      if (alternatorConfig.getCompressionAlgorithm().isEnabled()) {
        ClientOverrideConfiguration.Builder overrideBuilder =
            delegate.overrideConfiguration() != null
                ? delegate.overrideConfiguration().toBuilder()
                : ClientOverrideConfiguration.builder();
        overrideBuilder.addExecutionInterceptor(
            new GzipRequestInterceptor(alternatorConfig.getMinCompressionSizeBytes()));
        delegate.overrideConfiguration(overrideBuilder.build());
      }

      TlsConfig tlsConfig = alternatorConfig.getTlsConfig();
      boolean optimizeHeaders = alternatorConfig.isOptimizeHeaders();

      // Create the HTTP client for the SDK
      SdkHttpClient pollingClient = null;
      if (!httpClientSet) {
        // Determine which sync implementation to use
        SyncClientDetector.SyncClientType clientType = detectSyncClientType();

        // Create the main SDK HTTP client via factory
        SdkHttpClient mainClient = createMainSyncClient(clientType, alternatorConfig, tlsConfig);

        // Wrap with header filtering if enabled
        if (optimizeHeaders) {
          delegate.httpClient(
              new HeadersFilteringSdkHttpClient(
                  mainClient, alternatorConfig.getHeadersWhitelist()));
        } else {
          delegate.httpClient(mainClient);
        }

        // Create polling client for LiveNodes
        pollingClient = SyncClientDetector.createPollingClient(clientType, tlsConfig);
      } else {
        // User provided custom HTTP client - still need a polling client for LiveNodes
        SyncClientDetector.SyncClientType clientType = SyncClientDetector.detect();
        pollingClient = SyncClientDetector.createPollingClient(clientType, tlsConfig);
      }

      // Create AlternatorLiveNodes and start node discovery
      AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(alternatorConfig, pollingClient);
      liveNodes.start();

      // Add query plan interceptor
      ClientOverrideConfiguration.Builder overrideBuilder =
          delegate.overrideConfiguration() != null
              ? delegate.overrideConfiguration().toBuilder()
              : ClientOverrideConfiguration.builder();

      AffinityQueryPlanInterceptor affinityInterceptor = null;
      KeyRouteAffinityConfig keyAffinityConfig = alternatorConfig.getKeyRouteAffinityConfig();
      if (keyAffinityConfig != null
          && keyAffinityConfig.getType() != null
          && keyAffinityConfig.getType() != KeyRouteAffinity.NONE) {
        affinityInterceptor = new AffinityQueryPlanInterceptor(keyAffinityConfig, liveNodes);
        overrideBuilder.addExecutionInterceptor(affinityInterceptor);
      } else {
        overrideBuilder.addExecutionInterceptor(new BasicQueryPlanInterceptor(liveNodes));
      }
      delegate.overrideConfiguration(overrideBuilder.build());

      // Use seed URI as base endpoint - interceptor will override with actual target node
      delegate.endpointOverride(seedUri);

      // Set default region if not specified
      if (region == null) {
        delegate.region(Region.of("fake-aws-region"));
      }

      // Build the underlying client and wrap it
      DynamoDbClient client = delegate.build();
      return new AlternatorDynamoDbClientWrapper(
          client, liveNodes, alternatorConfig, affinityInterceptor, pollingClient);
    }

    private void validateHttpClientOptions() {
      boolean hasCustomizer = apacheCustomizer != null || crtCustomizer != null;
      if (httpClientSet && hasCustomizer) {
        throw new IllegalStateException(
            "Cannot use httpClient()/httpClientBuilder() together with "
                + "withApacheHttpClientCustomizer()/withCrtHttpClientCustomizer(). "
                + "Use one approach or the other.");
      }
      if (apacheCustomizer != null && crtCustomizer != null) {
        throw new IllegalStateException(
            "Cannot set both withApacheHttpClientCustomizer() and "
                + "withCrtHttpClientCustomizer(). Choose one HTTP client implementation.");
      }
    }

    private SyncClientDetector.SyncClientType detectSyncClientType() {
      // If a customizer was set, use the corresponding type
      if (apacheCustomizer != null) {
        return SyncClientDetector.SyncClientType.APACHE;
      }
      if (crtCustomizer != null) {
        return SyncClientDetector.SyncClientType.CRT;
      }
      // Auto-detect from classpath
      return SyncClientDetector.detect();
    }

    private SdkHttpClient createMainSyncClient(
        SyncClientDetector.SyncClientType clientType,
        AlternatorConfig config,
        TlsConfig tlsConfig) {
      switch (clientType) {
        case APACHE:
          return ApacheSyncClientFactory.create(apacheCustomizer, config, tlsConfig);
        case CRT:
          return CrtSyncClientFactory.create(crtCustomizer, config, tlsConfig);
        default:
          throw new IllegalStateException("Unknown sync client type: " + clientType);
      }
    }
  }
}
