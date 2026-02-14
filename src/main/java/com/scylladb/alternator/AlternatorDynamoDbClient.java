package com.scylladb.alternator;

import com.scylladb.alternator.internal.AlternatorLiveNodes;
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
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpConfigurationOption;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;
import software.amazon.awssdk.services.dynamodb.endpoints.DynamoDbEndpointProvider;
import software.amazon.awssdk.utils.AttributeMap;

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
     * <p>Example:
     *
     * <pre>{@code
     * // Custom TLS session cache configuration
     * DynamoDbClient client = AlternatorDynamoDbClient.builder()
     *     .endpointOverride(URI.create("https://localhost:8043"))
     *     .credentialsProvider(credentialsProvider)
     *     .withTlsSessionCacheConfig(TlsSessionCacheConfig.builder()
     *         .withSessionCacheSize(200)
     *         .withSessionTimeoutSeconds(3600)
     *         .build())
     *     .build();
     *
     * // Disable TLS session caching
     * DynamoDbClient client = AlternatorDynamoDbClient.builder()
     *     .endpointOverride(URI.create("https://localhost:8043"))
     *     .credentialsProvider(credentialsProvider)
     *     .withTlsSessionCacheConfig(TlsSessionCacheConfig.disabled())
     *     .build();
     * }</pre>
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
     * <p>Example:
     *
     * <pre>{@code
     * // Use system CA certificates (recommended for production)
     * DynamoDbClient client = AlternatorDynamoDbClient.builder()
     *     .endpointOverride(URI.create("https://localhost:8043"))
     *     .credentialsProvider(credentialsProvider)
     *     .withTlsConfig(TlsConfig.systemDefault())
     *     .build();
     *
     * // Use custom CA certificate
     * DynamoDbClient client = AlternatorDynamoDbClient.builder()
     *     .endpointOverride(URI.create("https://localhost:8043"))
     *     .credentialsProvider(credentialsProvider)
     *     .withTlsConfig(TlsConfig.builder()
     *         .withCaCertPath(Paths.get("/path/to/ca.pem"))
     *         .withTrustSystemCaCerts(false)
     *         .build())
     *     .build();
     *
     * // Trust all certificates (development/testing only)
     * DynamoDbClient client = AlternatorDynamoDbClient.builder()
     *     .endpointOverride(URI.create("https://localhost:8043"))
     *     .credentialsProvider(credentialsProvider)
     *     .withTlsConfig(TlsConfig.trustAll())
     *     .build();
     * }</pre>
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
     * <p>Key route affinity ensures that all requests for the same partition key are routed to the
     * same Alternator node, which improves performance for Lightweight Transactions (LWT) that use
     * Paxos consensus.
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
     * <p>This is a convenience method for configuring key route affinity without providing
     * pre-configured partition key information. Partition key names will be auto-discovered via
     * DescribeTable API on first access to each table.
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
     * Sets the complete Alternator configuration.
     *
     * <p>This method allows setting all Alternator-specific configuration options at once using a
     * pre-built {@link AlternatorConfig} instance. Any settings specified via individual builder
     * methods (like {@link #withRoutingScope}, {@link #withCompressionAlgorithm}, etc.) will be
     * overwritten by the config.
     *
     * <p>Example:
     *
     * <pre>{@code
     * AlternatorConfig config = AlternatorConfig.builder()
     *     .withSeedNode(URI.create("https://localhost:8043"))
     *     .withRoutingScope(DatacenterScope.of("dc1", ClusterScope.create()))
     *     .withCompressionAlgorithm(RequestCompressionAlgorithm.GZIP)
     *     .withTlsSessionCacheConfig(TlsSessionCacheConfig.getDefault())
     *     .build();
     *
     * DynamoDbClient client = AlternatorDynamoDbClient.builder()
     *     .endpointOverride(URI.create("https://localhost:8043"))
     *     .credentialsProvider(credentialsProvider)
     *     .withAlternatorConfig(config)
     *     .build();
     * }</pre>
     *
     * @param config the Alternator configuration
     * @return this builder instance
     * @since 2.0.0
     */
    public AlternatorDynamoDbClientBuilder withAlternatorConfig(AlternatorConfig config) {
      if (config != null) {
        configBuilder.withRoutingScope(config.getRoutingScope());
        configBuilder.withCompressionAlgorithm(config.getCompressionAlgorithm());
        configBuilder.withMinCompressionSizeBytes(config.getMinCompressionSizeBytes());
        configBuilder.withOptimizeHeaders(config.isOptimizeHeaders());
        configBuilder.withHeadersWhitelist(config.getHeadersWhitelist());
        configBuilder.withTlsConfig(config.getTlsConfig());
        configBuilder.withKeyRouteAffinity(config.getKeyRouteAffinityConfig());
        configBuilder.withActiveRefreshIntervalMs(config.getActiveRefreshIntervalMs());
        configBuilder.withIdleRefreshIntervalMs(config.getIdleRefreshIntervalMs());
      }
      return this;
    }

    /**
     * Disables SSL certificate validation for testing purposes.
     *
     * <p><strong>WARNING:</strong> This should only be used for testing with self-signed
     * certificates. Never use this in production as it makes connections vulnerable to
     * man-in-the-middle attacks.
     *
     * <p>This method configures the HTTP client to trust all certificates. If you've already set a
     * custom HTTP client via {@link #httpClient(SdkHttpClient)} or {@link
     * #httpClientBuilder(SdkHttpClient.Builder)}, this method will have no effect.
     *
     * @return this builder instance
     */
    public AlternatorDynamoDbClientBuilder withDisableCertificateChecks() {
      this.disableCertificateChecks = true;
      return this;
    }

    /**
     * Sets the AWS region. This method matches {@link DynamoDbClientBuilder#region(Region)}.
     *
     * <p>Note: The region is not used by Alternator for routing (the endpoint provider handles
     * that), but it is required by the AWS SDK and may appear in logs, traces, or metrics. If not
     * specified, a default "fake-aws-region" will be used.
     *
     * @param region the AWS region
     * @return this builder instance
     */
    @Override
    public AlternatorDynamoDbClientBuilder region(Region region) {
      this.region = region;
      delegate.region(region);
      return this;
    }

    /**
     * Sets the AWS credentials provider. This method matches {@link
     * DynamoDbClientBuilder#credentialsProvider(AwsCredentialsProvider)}.
     *
     * <p>The credentials are used for authentication with Alternator. Common providers include:
     *
     * <ul>
     *   <li>{@link software.amazon.awssdk.auth.credentials.StaticCredentialsProvider} for hardcoded
     *       credentials
     *   <li>{@link software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider} for
     *       environment-based credentials
     * </ul>
     *
     * <p>If no credentials provider is set, the client will automatically use anonymous credentials
     * and exclude authentication headers when header optimization is enabled.
     *
     * @param credentialsProvider the AWS credentials provider
     * @return this builder instance
     */
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

    /**
     * Sets the endpoint override (seed URI) for Alternator cluster discovery.
     *
     * <p>This is the initial Alternator node that will be contacted to discover all other nodes in
     * the cluster via the {@code /localnodes} API. Once the cluster topology is discovered,
     * requests will be distributed across all discovered nodes using round-robin load balancing.
     *
     * <p>The endpoint should be a complete URI including protocol and port, for example:
     *
     * <ul>
     *   <li>{@code https://localhost:8043} for HTTPS
     *   <li>{@code http://192.168.1.100:8000} for HTTP
     * </ul>
     *
     * <p>This method is required - the build will fail if no endpoint is specified.
     *
     * @param endpointOverride the seed URI for Alternator cluster discovery
     * @return this builder instance
     */
    @Override
    public AlternatorDynamoDbClientBuilder endpointOverride(URI endpointOverride) {
      this.seedUri = endpointOverride;
      return this;
    }

    /**
     * Sets a custom HTTP client. This method matches {@link
     * DynamoDbClientBuilder#httpClient(SdkHttpClient)}.
     *
     * <p>Use this to configure custom HTTP client settings such as connection pooling, timeouts, or
     * proxy settings. If not specified, the AWS SDK will create a default HTTP client.
     *
     * @param httpClient the HTTP client to use
     * @return this builder instance
     */
    @Override
    public AlternatorDynamoDbClientBuilder httpClient(SdkHttpClient httpClient) {
      this.httpClientSet = true;
      delegate.httpClient(httpClient);
      return this;
    }

    /**
     * Sets a custom HTTP client builder. This method matches {@link
     * DynamoDbClientBuilder#httpClientBuilder(SdkHttpClient.Builder)}.
     *
     * <p>Use this to configure HTTP client settings using a builder pattern. This is an alternative
     * to {@link #httpClient(SdkHttpClient)} when you want to customize the HTTP client
     * configuration without creating the client instance directly.
     *
     * @param httpClientBuilder the HTTP client builder to use
     * @return this builder instance
     */
    @Override
    public AlternatorDynamoDbClientBuilder httpClientBuilder(
        SdkHttpClient.Builder httpClientBuilder) {
      this.httpClientSet = true;
      delegate.httpClientBuilder(httpClientBuilder);
      return this;
    }

    /**
     * Returns the current client override configuration.
     *
     * @return the current {@link ClientOverrideConfiguration}, or null if not set
     */
    @Override
    public ClientOverrideConfiguration overrideConfiguration() {
      return delegate.overrideConfiguration();
    }

    /**
     * Sets client override configuration. This method matches {@link
     * DynamoDbClientBuilder#overrideConfiguration(ClientOverrideConfiguration)}.
     *
     * <p>Use this to configure advanced client settings such as:
     *
     * <ul>
     *   <li>Retry policies
     *   <li>Request timeout settings
     *   <li>Additional request headers
     *   <li>Metric publishers
     * </ul>
     *
     * @param overrideConfiguration the client override configuration
     * @return this builder instance
     */
    @Override
    public AlternatorDynamoDbClientBuilder overrideConfiguration(
        ClientOverrideConfiguration overrideConfiguration) {
      delegate.overrideConfiguration(overrideConfiguration);
      return this;
    }

    /**
     * Sets client override configuration using a builder consumer. This method matches {@link
     * DynamoDbClientBuilder#overrideConfiguration(Consumer)}.
     *
     * <p>This is a convenience method that allows configuring the override settings inline without
     * creating a separate {@link ClientOverrideConfiguration} instance.
     *
     * <p>Example:
     *
     * <pre>{@code
     * builder.overrideConfiguration(c -> c
     *     .apiCallTimeout(Duration.ofSeconds(10))
     *     .retryPolicy(RetryPolicy.builder().numRetries(3).build())
     * )
     * }</pre>
     *
     * @param builderConsumer a consumer that configures the override configuration builder
     * @return this builder instance
     */
    @Override
    public AlternatorDynamoDbClientBuilder overrideConfiguration(
        Consumer<ClientOverrideConfiguration.Builder> builderConsumer) {
      delegate.overrideConfiguration(builderConsumer);
      return this;
    }

    /**
     * This method is not supported by AlternatorDynamoDbClient.
     *
     * <p>The endpoint routing is automatically handled by query plan interceptors for load
     * balancing. Use {@link #endpointOverride(URI)} to specify the seed endpoint instead.
     *
     * @param endpointProvider ignored
     * @return never returns
     * @throws UnsupportedOperationException always thrown
     */
    @Override
    public AlternatorDynamoDbClientBuilder endpointProvider(
        DynamoDbEndpointProvider endpointProvider) {
      throw new UnsupportedOperationException(
          "AlternatorDynamoDbClient does not support custom endpoint providers. "
              + "Use endpointOverride(URI) to specify the seed endpoint instead.");
    }

    /**
     * This method is not supported by AlternatorDynamoDbClient.
     *
     * <p>Alternator uses its own node discovery mechanism via the {@code /localnodes} API, which is
     * incompatible with AWS endpoint discovery.
     *
     * @param endpointDiscoveryEnabled ignored
     * @return never returns
     * @throws UnsupportedOperationException always thrown
     */
    @Override
    public AlternatorDynamoDbClientBuilder endpointDiscoveryEnabled(
        boolean endpointDiscoveryEnabled) {
      throw new UnsupportedOperationException(
          "AlternatorDynamoDbClient does not support AWS endpoint discovery. "
              + "Node discovery is handled automatically via the /localnodes API.");
    }

    /**
     * This method is not supported by AlternatorDynamoDbClient.
     *
     * <p>Alternator uses its own node discovery mechanism via the {@code /localnodes} API, which is
     * incompatible with AWS endpoint discovery.
     *
     * @return never returns
     * @throws UnsupportedOperationException always thrown
     */
    @Override
    public AlternatorDynamoDbClientBuilder enableEndpointDiscovery() {
      throw new UnsupportedOperationException(
          "AlternatorDynamoDbClient does not support AWS endpoint discovery. "
              + "Node discovery is handled automatically via the /localnodes API.");
    }

    /**
     * This method is not supported by AlternatorDynamoDbClient.
     *
     * <p>FIPS (Federal Information Processing Standards) mode is an AWS-specific feature that is
     * not applicable to Alternator.
     *
     * @param fipsEnabled ignored
     * @return never returns
     * @throws UnsupportedOperationException always thrown
     */
    @Override
    public AlternatorDynamoDbClientBuilder fipsEnabled(Boolean fipsEnabled) {
      throw new UnsupportedOperationException(
          "AlternatorDynamoDbClient does not support FIPS mode (AWS-specific feature).");
    }

    /**
     * This method is not supported by AlternatorDynamoDbClient.
     *
     * <p>Dual-stack networking (IPv4/IPv6) is an AWS-specific feature that is not applicable to
     * Alternator.
     *
     * @param dualstackEnabled ignored
     * @return never returns
     * @throws UnsupportedOperationException always thrown
     */
    @Override
    public AlternatorDynamoDbClientBuilder dualstackEnabled(Boolean dualstackEnabled) {
      throw new UnsupportedOperationException(
          "AlternatorDynamoDbClient does not support dual-stack networking (AWS-specific feature).");
    }

    /**
     * Builds and returns a DynamoDB client with Alternator load balancing configured.
     *
     * <p>This method performs the following steps:
     *
     * <ol>
     *   <li>Validates that {@link #endpointOverride(URI)} was called (required)
     *   <li>Initializes {@link AlternatorConfig} with default values if not configured
     *   <li>Creates a query plan interceptor with the seed URI and routing scope
     *   <li>Sets a default region ("fake-aws-region") if none was specified
     *   <li>Builds the underlying {@link DynamoDbClient} with all configurations applied
     * </ol>
     *
     * <p>The returned client will automatically:
     *
     * <ul>
     *   <li>Discover all nodes in the Alternator cluster via the {@code /localnodes} API
     *   <li>Distribute requests across discovered nodes using load balancing
     *   <li>Periodically refresh the node list (every 5 seconds) to handle topology changes
     *   <li>Filter nodes by routing scope if configured via {@link #withRoutingScope}
     * </ul>
     *
     * <p>If you need access to Alternator-specific APIs (such as {@code getLiveNodes()} or {@code
     * nextAsURI()}), use {@link #buildWithAlternatorAPI()} instead.
     *
     * @return a {@link DynamoDbClient} instance configured with Alternator load balancing
     * @throws IllegalStateException if {@link #endpointOverride(URI)} was not called
     */
    @Override
    public DynamoDbClient build() {
      return buildWithAlternatorAPI().getClient();
    }

    /**
     * Builds and returns a DynamoDB client wrapper with Alternator load balancing and access to
     * Alternator-specific APIs.
     *
     * <p>This method is similar to {@link #build()}, but returns an {@link
     * AlternatorDynamoDbClientWrapper} that provides additional methods for accessing
     * Alternator-specific functionality:
     *
     * <ul>
     *   <li>{@link AlternatorDynamoDbClientWrapper#getLiveNodes()} - Get current live nodes
     *   <li>{@link AlternatorDynamoDbClientWrapper#nextAsURI()} - Get next node in round-robin
     *   <li>{@link AlternatorDynamoDbClientWrapper#checkIfRackDatacenterFeatureIsSupported()} -
     *       Check server capabilities
     *   <li>{@link AlternatorDynamoDbClientWrapper#getAlternatorLiveNodes()} - Access the live
     *       nodes manager
     * </ul>
     *
     * @return an {@link AlternatorDynamoDbClientWrapper} instance configured with Alternator load
     *     balancing
     * @throws IllegalStateException if {@link #endpointOverride(URI)} was not called
     */
    public AlternatorDynamoDbClientWrapper buildWithAlternatorAPI() {
      if (seedUri == null) {
        throw new IllegalStateException(
            "endpointOverride must be set when using AlternatorDynamoDbClientBuilder. "
                + "Call endpointOverride(URI) with the seed Alternator node URI.");
      }

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

      // Determine if we need custom SSL configuration for the SDK HTTP client
      TlsConfig tlsConfig = alternatorConfig.getTlsConfig();
      boolean needsTrustAll = tlsConfig.isTrustAllCertificates();

      // Configure HTTP client with optional certificate checking and header filtering
      if (httpClientSet && (needsTrustAll || alternatorConfig.isOptimizeHeaders())) {
        throw new IllegalStateException(
            "Cannot use custom HTTP client with trustAllCertificates or optimizeHeaders. "
                + "These options require configuring the HTTP client internally.");
      }
      if (!httpClientSet && (needsTrustAll || alternatorConfig.isOptimizeHeaders())) {
        // Build the base HTTP client
        SdkHttpClient baseHttpClient;
        if (needsTrustAll) {
          baseHttpClient =
              ApacheHttpClient.builder()
                  .buildWithDefaults(
                      AttributeMap.builder()
                          .put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, true)
                          .build());
        } else {
          baseHttpClient = ApacheHttpClient.builder().build();
        }

        // Wrap with header filtering if enabled
        if (alternatorConfig.isOptimizeHeaders()) {
          delegate.httpClient(
              new HeadersFilteringSdkHttpClient(
                  baseHttpClient, alternatorConfig.getHeadersWhitelist()));
        } else {
          delegate.httpClient(baseHttpClient);
        }
      }

      // Create AlternatorLiveNodes and start node discovery
      // Fallback is handled automatically at runtime based on the routing scope's fallback chain
      AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(alternatorConfig);
      liveNodes.start();

      // Add query plan interceptor - use affinity interceptor if configured, otherwise basic
      // The interceptor handles all routing via modifyHttpRequest()
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

      // Set default region if not specified (required by AWS SDK but not used by Alternator)
      if (region == null) {
        delegate.region(Region.of("fake-aws-region"));
      }

      // Build the underlying client and wrap it with Alternator metadata
      DynamoDbClient client = delegate.build();
      return new AlternatorDynamoDbClientWrapper(
          client, liveNodes, alternatorConfig, affinityInterceptor);
    }
  }
}
