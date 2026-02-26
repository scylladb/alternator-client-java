package com.scylladb.alternator;

import com.scylladb.alternator.internal.AlternatorLiveNodes;
import com.scylladb.alternator.keyrouting.KeyRouteAffinity;
import com.scylladb.alternator.keyrouting.KeyRouteAffinityConfig;
import com.scylladb.alternator.queryplan.AffinityQueryPlanInterceptor;
import com.scylladb.alternator.queryplan.BasicQueryPlanInterceptor;
import com.scylladb.alternator.routing.RoutingScope;
import java.net.URI;
import java.time.Duration;
import java.util.Collection;
import java.util.function.Consumer;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.awscore.endpoints.AccountIdEndpointMode;
import software.amazon.awssdk.core.client.config.ClientAsyncConfiguration;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.internal.http.loader.DefaultSdkAsyncHttpClientBuilder;
import software.amazon.awssdk.http.SdkHttpConfigurationOption;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClientBuilder;
import software.amazon.awssdk.services.dynamodb.endpoints.DynamoDbEndpointProvider;
import software.amazon.awssdk.utils.AttributeMap;

/**
 * Factory class for creating DynamoDB async clients with Alternator load balancing support.
 *
 * <p>This class provides a builder that simplifies the construction of an async DynamoDB client
 * (AWS SDK v2) that automatically distributes requests across all nodes in an Alternator cluster.
 * It provides a fluent API compatible with {@link DynamoDbAsyncClient#builder()} while
 * automatically integrating query plan interceptors for client-side load balancing.
 *
 * <p>The builder implements {@link DynamoDbAsyncClientBuilder}, ensuring compatibility with
 * standard AWS SDK v2 patterns while adding Alternator-specific configuration via {@link
 * AlternatorDynamoDbAsyncClientBuilder#withRoutingScope}.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * // Standard usage - returns DynamoDbAsyncClient
 * DynamoDbAsyncClient client = AlternatorDynamoDbAsyncClient.builder()
 *     .endpointOverride(URI.create("https://localhost:8043"))
 *     .credentialsProvider(credentialsProvider)
 *     .build();
 *
 * // With Alternator-specific APIs - returns AlternatorDynamoDbAsyncClientWrapper
 * AlternatorDynamoDbAsyncClientWrapper client = AlternatorDynamoDbAsyncClient.builder()
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
public class AlternatorDynamoDbAsyncClient {

  /**
   * Creates a new builder instance using the standard builder pattern, similar to {@link
   * DynamoDbAsyncClient#builder()}.
   *
   * @return a new {@link AlternatorDynamoDbAsyncClientBuilder} configured for Alternator load
   *     balancing
   */
  public static AlternatorDynamoDbAsyncClientBuilder builder() {
    return new AlternatorDynamoDbAsyncClientBuilder();
  }

  /**
   * Builder implementation for constructing async DynamoDB clients with Alternator load balancing.
   *
   * <p>This builder implements {@link DynamoDbAsyncClientBuilder} and delegates most configuration
   * to the standard AWS SDK {@link DynamoDbAsyncClient} builder while automatically integrating
   * query plan interceptors for node discovery and load balancing.
   *
   * <p>The builder tracks the seed URI (via {@link #endpointOverride(URI)}) and optional
   * datacenter/rack configuration, then creates the appropriate query plan interceptor during the
   * {@link #build()} phase.
   *
   * <p>Note: Some AWS-specific features are not supported by Alternator and will throw {@link
   * UnsupportedOperationException}, including endpoint discovery, FIPS mode, and dual-stack
   * networking.
   */
  public static class AlternatorDynamoDbAsyncClientBuilder implements DynamoDbAsyncClientBuilder {
    private final DynamoDbAsyncClientBuilder delegate;
    private final AlternatorConfig.Builder configBuilder;
    private URI seedUri;
    private Region region;
    private boolean disableCertificateChecks = false;
    private boolean httpClientSet = false;
    private boolean credentialsProviderSet = false;

    private AlternatorDynamoDbAsyncClientBuilder() {
      this.delegate = DynamoDbAsyncClient.builder();
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
    public AlternatorDynamoDbAsyncClientBuilder withRoutingScope(RoutingScope routingScope) {
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
    public AlternatorDynamoDbAsyncClientBuilder withCompressionAlgorithm(
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
    public AlternatorDynamoDbAsyncClientBuilder withMinCompressionSizeBytes(
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
    public AlternatorDynamoDbAsyncClientBuilder withOptimizeHeaders(boolean optimizeHeaders) {
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
    public AlternatorDynamoDbAsyncClientBuilder withHeadersWhitelist(Collection<String> headers) {
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
    public AlternatorDynamoDbAsyncClientBuilder withTlsSessionCacheConfig(
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
     * DynamoDbAsyncClient client = AlternatorDynamoDbAsyncClient.builder()
     *     .endpointOverride(URI.create("https://localhost:8043"))
     *     .credentialsProvider(credentialsProvider)
     *     .withTlsConfig(TlsConfig.systemDefault())
     *     .build();
     *
     * // Use custom CA certificate
     * DynamoDbAsyncClient client = AlternatorDynamoDbAsyncClient.builder()
     *     .endpointOverride(URI.create("https://localhost:8043"))
     *     .credentialsProvider(credentialsProvider)
     *     .withTlsConfig(TlsConfig.builder()
     *         .withCaCertPath(Paths.get("/path/to/ca.pem"))
     *         .withTrustSystemCaCerts(false)
     *         .build())
     *     .build();
     *
     * // Trust all certificates (development/testing only)
     * DynamoDbAsyncClient client = AlternatorDynamoDbAsyncClient.builder()
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
    public AlternatorDynamoDbAsyncClientBuilder withTlsConfig(TlsConfig tlsConfig) {
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
     * @since 1.0.7
     */
    public AlternatorDynamoDbAsyncClientBuilder withKeyRouteAffinity(
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
    public AlternatorDynamoDbAsyncClientBuilder withKeyRouteAffinity(KeyRouteAffinity type) {
      configBuilder.withKeyRouteAffinity(type);
      return this;
    }

    /**
     * Sets the refresh interval for updating the node list when there are active requests.
     *
     * <p>When requests are being made to the cluster, the node list is refreshed at this interval
     * to quickly detect topology changes.
     *
     * <p>Default: {@link AlternatorConfig#DEFAULT_ACTIVE_REFRESH_INTERVAL_MS} (1000ms / 1 second)
     *
     * @param intervalMs the refresh interval in milliseconds, must be positive
     * @return this builder instance
     * @since 1.0.8
     */
    public AlternatorDynamoDbAsyncClientBuilder withActiveRefreshIntervalMs(long intervalMs) {
      configBuilder.withActiveRefreshIntervalMs(intervalMs);
      return this;
    }

    /**
     * Sets the refresh interval for updating the node list when the client is idle.
     *
     * <p>When no requests have been made recently, the node list is refreshed at this longer
     * interval to reduce unnecessary network traffic while still keeping the node list reasonably
     * up-to-date.
     *
     * <p>Default: {@link AlternatorConfig#DEFAULT_IDLE_REFRESH_INTERVAL_MS} (60000ms / 1 minute)
     *
     * @param intervalMs the refresh interval in milliseconds, must be positive
     * @return this builder instance
     * @since 1.0.8
     */
    public AlternatorDynamoDbAsyncClientBuilder withIdleRefreshIntervalMs(long intervalMs) {
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
    public AlternatorDynamoDbAsyncClientBuilder withMaxConnections(int maxConnections) {
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
    public AlternatorDynamoDbAsyncClientBuilder withConnectionMaxIdleTimeMs(
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
    public AlternatorDynamoDbAsyncClientBuilder withConnectionTimeToLiveMs(
        long connectionTimeToLiveMs) {
      configBuilder.withConnectionTimeToLiveMs(connectionTimeToLiveMs);
      return this;
    }

    /**
     * Sets the complete Alternator configuration.
     *
     * <p>This method allows setting all Alternator-specific configuration options at once using a
     * pre-built {@link AlternatorConfig} instance.
     *
     * @param config the Alternator configuration
     * @return this builder instance
     * @since 2.0.0
     * @deprecated Use individual builder methods instead (e.g., {@link #withRoutingScope}, {@link
     *     #withCompressionAlgorithm}, {@link #withTlsConfig}, etc.)
     */
    @Deprecated
    public AlternatorDynamoDbAsyncClientBuilder withAlternatorConfig(AlternatorConfig config) {
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
     * certificates. Never use this in production as it makes connections vulnerable to
     * man-in-the-middle attacks.
     *
     * <p>This method configures the HTTP client to trust all certificates. If you've already set a
     * custom HTTP client via {@link #httpClient(SdkAsyncHttpClient)} or {@link
     * #httpClientBuilder(SdkAsyncHttpClient.Builder)}, this method will have no effect.
     *
     * @return this builder instance
     */
    public AlternatorDynamoDbAsyncClientBuilder withDisableCertificateChecks() {
      this.disableCertificateChecks = true;
      return this;
    }

    /**
     * Sets the AWS region. This method matches {@link DynamoDbAsyncClientBuilder#region(Region)}.
     *
     * <p>Note: The region is not used by Alternator for routing (the endpoint provider handles
     * that), but it is required by the AWS SDK and may appear in logs, traces, or metrics. If not
     * specified, a default "fake-aws-region" will be used.
     *
     * @param region the AWS region
     * @return this builder instance
     */
    @Override
    public AlternatorDynamoDbAsyncClientBuilder region(Region region) {
      this.region = region;
      delegate.region(region);
      return this;
    }

    /**
     * Sets the AWS credentials provider. This method matches {@link
     * DynamoDbAsyncClientBuilder#credentialsProvider(AwsCredentialsProvider)}.
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
    public AlternatorDynamoDbAsyncClientBuilder credentialsProvider(
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
    public AlternatorDynamoDbAsyncClientBuilder endpointOverride(URI endpointOverride) {
      this.seedUri = endpointOverride;
      return this;
    }

    /**
     * Sets a custom async HTTP client. This method matches {@link
     * DynamoDbAsyncClientBuilder#httpClient(SdkAsyncHttpClient)}.
     *
     * <p>Use this to configure custom HTTP client settings such as connection pooling, timeouts, or
     * proxy settings. If not specified, the AWS SDK will create a default HTTP client.
     *
     * @param httpClient the async HTTP client to use
     * @return this builder instance
     */
    @Override
    public AlternatorDynamoDbAsyncClientBuilder httpClient(SdkAsyncHttpClient httpClient) {
      this.httpClientSet = true;
      delegate.httpClient(httpClient);
      return this;
    }

    /**
     * Sets a custom async HTTP client builder. This method matches {@link
     * DynamoDbAsyncClientBuilder#httpClientBuilder(SdkAsyncHttpClient.Builder)}.
     *
     * <p>Use this to configure HTTP client settings using a builder pattern. This is an alternative
     * to {@link #httpClient(SdkAsyncHttpClient)} when you want to customize the HTTP client
     * configuration without creating the client instance directly.
     *
     * @param httpClientBuilder the async HTTP client builder to use
     * @return this builder instance
     */
    @Override
    public AlternatorDynamoDbAsyncClientBuilder httpClientBuilder(
        SdkAsyncHttpClient.Builder httpClientBuilder) {
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
     * DynamoDbAsyncClientBuilder#overrideConfiguration(ClientOverrideConfiguration)}.
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
    public AlternatorDynamoDbAsyncClientBuilder overrideConfiguration(
        ClientOverrideConfiguration overrideConfiguration) {
      delegate.overrideConfiguration(overrideConfiguration);
      return this;
    }

    /**
     * Sets client override configuration using a builder consumer. This method matches {@link
     * DynamoDbAsyncClientBuilder#overrideConfiguration(Consumer)}.
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
    public AlternatorDynamoDbAsyncClientBuilder overrideConfiguration(
        Consumer<ClientOverrideConfiguration.Builder> builderConsumer) {
      delegate.overrideConfiguration(builderConsumer);
      return this;
    }

    /**
     * Sets async client configuration. This method matches {@link
     * DynamoDbAsyncClientBuilder#asyncConfiguration(ClientAsyncConfiguration)}.
     *
     * <p>Use this to configure async-specific settings such as the future completion executor.
     *
     * @param asyncConfiguration the async client configuration
     * @return this builder instance
     */
    @Override
    public AlternatorDynamoDbAsyncClientBuilder asyncConfiguration(
        ClientAsyncConfiguration asyncConfiguration) {
      delegate.asyncConfiguration(asyncConfiguration);
      return this;
    }

    /**
     * Sets async client configuration using a builder consumer. This method matches {@link
     * DynamoDbAsyncClientBuilder#asyncConfiguration(Consumer)}.
     *
     * @param builderConsumer a consumer that configures the async configuration builder
     * @return this builder instance
     */
    @Override
    public AlternatorDynamoDbAsyncClientBuilder asyncConfiguration(
        Consumer<ClientAsyncConfiguration.Builder> builderConsumer) {
      delegate.asyncConfiguration(builderConsumer);
      return this;
    }

    /**
     * This method is not supported by AlternatorDynamoDbAsyncClient.
     *
     * <p>The endpoint routing is automatically handled by query plan interceptors for load
     * balancing. Use {@link #endpointOverride(URI)} to specify the seed endpoint instead.
     *
     * @param endpointProvider ignored
     * @return never returns
     * @throws UnsupportedOperationException always thrown
     */
    @Override
    public AlternatorDynamoDbAsyncClientBuilder endpointProvider(
        DynamoDbEndpointProvider endpointProvider) {
      throw new UnsupportedOperationException(
          "AlternatorDynamoDbAsyncClient does not support custom endpoint providers. "
              + "Use endpointOverride(URI) to specify the seed endpoint instead.");
    }

    /**
     * This method is not supported by AlternatorDynamoDbAsyncClient.
     *
     * <p>Alternator uses its own node discovery mechanism via the {@code /localnodes} API, which is
     * incompatible with AWS endpoint discovery.
     *
     * @param endpointDiscoveryEnabled ignored
     * @return never returns
     * @throws UnsupportedOperationException always thrown
     */
    @Override
    public AlternatorDynamoDbAsyncClientBuilder endpointDiscoveryEnabled(
        boolean endpointDiscoveryEnabled) {
      throw new UnsupportedOperationException(
          "AlternatorDynamoDbAsyncClient does not support AWS endpoint discovery. "
              + "Node discovery is handled automatically via the /localnodes API.");
    }

    /**
     * This method is not supported by AlternatorDynamoDbAsyncClient.
     *
     * <p>Alternator uses its own node discovery mechanism via the {@code /localnodes} API, which is
     * incompatible with AWS endpoint discovery.
     *
     * @return never returns
     * @throws UnsupportedOperationException always thrown
     */
    @Override
    public AlternatorDynamoDbAsyncClientBuilder enableEndpointDiscovery() {
      throw new UnsupportedOperationException(
          "AlternatorDynamoDbAsyncClient does not support AWS endpoint discovery. "
              + "Node discovery is handled automatically via the /localnodes API.");
    }

    /**
     * This method is not supported by AlternatorDynamoDbAsyncClient.
     *
     * <p>FIPS (Federal Information Processing Standards) mode is an AWS-specific feature that is
     * not applicable to Alternator.
     *
     * @param fipsEnabled ignored
     * @return never returns
     * @throws UnsupportedOperationException always thrown
     */
    @Override
    public AlternatorDynamoDbAsyncClientBuilder fipsEnabled(Boolean fipsEnabled) {
      throw new UnsupportedOperationException(
          "AlternatorDynamoDbAsyncClient does not support FIPS mode (AWS-specific feature).");
    }

    /**
     * This method is not supported by AlternatorDynamoDbAsyncClient.
     *
     * <p>Dual-stack networking (IPv4/IPv6) is an AWS-specific feature that is not applicable to
     * Alternator.
     *
     * @param dualstackEnabled ignored
     * @return never returns
     * @throws UnsupportedOperationException always thrown
     */
    @Override
    public AlternatorDynamoDbAsyncClientBuilder dualstackEnabled(Boolean dualstackEnabled) {
      throw new UnsupportedOperationException(
          "AlternatorDynamoDbAsyncClient does not support dual-stack networking (AWS-specific feature).");
    }

    /**
     * Sets the account ID endpoint mode.
     *
     * <p>This is an AWS-specific feature for account ID-based endpoint routing. It is not used by
     * Alternator but is implemented to satisfy the {@link
     * software.amazon.awssdk.services.dynamodb.DynamoDbBaseClientBuilder} interface. The value is
     * passed through to the underlying AWS SDK builder.
     *
     * @param accountIdEndpointMode the account ID endpoint mode
     * @return this builder instance
     */
    @Override
    public AlternatorDynamoDbAsyncClientBuilder accountIdEndpointMode(
        AccountIdEndpointMode accountIdEndpointMode) {
      delegate.accountIdEndpointMode(accountIdEndpointMode);
      return this;
    }

    /**
     * Builds and returns an async DynamoDB client with Alternator load balancing configured.
     *
     * <p>This method performs the following steps:
     *
     * <ol>
     *   <li>Validates that {@link #endpointOverride(URI)} was called (required)
     *   <li>Initializes {@link AlternatorConfig} with default values if not configured
     *   <li>Creates a query plan interceptor with the seed URI and routing scope
     *   <li>Sets a default region ("fake-aws-region") if none was specified
     *   <li>Builds the underlying {@link DynamoDbAsyncClient} with all configurations applied
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
     * @return a {@link DynamoDbAsyncClient} instance configured with Alternator load balancing
     * @throws IllegalStateException if {@link #endpointOverride(URI)} was not called
     */
    @Override
    public DynamoDbAsyncClient build() {
      return buildWithAlternatorAPI().getClient();
    }

    /**
     * Builds and returns an async DynamoDB client wrapper with Alternator load balancing and access
     * to Alternator-specific APIs.
     *
     * <p>This method is similar to {@link #build()}, but returns an {@link
     * AlternatorDynamoDbAsyncClientWrapper} that provides additional methods for accessing
     * Alternator-specific functionality:
     *
     * <ul>
     *   <li>{@link AlternatorDynamoDbAsyncClientWrapper#getLiveNodes()} - Get current live nodes
     *   <li>{@link AlternatorDynamoDbAsyncClientWrapper#nextAsURI()} - Get next node in round-robin
     *   <li>{@link AlternatorDynamoDbAsyncClientWrapper#checkIfRackDatacenterFeatureIsSupported()}
     *       - Check server capabilities
     *   <li>{@link AlternatorDynamoDbAsyncClientWrapper#getAlternatorLiveNodes()} - Access the live
     *       nodes manager
     * </ul>
     *
     * @return an {@link AlternatorDynamoDbAsyncClientWrapper} instance configured with Alternator
     *     load balancing
     * @throws IllegalStateException if {@link #endpointOverride(URI)} was not called
     */
    public AlternatorDynamoDbAsyncClientWrapper buildWithAlternatorAPI() {
      if (seedUri == null) {
        throw new IllegalStateException(
            "endpointOverride must be set when using AlternatorDynamoDbAsyncClientBuilder. "
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
      boolean optimizeHeaders = alternatorConfig.isOptimizeHeaders();
      boolean needsCustomPool = alternatorConfig.hasCustomConnectionPoolSettings();

      // Configure async HTTP client with optional certificate checking, header filtering,
      // and connection pool settings
      if (httpClientSet && (needsTrustAll || optimizeHeaders || needsCustomPool)) {
        throw new IllegalStateException(
            "Cannot use custom HTTP client with trustAllCertificates, optimizeHeaders, "
                + "or connection pool settings. "
                + "These options require configuring the HTTP client internally.");
      }
      if (!httpClientSet && (needsTrustAll || optimizeHeaders || needsCustomPool)) {
        // Build the attribute map with optional pool settings
        AttributeMap.Builder attrs = AttributeMap.builder();
        if (needsTrustAll) {
          attrs.put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, true);
        }
        if (alternatorConfig.getMaxConnections() > 0) {
          attrs.put(
              SdkHttpConfigurationOption.MAX_CONNECTIONS, alternatorConfig.getMaxConnections());
        }
        if (alternatorConfig.getConnectionMaxIdleTimeMs() > 0) {
          attrs.put(
              SdkHttpConfigurationOption.CONNECTION_MAX_IDLE_TIMEOUT,
              Duration.ofMillis(alternatorConfig.getConnectionMaxIdleTimeMs()));
        }
        if (alternatorConfig.getConnectionTimeToLiveMs() > 0) {
          attrs.put(
              SdkHttpConfigurationOption.CONNECTION_TIME_TO_LIVE,
              Duration.ofMillis(alternatorConfig.getConnectionTimeToLiveMs()));
        }

        SdkAsyncHttpClient baseHttpClient =
            new DefaultSdkAsyncHttpClientBuilder().buildWithDefaults(attrs.build());

        // Wrap with header filtering if enabled
        if (optimizeHeaders) {
          delegate.httpClient(
              new HeadersFilteringSdkAsyncHttpClient(
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
      // The interceptor handles all routing via modifyHttpRequest() using ExecutionAttributes,
      // which works correctly with both sync and async clients.
      ClientOverrideConfiguration.Builder overrideBuilder =
          delegate.overrideConfiguration() != null
              ? delegate.overrideConfiguration().toBuilder()
              : ClientOverrideConfiguration.builder();

      KeyRouteAffinityConfig keyAffinityConfig = alternatorConfig.getKeyRouteAffinityConfig();
      if (keyAffinityConfig != null
          && keyAffinityConfig.getType() != null
          && keyAffinityConfig.getType() != KeyRouteAffinity.NONE) {
        overrideBuilder.addExecutionInterceptor(
            new AffinityQueryPlanInterceptor(keyAffinityConfig, liveNodes));
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
      DynamoDbAsyncClient client = delegate.build();
      return new AlternatorDynamoDbAsyncClientWrapper(client, liveNodes, alternatorConfig);
    }
  }
}
