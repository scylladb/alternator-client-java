package com.scylladb.alternator;

import com.scylladb.alternator.routing.ClusterScope;
import com.scylladb.alternator.routing.RoutingScope;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Configuration class for Alternator load balancing settings.
 *
 * <p>This class holds all configuration needed for connecting to an Alternator cluster:
 *
 * <ul>
 *   <li>Seed hosts, scheme, and port for cluster discovery
 *   <li>Routing scope for node targeting with fallback support
 *   <li>Request compression settings
 *   <li>HTTP header optimization settings
 * </ul>
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * // Using a seed URI (convenience)
 * AlternatorConfig config = AlternatorConfig.builder()
 *     .withSeedNode(URI.create("https://localhost:8043"))
 *     .withRoutingScope(DatacenterScope.of("dc1", ClusterScope.create()))
 *     .build();
 *
 * // Using explicit host, scheme, and port
 * AlternatorConfig config = AlternatorConfig.builder()
 *     .withSeedHosts(Arrays.asList("192.168.1.100", "192.168.1.101"))
 *     .withScheme("https")
 *     .withPort(8043)
 *     .withRoutingScope(DatacenterScope.of("dc1", ClusterScope.create()))
 *     .build();
 * }</pre>
 *
 * @author dmitry.kropachev
 * @since 1.0.5
 */
public class AlternatorConfig {
  /** Default minimum request body size (in bytes) that triggers compression. */
  public static final int DEFAULT_MIN_COMPRESSION_SIZE_BYTES = 1024;

  /**
   * Base HTTP headers required for proper operation with Alternator.
   *
   * <p>These headers are always required regardless of configuration:
   *
   * <ul>
   *   <li>{@code Host} - Required by HTTP/1.1
   *   <li>{@code X-Amz-Target} - Specifies the DynamoDB operation
   *   <li>{@code Content-Type} - MIME type for DynamoDB API (application/x-amz-json-1.0)
   *   <li>{@code Content-Length} - Required for request body
   *   <li>{@code Accept-Encoding} - For response compression negotiation
   * </ul>
   *
   * @since 1.0.6
   */
  public static final Set<String> BASE_REQUIRED_HEADERS =
      Collections.unmodifiableSet(
          new HashSet<>(
              Arrays.asList(
                  "Host", "X-Amz-Target", "Content-Type", "Content-Length", "Accept-Encoding")));

  /**
   * HTTP headers required when compression is enabled.
   *
   * <ul>
   *   <li>{@code Content-Encoding} - For request compression (e.g., gzip)
   * </ul>
   *
   * @since 1.0.6
   */
  public static final Set<String> COMPRESSION_HEADERS =
      Collections.unmodifiableSet(new HashSet<>(Arrays.asList("Content-Encoding")));

  /**
   * HTTP headers required when authentication is enabled.
   *
   * <ul>
   *   <li>{@code Authorization} - AWS SigV4 signature
   *   <li>{@code X-Amz-Date} - Timestamp for AWS signature
   * </ul>
   *
   * @since 1.0.6
   */
  public static final Set<String> AUTHENTICATION_HEADERS =
      Collections.unmodifiableSet(new HashSet<>(Arrays.asList("Authorization", "X-Amz-Date")));

  private final List<String> seedHosts;
  private final String scheme;
  private final int port;
  private final RoutingScope routingScope;
  private final RequestCompressionAlgorithm compressionAlgorithm;
  private final int minCompressionSizeBytes;
  private final boolean optimizeHeaders;
  private final Set<String> headersWhitelist;
  private final boolean authenticationEnabled;

  /**
   * Package-private constructor. Use {@link AlternatorConfig#builder()} to create instances.
   *
   * @param seedHosts the list of seed host addresses (IP or hostname) for cluster discovery
   * @param scheme the URI scheme (http or https)
   * @param port the port number
   * @param routingScope the routing scope for node targeting
   * @param compressionAlgorithm the compression algorithm to use
   * @param minCompressionSizeBytes minimum request size in bytes to trigger compression
   * @param optimizeHeaders whether to enable HTTP header optimization
   * @param headersWhitelist the set of headers to preserve when optimization is enabled
   * @param authenticationEnabled whether authentication is enabled
   */
  protected AlternatorConfig(
      List<String> seedHosts,
      String scheme,
      int port,
      RoutingScope routingScope,
      RequestCompressionAlgorithm compressionAlgorithm,
      int minCompressionSizeBytes,
      boolean optimizeHeaders,
      Set<String> headersWhitelist,
      boolean authenticationEnabled) {
    this.seedHosts =
        seedHosts != null
            ? Collections.unmodifiableList(new ArrayList<>(seedHosts))
            : Collections.<String>emptyList();
    this.scheme = scheme != null ? scheme : "";
    this.port = port;
    this.routingScope = routingScope;
    this.compressionAlgorithm =
        compressionAlgorithm != null ? compressionAlgorithm : RequestCompressionAlgorithm.NONE;
    this.minCompressionSizeBytes =
        minCompressionSizeBytes >= 0 ? minCompressionSizeBytes : DEFAULT_MIN_COMPRESSION_SIZE_BYTES;
    this.optimizeHeaders = optimizeHeaders;
    this.authenticationEnabled = authenticationEnabled;

    // Compute default whitelist based on configuration if not provided
    if (headersWhitelist != null) {
      this.headersWhitelist = Collections.unmodifiableSet(new HashSet<>(headersWhitelist));
    } else {
      this.headersWhitelist = getRequiredHeaders();
    }
  }

  /**
   * Gets the list of seed host addresses for cluster discovery.
   *
   * <p>The seed hosts are the initial endpoints (IP addresses or hostnames) used to discover the
   * full cluster topology via the {@code /localnodes} API. Use {@link #getScheme()} and {@link
   * #getPort()} to construct full URIs.
   *
   * @return an unmodifiable list of seed host addresses, never null but may be empty
   * @since 1.0.5
   */
  public List<String> getSeedHosts() {
    return seedHosts;
  }

  /**
   * Gets the URI scheme (http or https).
   *
   * @return the scheme, or empty string if not set
   * @since 1.0.5
   */
  public String getScheme() {
    return scheme;
  }

  /**
   * Gets the port number for Alternator connections.
   *
   * @return the port number
   * @since 1.0.5
   */
  public int getPort() {
    return port;
  }

  /**
   * Gets the configured routing scope.
   *
   * <p>The routing scope defines which nodes should be used for load balancing and how to fall back
   * when nodes are unavailable. This method always returns a non-null scope (defaults to {@link
   * ClusterScope} if not configured).
   *
   * @return the routing scope, never {@code null}
   * @since 1.0.5
   */
  public RoutingScope getRoutingScope() {
    return routingScope;
  }

  /**
   * Gets the configured request compression algorithm.
   *
   * <p>Compression is applied to request bodies that exceed {@link #getMinCompressionSizeBytes()}.
   * By default, compression is disabled ({@link RequestCompressionAlgorithm#NONE}).
   *
   * @return the compression algorithm, never null
   * @since 1.0.5
   */
  public RequestCompressionAlgorithm getCompressionAlgorithm() {
    return compressionAlgorithm;
  }

  /**
   * Gets the minimum request body size (in bytes) that triggers compression.
   *
   * <p>Requests smaller than this threshold will not be compressed, even if a compression algorithm
   * is configured. This avoids the overhead of compressing small payloads that may not benefit from
   * compression or may even increase in size.
   *
   * <p>Default value: {@link #DEFAULT_MIN_COMPRESSION_SIZE_BYTES} (1024 bytes / 1 KB)
   *
   * @return the minimum request size in bytes, always non-negative
   * @since 1.0.5
   */
  public int getMinCompressionSizeBytes() {
    return minCompressionSizeBytes;
  }

  /**
   * Checks if HTTP header optimization is enabled.
   *
   * <p>When enabled, outgoing requests will have their HTTP headers filtered to include only those
   * in the whitelist, reducing network traffic overhead. Alternator does not use all headers that
   * DynamoDB normally uses, so this optimization can reduce outgoing traffic by up to 56%.
   *
   * @return true if header optimization is enabled, false otherwise
   * @since 1.0.6
   */
  public boolean isOptimizeHeaders() {
    return optimizeHeaders;
  }

  /**
   * Gets the set of HTTP headers to preserve when optimization is enabled.
   *
   * <p>Only headers in this whitelist will be sent with requests when header optimization is
   * enabled. All other headers will be removed. Header names are matched case-insensitively per RFC
   * 7230.
   *
   * @return unmodifiable set of allowed header names
   * @since 1.0.6
   */
  public Set<String> getHeadersWhitelist() {
    return headersWhitelist;
  }

  /**
   * Checks if authentication is enabled.
   *
   * <p>When authentication is disabled, the client will use anonymous credentials and exclude
   * authentication headers ({@code Authorization}, {@code X-Amz-Date}, {@code
   * X-Amz-Content-Sha256}) from requests when header optimization is enabled.
   *
   * <p>This is useful when connecting to Alternator clusters that have authentication disabled.
   *
   * @return true if authentication is enabled (default), false if disabled
   * @since 1.0.6
   */
  public boolean isAuthenticationEnabled() {
    return authenticationEnabled;
  }

  /**
   * Returns the set of HTTP headers required for this configuration.
   *
   * <p>This method returns the minimum set of headers needed based on the current settings
   * (compression algorithm and authentication state).
   *
   * @return an unmodifiable set of required header names
   * @since 1.0.6
   */
  public Set<String> getRequiredHeaders() {
    Set<String> required = new HashSet<>(BASE_REQUIRED_HEADERS);
    if (compressionAlgorithm != null && compressionAlgorithm != RequestCompressionAlgorithm.NONE) {
      required.addAll(COMPRESSION_HEADERS);
    }
    if (authenticationEnabled) {
      required.addAll(AUTHENTICATION_HEADERS);
    }
    return Collections.unmodifiableSet(required);
  }

  /**
   * Creates a new builder for AlternatorConfig.
   *
   * @return a new {@link Builder}
   */
  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private List<String> seedHosts = new ArrayList<>();
    private String scheme = "";
    private int port = -1;
    private RoutingScope routingScope = null;
    private RequestCompressionAlgorithm compressionAlgorithm = RequestCompressionAlgorithm.NONE;
    private int minCompressionSizeBytes = DEFAULT_MIN_COMPRESSION_SIZE_BYTES;
    private boolean optimizeHeaders = false;
    private Set<String> headersWhitelist = null; // null means use default based on config
    private boolean headersWhitelistWasSet = false;
    private boolean authenticationEnabled = true;

    /** Package-private constructor. Use {@link AlternatorConfig#builder()} to create instances. */
    Builder() {}

    /**
     * Sets whether authentication is enabled. Package-private - authentication is auto-detected
     * based on whether credentials are provided to the client builder.
     *
     * @param authenticationEnabled true if credentials are provided
     * @return this builder instance
     */
    Builder authenticationEnabled(boolean authenticationEnabled) {
      this.authenticationEnabled = authenticationEnabled;
      return this;
    }

    /**
     * Sets a single seed node URI for cluster discovery.
     *
     * <p>This is a convenience method that extracts the host, scheme, and port from a URI. The URI
     * will be used to discover all other nodes in the cluster via the {@code /localnodes} API.
     *
     * @param seedUri the seed URI for Alternator cluster discovery
     * @return this builder instance
     * @since 1.0.5
     */
    public Builder withSeedNode(URI seedUri) {
      if (seedUri != null) {
        this.seedHosts = Collections.singletonList(seedUri.getHost());
        this.scheme = seedUri.getScheme();
        this.port = seedUri.getPort();
      }
      return this;
    }

    /**
     * Sets the URI scheme for Alternator connections.
     *
     * @param scheme the URI scheme ("http" or "https")
     * @return this builder instance
     * @since 1.0.5
     */
    public Builder withScheme(String scheme) {
      this.scheme = scheme != null ? scheme : "";
      return this;
    }

    /**
     * Sets the port number for Alternator connections.
     *
     * @param port the port number
     * @return this builder instance
     * @since 1.0.5
     */
    public Builder withPort(int port) {
      this.port = port;
      return this;
    }

    /**
     * Sets seed host addresses for cluster discovery.
     *
     * <p>The seed hosts are IP addresses or hostnames of Alternator nodes. You must also set the
     * scheme and port using {@link #withScheme(String)} and {@link #withPort(int)}.
     *
     * @param hosts the collection of seed host addresses (IP or hostname)
     * @return this builder instance
     * @since 1.0.5
     */
    public Builder withSeedHosts(Collection<String> hosts) {
      if (hosts != null) {
        this.seedHosts = new ArrayList<>(hosts);
      }
      return this;
    }

    /**
     * Sets a single seed host address for cluster discovery.
     *
     * <p>The seed host is an IP address or hostname of an Alternator node. You must also set the
     * scheme and port using {@link #withScheme(String)} and {@link #withPort(int)}.
     *
     * @param host the seed host address (IP or hostname)
     * @return this builder instance
     * @since 1.0.5
     */
    public Builder withSeedHost(String host) {
      if (host != null) {
        this.seedHosts = Collections.singletonList(host);
      }
      return this;
    }

    /**
     * Returns the set of HTTP headers required for the current configuration.
     *
     * <p>This method computes the minimum set of headers needed based on the current builder
     * settings (compression algorithm and authentication state). Use this to understand what
     * headers must be included when providing a custom whitelist.
     *
     * <p>Example:
     *
     * <pre>{@code
     * AlternatorConfig.Builder builder = AlternatorConfig.builder()
     *     .withCompressionAlgorithm(RequestCompressionAlgorithm.GZIP);
     *
     * Set<String> required = builder.getRequiredHeaders();
     * // required contains: Host, X-Amz-Target, Content-Type, Content-Length,
     * //                    Accept-Encoding, Content-Encoding, Authorization, X-Amz-Date
     * }</pre>
     *
     * @return an unmodifiable set of required header names for the current configuration
     * @since 1.0.6
     */
    public Set<String> getRequiredHeaders() {
      Set<String> required = new HashSet<>(BASE_REQUIRED_HEADERS);
      if (compressionAlgorithm != null && compressionAlgorithm.isEnabled()) {
        required.addAll(COMPRESSION_HEADERS);
      }
      if (authenticationEnabled) {
        required.addAll(AUTHENTICATION_HEADERS);
      }
      return Collections.unmodifiableSet(required);
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
     * @param routingScope the routing scope, or {@code null} to use all nodes (ClusterScope)
     * @return this builder instance
     * @since 1.0.5
     */
    public Builder withRoutingScope(RoutingScope routingScope) {
      this.routingScope = routingScope;
      return this;
    }

    /**
     * Sets the request compression algorithm.
     *
     * <p>When a compression algorithm other than {@link RequestCompressionAlgorithm#NONE} is
     * specified, request bodies exceeding the minimum size threshold will be compressed before
     * transmission.
     *
     * <p>Example:
     *
     * <pre>{@code
     * AlternatorConfig config = AlternatorConfig.builder()
     *     .withCompressionAlgorithm(RequestCompressionAlgorithm.GZIP)
     *     .build();
     * }</pre>
     *
     * @param algorithm the compression algorithm to use, or null to disable compression
     * @return this builder instance
     * @since 1.0.5
     */
    public Builder withCompressionAlgorithm(RequestCompressionAlgorithm algorithm) {
      this.compressionAlgorithm = algorithm != null ? algorithm : RequestCompressionAlgorithm.NONE;
      return this;
    }

    /**
     * Sets the minimum request body size (in bytes) that triggers compression.
     *
     * <p>Requests smaller than this threshold will not be compressed, even if a compression
     * algorithm is configured. This avoids compressing small payloads that may not benefit.
     *
     * <p>Default: {@link AlternatorConfig#DEFAULT_MIN_COMPRESSION_SIZE_BYTES} (1024 bytes / 1 KB)
     *
     * <p>Example:
     *
     * <pre>{@code
     * AlternatorConfig config = AlternatorConfig.builder()
     *     .withCompressionAlgorithm(RequestCompressionAlgorithm.GZIP)
     *     .withMinCompressionSizeBytes(2048)  // Only compress requests > 2KB
     *     .build();
     * }</pre>
     *
     * @param minCompressionSizeBytes minimum request size in bytes, must be non-negative
     * @return this builder instance
     * @since 1.0.5
     */
    public Builder withMinCompressionSizeBytes(int minCompressionSizeBytes) {
      this.minCompressionSizeBytes = minCompressionSizeBytes;
      return this;
    }

    /**
     * Enables or disables HTTP header optimization.
     *
     * <p>When enabled, outgoing requests will have their HTTP headers filtered to include only
     * those in the configured whitelist (see {@link #withHeadersWhitelist(Collection)}). This
     * reduces network traffic by removing headers that Alternator does not use.
     *
     * <p>Default: false (disabled)
     *
     * <p>Example:
     *
     * <pre>{@code
     * AlternatorConfig config = AlternatorConfig.builder()
     *     .withOptimizeHeaders(true)
     *     .build();
     * }</pre>
     *
     * @param optimizeHeaders true to enable header filtering, false to disable
     * @return this builder instance
     * @since 1.0.6
     */
    public Builder withOptimizeHeaders(boolean optimizeHeaders) {
      this.optimizeHeaders = optimizeHeaders;
      return this;
    }

    /**
     * Sets a custom whitelist of HTTP headers to preserve when optimization is enabled.
     *
     * <p>Only headers in this list will be sent with requests when header optimization is enabled.
     * All other headers will be removed. Header names are matched case-insensitively per RFC 7230.
     *
     * <p>The provided whitelist must include all headers required for the current configuration.
     * Use {@link #getRequiredHeaders()} to see which headers are required. If required headers are
     * missing, an {@link IllegalArgumentException} will be thrown at build time.
     *
     * <p>Default: Computed automatically based on compression and authentication settings. See
     * {@link AlternatorConfig#getRequiredHeaders()}.
     *
     * <p>Example:
     *
     * <pre>{@code
     * AlternatorConfig.Builder builder = AlternatorConfig.builder()
     *     .withCompressionAlgorithm(RequestCompressionAlgorithm.GZIP)
     *     .withOptimizeHeaders(true);
     *
     * // Get required headers and add custom ones
     * Set<String> whitelist = new HashSet<>(builder.getRequiredHeaders());
     * whitelist.add("X-Custom-Header");
     *
     * AlternatorConfig config = builder
     *     .withHeadersWhitelist(whitelist)
     *     .build();
     * }</pre>
     *
     * @param headers collection of header names to preserve (case-insensitive), must not be null
     * @return this builder instance
     * @since 1.0.6
     */
    public Builder withHeadersWhitelist(Collection<String> headers) {
      this.headersWhitelist = headers != null ? new HashSet<>(headers) : null;
      this.headersWhitelistWasSet = true;
      return this;
    }

    /**
     * Builds and returns an {@link AlternatorConfig} instance with the configured settings.
     *
     * @return a new {@link AlternatorConfig} instance
     * @throws IllegalArgumentException if minCompressionSizeBytes is negative, or if
     *     headersWhitelist is empty or missing required headers
     */
    public AlternatorConfig build() {
      // Validate minCompressionSizeBytes
      if (minCompressionSizeBytes < 0) {
        throw new IllegalArgumentException(
            "minCompressionSizeBytes must be non-negative, but was: " + minCompressionSizeBytes);
      }

      // Validate headersWhitelist if it was explicitly set
      if (headersWhitelistWasSet) {
        if (headersWhitelist == null || headersWhitelist.isEmpty()) {
          throw new IllegalArgumentException(
              "headersWhitelist cannot be null or empty. "
                  + "To disable optimization, use withOptimizeHeaders(false)");
        }

        // Validate that all required headers are present (case-insensitive)
        Set<String> required = getRequiredHeaders();
        Set<String> headersLowerCase = new HashSet<>();
        for (String h : headersWhitelist) {
          headersLowerCase.add(h.toLowerCase());
        }

        Set<String> missing = new HashSet<>();
        for (String requiredHeader : required) {
          if (!headersLowerCase.contains(requiredHeader.toLowerCase())) {
            missing.add(requiredHeader);
          }
        }

        if (!missing.isEmpty()) {
          throw new IllegalArgumentException(
              "Custom headers whitelist is missing required headers: "
                  + missing
                  + ". "
                  + "Use getRequiredHeaders() to see all required headers for the current configuration.");
        }
      }

      // Determine the effective routing scope (default to ClusterScope if not set)
      RoutingScope effectiveRoutingScope =
          routingScope != null ? routingScope : ClusterScope.create();

      return new AlternatorConfig(
          seedHosts,
          scheme,
          port,
          effectiveRoutingScope,
          compressionAlgorithm,
          minCompressionSizeBytes,
          optimizeHeaders,
          headersWhitelist,
          authenticationEnabled);
    }
  }
}
