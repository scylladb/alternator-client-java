package com.scylladb.alternator;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Configuration class for Alternator load balancing settings. Contains datacenter and rack
 * configuration for filtering nodes.
 *
 * @author dmitry.kropachev
 * @since 1.0.5
 */
public class AlternatorConfig {
  /**
   * Default minimum request body size (in bytes) that triggers compression.
   */
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
              Arrays.asList("Host", "X-Amz-Target", "Content-Type", "Content-Length", "Accept-Encoding")));

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

  private final String datacenter;
  private final String rack;
  private final RequestCompressionAlgorithm compressionAlgorithm;
  private final int minCompressionSizeBytes;
  private final boolean optimizeHeaders;
  private final Set<String> headersWhitelist;
  private final boolean authenticationEnabled;

  /**
   * Package-private constructor. Use {@link AlternatorConfig#builder()} to create instances.
   *
   * @param datacenter              the datacenter name
   * @param rack                    the rack name
   * @param compressionAlgorithm    the compression algorithm to use
   * @param minCompressionSizeBytes minimum request size in bytes to trigger compression
   * @param optimizeHeaders         whether to enable HTTP header optimization
   * @param headersWhitelist        the set of headers to preserve when optimization is enabled
   * @param authenticationEnabled   whether authentication is enabled
   */
  protected AlternatorConfig(
      String datacenter,
      String rack,
      RequestCompressionAlgorithm compressionAlgorithm,
      int minCompressionSizeBytes,
      boolean optimizeHeaders,
      Set<String> headersWhitelist,
      boolean authenticationEnabled) {
    this.datacenter = datacenter != null ? datacenter : "";
    this.rack = rack != null ? rack : "";
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
   * Gets the configured datacenter.
   *
   * @return the datacenter name, or empty string if not set
   */
  public String getDatacenter() {
    return datacenter;
  }

  /**
   * Gets the configured rack.
   *
   * @return the rack name, or empty string if not set
   */
  public String getRack() {
    return rack;
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
   * authentication headers ({@code Authorization}, {@code X-Amz-Date}, {@code X-Amz-Content-Sha256})
   * from requests when header optimization is enabled.
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
    private String datacenter = "";
    private String rack = "";
    private RequestCompressionAlgorithm compressionAlgorithm = RequestCompressionAlgorithm.NONE;
    private int minCompressionSizeBytes = DEFAULT_MIN_COMPRESSION_SIZE_BYTES;
    private boolean optimizeHeaders = false;
    private Set<String> headersWhitelist = null; // null means use default based on config
    private boolean headersWhitelistWasSet = false;
    private boolean authenticationEnabled = true;

    /**
     * Package-private constructor. Use {@link AlternatorConfig#builder()} to create instances.
     */
    Builder() {
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
     *     .withCompressionAlgorithm(RequestCompressionAlgorithm.GZIP)
     *     .withAuthenticationEnabled(true);
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
     * Sets the target datacenter. When specified, only nodes from this datacenter will be used for
     * load balancing. If not set, all nodes will be used.
     *
     * @param datacenter the datacenter name
     * @return this builder instance
     */
    public Builder withDatacenter(String datacenter) {
      this.datacenter = datacenter != null ? datacenter : "";
      return this;
    }

    /**
     * Sets the target rack. When specified along with a datacenter, only nodes from this rack will
     * be used for load balancing.
     *
     * @param rack the rack name
     * @return this builder instance
     */
    public Builder withRack(String rack) {
      this.rack = rack != null ? rack : "";
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
      this.compressionAlgorithm =
          algorithm != null ? algorithm : RequestCompressionAlgorithm.NONE;
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
     * Use {@link #getRequiredHeaders()} to see which headers are required. If required headers
     * are missing, an {@link IllegalArgumentException} will be thrown at build time.
     *
     * <p>Default: Computed automatically based on compression and authentication settings.
     * See {@link AlternatorConfig#getRequiredHeaders()}.
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
     * Enables or disables authentication for the Alternator connection.
     *
     * <p>When authentication is disabled:
     *
     * <ul>
     *   <li>The client will use anonymous credentials (no AWS signature)
     *   <li>Authentication headers ({@code Authorization}, {@code X-Amz-Date}, {@code
     *       X-Amz-Content-Sha256}) will be excluded from the default headers whitelist when header
     *       optimization is enabled
     * </ul>
     *
     * <p>This is useful when connecting to Alternator clusters that have authentication disabled.
     *
     * <p>Default: true (authentication enabled)
     *
     * <p>Example:
     *
     * <pre>{@code
     * AlternatorConfig config = AlternatorConfig.builder()
     *     .withAuthenticationEnabled(false)
     *     .withOptimizeHeaders(true)
     *     .build();
     *
     * // No credentials needed - client will use anonymous credentials
     * DynamoDbClient client = AlternatorDynamoDbClient.builder()
     *     .endpointOverride(URI.create("http://localhost:8000"))
     *     .withAlternatorConfig(config)
     *     .build();
     * }</pre>
     *
     * @param authenticationEnabled true to enable authentication (default), false to disable
     * @return this builder instance
     * @since 1.0.6
     */
    public Builder withAuthenticationEnabled(boolean authenticationEnabled) {
      this.authenticationEnabled = authenticationEnabled;
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
              "Custom headers whitelist is missing required headers: " + missing + ". "
                  + "Use getRequiredHeaders() to see all required headers for the current configuration.");
        }
      }

      return new AlternatorConfig(
          datacenter,
          rack,
          compressionAlgorithm,
          minCompressionSizeBytes,
          optimizeHeaders,
          headersWhitelist,
          authenticationEnabled);
    }
  }
}
