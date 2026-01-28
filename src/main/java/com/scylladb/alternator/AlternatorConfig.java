package com.scylladb.alternator;

import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;

/**
 * Configuration class for Alternator load balancing settings. Contains datacenter and rack
 * configuration for filtering nodes.
 *
 * @author dmitry.kropachev
 * @since 1.0.5
 */
public class AlternatorConfig {
  /** Default minimum request body size (in bytes) that triggers compression. */
  public static final int DEFAULT_MIN_COMPRESSION_SIZE_BYTES = 1024;

  private final String datacenter;
  private final String rack;
  private final RequestCompressionAlgorithm compressionAlgorithm;
  private final int minCompressionSizeBytes;

  /**
   * Package-private constructor. Use {@link AlternatorConfig#builder()} to create instances.
   *
   * @param datacenter the datacenter name
   * @param rack the rack name
   * @param compressionAlgorithm the compression algorithm to use
   * @param minCompressionSizeBytes minimum request size in bytes to trigger compression
   */
  protected AlternatorConfig(
      String datacenter,
      String rack,
      RequestCompressionAlgorithm compressionAlgorithm,
      int minCompressionSizeBytes) {
    this.datacenter = datacenter != null ? datacenter : "";
    this.rack = rack != null ? rack : "";
    this.compressionAlgorithm =
        compressionAlgorithm != null ? compressionAlgorithm : RequestCompressionAlgorithm.NONE;
    this.minCompressionSizeBytes =
        minCompressionSizeBytes >= 0 ? minCompressionSizeBytes : DEFAULT_MIN_COMPRESSION_SIZE_BYTES;
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
   * Creates a new builder for AlternatorConfig.
   *
   * @return a new {@link Builder}
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Applies compression configuration to a ClientOverrideConfiguration builder if compression is
   * enabled in this config.
   *
   * <p>This is a helper method used internally by {@link AlternatorDynamoDbClient} and {@link
   * AlternatorDynamoDbAsyncClient} builders to apply compression settings to the AWS SDK client.
   *
   * <p>When compression is enabled, this adds a {@link GzipRequestInterceptor} that compresses
   * request bodies exceeding the minimum size threshold.
   *
   * @param existingConfig the existing ClientOverrideConfiguration, or null if none exists
   * @return a ClientOverrideConfiguration with compression settings applied, or the existing config
   *     if compression is disabled
   * @since 1.0.5
   */
  public ClientOverrideConfiguration applyCompressionConfig(
      ClientOverrideConfiguration existingConfig) {
    if (!compressionAlgorithm.isEnabled()) {
      return existingConfig;
    }

    ClientOverrideConfiguration.Builder overrideBuilder;
    if (existingConfig != null) {
      overrideBuilder = existingConfig.toBuilder();
    } else {
      overrideBuilder = ClientOverrideConfiguration.builder();
    }

    // Add GZIP compression interceptor
    overrideBuilder.addExecutionInterceptor(new GzipRequestInterceptor(minCompressionSizeBytes));

    return overrideBuilder.build();
  }

  public static class Builder {
    private String datacenter = "";
    private String rack = "";
    private RequestCompressionAlgorithm compressionAlgorithm = RequestCompressionAlgorithm.NONE;
    private int minCompressionSizeBytes = DEFAULT_MIN_COMPRESSION_SIZE_BYTES;

    /** Package-private constructor. Use {@link AlternatorConfig#builder()} to create instances. */
    Builder() {}

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
     * @throws IllegalArgumentException if minCompressionSizeBytes is negative
     * @since 1.0.5
     */
    public Builder withMinCompressionSizeBytes(int minCompressionSizeBytes) {
      if (minCompressionSizeBytes < 0) {
        throw new IllegalArgumentException(
            "minCompressionSizeBytes must be non-negative, but was: " + minCompressionSizeBytes);
      }
      this.minCompressionSizeBytes = minCompressionSizeBytes;
      return this;
    }

    /**
     * Builds and returns an {@link AlternatorConfig} instance with the configured settings.
     *
     * @return a new {@link AlternatorConfig} instance
     */
    public AlternatorConfig build() {
      return new AlternatorConfig(
          datacenter, rack, compressionAlgorithm, minCompressionSizeBytes);
    }
  }
}
