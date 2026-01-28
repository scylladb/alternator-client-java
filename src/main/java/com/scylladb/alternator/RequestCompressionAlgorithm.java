package com.scylladb.alternator;

/**
 * Enumeration of supported request compression algorithms for Alternator client requests.
 *
 * <p>Request compression can reduce network bandwidth usage by compressing request payloads before
 * sending them to the server. Compression is applied automatically by the AWS SDK when enabled via
 * {@link AlternatorConfig}.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * AlternatorConfig config = AlternatorConfig.builder()
 *     .withCompressionAlgorithm(RequestCompressionAlgorithm.GZIP)
 *     .withMinCompressionSizeBytes(2048)
 *     .build();
 * }</pre>
 *
 * @author dmitry.kropachev
 * @since 1.0.5
 */
public enum RequestCompressionAlgorithm {
  /**
   * No compression. Request payloads are sent uncompressed. This is the default setting.
   *
   * <p>Use this when:
   *
   * <ul>
   *   <li>Network bandwidth is not a bottleneck
   *   <li>Request sizes are typically small
   *   <li>CPU overhead of compression is not desirable
   * </ul>
   */
  NONE,

  /**
   * GZIP compression.
   *
   * <p>Request payloads exceeding the minimum size threshold will be compressed using the gzip
   * algorithm before transmission.
   *
   * <p>GZIP provides good compression ratios (typically 60-80% size reduction for JSON payloads)
   * with moderate CPU overhead. It is recommended for:
   *
   * <ul>
   *   <li>Large item attributes (documents, JSON blobs)
   *   <li>Batch operations with many items
   *   <li>Text-heavy data that compresses well
   * </ul>
   */
  GZIP;

  /**
   * Checks if this algorithm represents an enabled compression setting.
   *
   * @return true if compression is enabled (algorithm is not NONE), false otherwise
   */
  public boolean isEnabled() {
    return this != NONE;
  }
}
