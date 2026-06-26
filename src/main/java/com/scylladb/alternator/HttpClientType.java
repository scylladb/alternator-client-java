package com.scylladb.alternator;

import com.scylladb.alternator.internal.AsyncClientDetector;
import com.scylladb.alternator.internal.SyncClientDetector;

/**
 * Enumeration of HTTP client implementations that can be used with Alternator client builders.
 *
 * <p>By default, the builder auto-detects the HTTP client from the classpath. Use this enum to
 * explicitly select a specific implementation. If the requested implementation is not on the
 * classpath, the builder will throw an {@link IllegalStateException} at build time.
 *
 * <p>Not all implementations support both sync and async clients:
 *
 * <table>
 * <caption>HTTP client compatibility matrix</caption>
 * <tr><th>Type</th><th>Sync</th><th>Async</th><th>Custom CA certs</th><th>Client certs</th><th>Connection TTL</th></tr>
 * <tr><td>{@link #APACHE}</td><td>Yes</td><td>No</td><td>Yes</td><td>Yes</td><td>Yes</td></tr>
 * <tr><td>{@link #NETTY}</td><td>No</td><td>Yes</td><td>Yes</td><td>Yes</td><td>Yes</td></tr>
 * <tr><td>{@link #CRT}</td><td>Yes</td><td>Yes</td><td>No</td><td>No</td><td>No</td></tr>
 * <tr><td>{@link #AUTO}</td><td>Yes</td><td>Yes</td><td>Depends</td><td>Depends</td><td>Depends</td></tr>
 * </table>
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * // Force CRT HTTP client for sync
 * DynamoDbClient client = AlternatorDynamoDbClient.builder()
 *     .endpointOverride(URI.create("https://localhost:8043"))
 *     .withHttpClientType(HttpClientType.CRT)
 *     .build();
 *
 * // Force Netty for async
 * DynamoDbAsyncClient asyncClient = AlternatorDynamoDbAsyncClient.builder()
 *     .endpointOverride(URI.create("https://localhost:8043"))
 *     .withHttpClientType(HttpClientType.NETTY)
 *     .build();
 * }</pre>
 *
 * @author dmitry.kropachev
 * @since 2.1.0
 */
public enum HttpClientType {

  /**
   * Auto-detect the HTTP client from the classpath.
   *
   * <p>For sync clients, the detection order is: Apache &gt; CRT. For async clients: Netty &gt;
   * CRT. This is the default behavior when {@code withHttpClientType} is not called.
   */
  AUTO(true, true),

  /**
   * Apache HTTP Client ({@code software.amazon.awssdk:apache-client}).
   *
   * <p>Sync only. Supports custom CA certificates, client TLS certificates, and connection
   * time-to-live. Using this type on an async client builder will throw {@link
   * IllegalStateException}.
   */
  APACHE(true, false),

  /**
   * Netty NIO HTTP Client ({@code software.amazon.awssdk:netty-nio-client}).
   *
   * <p>Async only. Supports custom CA certificates, client TLS certificates, and connection
   * time-to-live. Using this type on a sync client builder will throw {@link
   * IllegalStateException}.
   */
  NETTY(false, true),

  /**
   * AWS CRT HTTP Client ({@code software.amazon.awssdk:aws-crt-client}).
   *
   * <p>Supports both sync and async clients. Does not support custom CA certificates, client TLS
   * certificates, or connection time-to-live.
   */
  CRT(true, true);

  private final boolean supportsSync;
  private final boolean supportsAsync;

  HttpClientType(boolean supportsSync, boolean supportsAsync) {
    this.supportsSync = supportsSync;
    this.supportsAsync = supportsAsync;
  }

  boolean supportsSync() {
    return supportsSync;
  }

  boolean supportsAsync() {
    return supportsAsync;
  }

  /**
   * Maps this type to a concrete sync client type.
   *
   * @return the corresponding {@link SyncClientDetector.SyncClientType}
   * @throws IllegalArgumentException if this type is {@link #AUTO} (must be resolved first) or does
   *     not support sync clients
   */
  SyncClientDetector.SyncClientType toSyncClientType() {
    switch (this) {
      case APACHE:
        return SyncClientDetector.SyncClientType.APACHE;
      case CRT:
        return SyncClientDetector.SyncClientType.CRT;
      case AUTO:
        throw new IllegalArgumentException(
            "AUTO must be resolved before mapping to a concrete sync client type");
      default:
        throw new IllegalArgumentException(
            "HttpClientType." + this.name() + " cannot be mapped to a sync client type");
    }
  }

  /**
   * Maps this type to a concrete async client type.
   *
   * @return the corresponding {@link AsyncClientDetector.AsyncClientType}
   * @throws IllegalArgumentException if this type is {@link #AUTO} (must be resolved first) or does
   *     not support async clients
   */
  AsyncClientDetector.AsyncClientType toAsyncClientType() {
    switch (this) {
      case NETTY:
        return AsyncClientDetector.AsyncClientType.NETTY;
      case CRT:
        return AsyncClientDetector.AsyncClientType.CRT;
      case AUTO:
        throw new IllegalArgumentException(
            "AUTO must be resolved before mapping to a concrete async client type");
      default:
        throw new IllegalArgumentException(
            "HttpClientType." + this.name() + " cannot be mapped to an async client type");
    }
  }
}
