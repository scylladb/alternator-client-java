package com.scylladb.alternator.keyrouting;

import java.net.URI;

/**
 * Thread-local context for passing key route affinity decisions to the endpoint provider.
 *
 * <p>This class provides a mechanism for the {@link KeyRouteAffinityInterceptor} to communicate the
 * target node URI to the {@link com.scylladb.alternator.AlternatorEndpointProvider} during request
 * execution.
 *
 * <p>The context uses a ThreadLocal to store the target URI, which is set by the interceptor before
 * endpoint resolution and cleared after the request completes.
 *
 * <p><strong>Important:</strong> This ThreadLocal-based approach only works reliably with
 * synchronous clients ({@link software.amazon.awssdk.services.dynamodb.DynamoDbClient}). With async
 * clients ({@link software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient}), the interceptor's
 * {@code beforeExecution} may run on a different thread than the endpoint provider's {@code
 * resolveEndpoint}, causing the ThreadLocal value to be null. Key route affinity should only be
 * used with synchronous DynamoDB clients.
 *
 * @author dmitry.kropachev
 * @since 1.0.7
 */
public final class KeyRouteAffinityContext {

  private static final ThreadLocal<URI> targetNode = new ThreadLocal<>();

  private KeyRouteAffinityContext() {}

  /**
   * Sets the target node URI for the current request.
   *
   * @param uri the target node URI, or null to clear
   */
  public static void setTargetNode(URI uri) {
    if (uri == null) {
      targetNode.remove();
    } else {
      targetNode.set(uri);
    }
  }

  /**
   * Gets the target node URI for the current request.
   *
   * @return the target node URI, or null if not set
   */
  public static URI getTargetNode() {
    return targetNode.get();
  }

  /** Clears the target node URI for the current thread. */
  public static void clear() {
    targetNode.remove();
  }

  /**
   * Checks if a target node has been set for the current request.
   *
   * @return true if a target node is set
   */
  public static boolean hasTargetNode() {
    return targetNode.get() != null;
  }
}
