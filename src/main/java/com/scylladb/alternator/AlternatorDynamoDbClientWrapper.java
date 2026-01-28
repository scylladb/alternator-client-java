package com.scylladb.alternator;

import com.scylladb.alternator.internal.AlternatorLiveNodes;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.util.List;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/**
 * A wrapper interface for DynamoDbClient that provides access to Alternator-specific functionality.
 *
 * <p>This interface extends {@link DynamoDbClient} and adds methods to access the underlying
 * Alternator infrastructure, such as the list of live nodes for load balancing and the {@link
 * AlternatorEndpointProvider} managing endpoint resolution.
 *
 * <p>Instances of this interface are created by {@link AlternatorDynamoDbClient#builder()} and can
 * be used anywhere a standard {@link DynamoDbClient} is expected, while also providing access to
 * Alternator-specific features.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * AlternatorDynamoDbClientWrapper client = AlternatorDynamoDbClient.builder()
 *     .endpointOverride(URI.create("http://localhost:8000"))
 *     .credentialsProvider(credentialsProvider)
 *     .build();
 *
 * // Use as a standard DynamoDB client
 * client.createTable(...);
 *
 * // Access Alternator-specific functionality
 * List<URI> nodes = client.getLiveNodes();
 * URI nextNode = client.nextAsURI();
 * }</pre>
 *
 * @author dmitry.kropachev
 * @since 1.0.5
 */
public interface AlternatorDynamoDbClientWrapper extends DynamoDbClient {

  /**
   * Returns a snapshot of the current live nodes list.
   *
   * @return an unmodifiable list of the current live node URIs
   */
  List<URI> getLiveNodes();

  /**
   * Returns the next node URI using round-robin selection.
   *
   * @return the next {@link URI} in the round-robin sequence
   */
  URI nextAsURI();

  /**
   * Checks if the server supports rack/datacenter filtering for node discovery.
   *
   * @return true if rack/datacenter filtering is supported, false otherwise
   * @throws AlternatorLiveNodes.FailedToCheck if the check cannot be completed
   */
  boolean checkIfRackDatacenterFeatureIsSupported() throws AlternatorLiveNodes.FailedToCheck;

  /**
   * Returns the AlternatorEndpointProvider used for endpoint resolution.
   *
   * <p>The endpoint provider is responsible for selecting which node to use for each request.
   *
   * @return the {@link AlternatorEndpointProvider} instance
   */
  AlternatorEndpointProvider getAlternatorEndpointProvider();

  /**
   * Creates a wrapper around an existing DynamoDbClient with Alternator metadata.
   *
   * @param delegate the underlying DynamoDbClient to wrap
   * @param endpointProvider the AlternatorEndpointProvider used for this client
   * @return a new AlternatorDynamoDbClientWrapper that delegates to the given client
   */
  static AlternatorDynamoDbClientWrapper wrap(
      DynamoDbClient delegate, AlternatorEndpointProvider endpointProvider) {
    return (AlternatorDynamoDbClientWrapper)
        Proxy.newProxyInstance(
            AlternatorDynamoDbClientWrapper.class.getClassLoader(),
            new Class<?>[] {AlternatorDynamoDbClientWrapper.class},
            new DynamoDbClientInvocationHandler(delegate, endpointProvider));
  }

  /** Invocation handler that delegates DynamoDbClient calls and handles wrapper-specific methods. */
  class DynamoDbClientInvocationHandler implements InvocationHandler {
    private final DynamoDbClient delegate;
    private final AlternatorEndpointProvider endpointProvider;

    DynamoDbClientInvocationHandler(
        DynamoDbClient delegate, AlternatorEndpointProvider endpointProvider) {
      this.delegate = delegate;
      this.endpointProvider = endpointProvider;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      String methodName = method.getName();

      // Handle AlternatorDynamoDbClientWrapper-specific methods
      if ("getLiveNodes".equals(methodName) && (args == null || args.length == 0)) {
        return endpointProvider.getAlternatorLiveNodes().getLiveNodes();
      }
      if ("nextAsURI".equals(methodName) && (args == null || args.length == 0)) {
        return endpointProvider.getAlternatorLiveNodes().nextAsURI();
      }
      if ("checkIfRackDatacenterFeatureIsSupported".equals(methodName)
          && (args == null || args.length == 0)) {
        return endpointProvider.getAlternatorLiveNodes().checkIfRackDatacenterFeatureIsSupported();
      }
      if ("getAlternatorEndpointProvider".equals(methodName) && (args == null || args.length == 0)) {
        return endpointProvider;
      }

      // Handle Object methods
      if ("equals".equals(methodName) && args != null && args.length == 1) {
        return proxy == args[0];
      }
      if ("hashCode".equals(methodName) && (args == null || args.length == 0)) {
        return System.identityHashCode(proxy);
      }
      if ("toString".equals(methodName) && (args == null || args.length == 0)) {
        return "AlternatorDynamoDbClientWrapper[delegate=" + delegate + "]";
      }

      // Delegate all other methods to the underlying client
      try {
        return method.invoke(delegate, args);
      } catch (InvocationTargetException e) {
        // Unwrap the cause to preserve the original exception type
        throw e.getCause();
      }
    }
  }
}
