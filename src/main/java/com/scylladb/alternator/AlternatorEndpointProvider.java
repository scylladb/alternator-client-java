package com.scylladb.alternator;

import com.scylladb.alternator.internal.AlternatorLiveNodes;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;
import software.amazon.awssdk.endpoints.Endpoint;
import software.amazon.awssdk.services.dynamodb.endpoints.DynamoDbEndpointParams;
import software.amazon.awssdk.services.dynamodb.endpoints.DynamoDbEndpointProvider;

// AWS Java SDK v2 allows providing a DynamoDbEndpointProvider which can
// choose a different endpoint for each request. Here we implement an
// AlternatorEndpointProvider, which maintains up-to-date knowledge of the
// live nodes in Alternator data center (by holding a AlternatorLiveNodes
// object), and choose a different node for each request.
/**
 * AlternatorEndpointProvider class.
 *
 * @author dmitry.kropachev
 */
public class AlternatorEndpointProvider implements DynamoDbEndpointProvider {
  private final AlternatorLiveNodes liveNodes;
  private final Map<URI, CompletableFuture<Endpoint>> futureCache;
  private static Logger logger = Logger.getLogger(AlternatorEndpointProvider.class.getName());

  /**
   * Constructor for AlternatorEndpointProvider.
   *
   * @param liveNodes the {@link AlternatorLiveNodes} instance to use for endpoint resolution
   */
  public AlternatorEndpointProvider(AlternatorLiveNodes liveNodes) {
    this.futureCache = new ConcurrentHashMap<>();
    this.liveNodes = liveNodes;
  }

  /** {@inheritDoc} */
  @Override
  public CompletableFuture<Endpoint> resolveEndpoint(DynamoDbEndpointParams endpointParams) {
    URI uri = liveNodes.nextAsURI();
    CompletableFuture<Endpoint> endpoint = futureCache.getOrDefault(uri, null);
    if (endpoint != null) {
      return endpoint;
    }
    endpoint = new CompletableFuture<>();
    endpoint.complete(Endpoint.builder().url(uri).build());
    futureCache.put(uri, endpoint);
    return endpoint;
  }

  /**
   * Returns the AlternatorLiveNodes instance used by this endpoint provider.
   *
   * @return the {@link AlternatorLiveNodes} instance managing the list of live nodes
   * @since 1.0.5
   */
  public AlternatorLiveNodes getAlternatorLiveNodes() {
    return liveNodes;
  }
}
