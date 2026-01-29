package com.scylladb.alternator;

import com.scylladb.alternator.internal.AlternatorLiveNodes;
import java.net.URI;
import java.util.List;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/**
 * A wrapper that holds a DynamoDbClient along with Alternator-specific functionality.
 *
 * <p>This class provides access to the underlying {@link DynamoDbClient} via {@link #getClient()},
 * and adds methods to access Alternator infrastructure such as the list of live nodes for load
 * balancing.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * AlternatorDynamoDbClientWrapper wrapper = AlternatorDynamoDbClient.builder()
 *     .endpointOverride(URI.create("http://localhost:8000"))
 *     .credentialsProvider(credentialsProvider)
 *     .build();
 *
 * // Get the DynamoDB client for normal operations
 * DynamoDbClient client = wrapper.getClient();
 * client.createTable(...);
 *
 * // Access Alternator-specific functionality
 * List<URI> nodes = wrapper.getLiveNodes();
 * URI nextNode = wrapper.nextAsURI();
 * }</pre>
 *
 * @author dmitry.kropachev
 * @since 1.0.5
 */
public class AlternatorDynamoDbClientWrapper implements AutoCloseable {

  private final DynamoDbClient client;
  private final AlternatorLiveNodes liveNodes;
  private final AlternatorEndpointProvider endpointProvider;

  /**
   * Creates a new wrapper with the given client, live nodes, and endpoint provider.
   *
   * @param client the underlying DynamoDbClient
   * @param liveNodes the AlternatorLiveNodes instance managing node discovery
   * @param endpointProvider the AlternatorEndpointProvider used for this client
   */
  public AlternatorDynamoDbClientWrapper(
      DynamoDbClient client,
      AlternatorLiveNodes liveNodes,
      AlternatorEndpointProvider endpointProvider) {
    this.client = client;
    this.liveNodes = liveNodes;
    this.endpointProvider = endpointProvider;
  }

  /**
   * Returns the underlying DynamoDbClient.
   *
   * @return the DynamoDbClient instance
   */
  public DynamoDbClient getClient() {
    return client;
  }

  /**
   * Returns a snapshot of the current live nodes list.
   *
   * @return an unmodifiable list of the current live node URIs
   */
  public List<URI> getLiveNodes() {
    return liveNodes.getLiveNodes();
  }

  /**
   * Returns the next node URI using round-robin selection.
   *
   * @return the next {@link URI} in the round-robin sequence
   */
  public URI nextAsURI() {
    return liveNodes.nextAsURI();
  }

  /**
   * Checks if the server supports rack/datacenter filtering for node discovery.
   *
   * @return true if rack/datacenter filtering is supported, false otherwise
   * @throws AlternatorLiveNodes.FailedToCheck if the check cannot be completed
   */
  public boolean checkIfRackDatacenterFeatureIsSupported()
      throws AlternatorLiveNodes.FailedToCheck {
    return liveNodes.checkIfRackDatacenterFeatureIsSupported();
  }

  /**
   * Returns the AlternatorLiveNodes instance used for node discovery.
   *
   * @return the {@link AlternatorLiveNodes} instance
   */
  public AlternatorLiveNodes getAlternatorLiveNodes() {
    return liveNodes;
  }

  /**
   * Returns the AlternatorEndpointProvider used for endpoint resolution.
   *
   * <p>The endpoint provider is responsible for selecting which node to use for each request.
   *
   * @return the {@link AlternatorEndpointProvider} instance
   */
  public AlternatorEndpointProvider getAlternatorEndpointProvider() {
    return endpointProvider;
  }

  /** Closes the underlying DynamoDbClient. */
  @Override
  public void close() {
    client.close();
  }
}
