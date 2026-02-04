package com.scylladb.alternator;

import com.scylladb.alternator.internal.AlternatorLiveNodes;
import com.scylladb.alternator.queryplan.AffinityQueryPlanInterceptor;
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
 * @since 2.0.0
 */
public class AlternatorDynamoDbClientWrapper implements AutoCloseable {

  private final DynamoDbClient client;
  private final AlternatorLiveNodes liveNodes;
  private final AlternatorConfig config;
  private final AffinityQueryPlanInterceptor affinityInterceptor;

  /**
   * Creates a new wrapper with the given client and live nodes.
   *
   * @param client the underlying DynamoDbClient
   * @param liveNodes the AlternatorLiveNodes instance managing node discovery
   */
  public AlternatorDynamoDbClientWrapper(DynamoDbClient client, AlternatorLiveNodes liveNodes) {
    this(client, liveNodes, null, null);
  }

  /**
   * Creates a new wrapper with the given client, live nodes, and config.
   *
   * @param client the underlying DynamoDbClient
   * @param liveNodes the AlternatorLiveNodes instance managing node discovery
   * @param config the AlternatorConfig used for this client
   */
  public AlternatorDynamoDbClientWrapper(
      DynamoDbClient client, AlternatorLiveNodes liveNodes, AlternatorConfig config) {
    this(client, liveNodes, config, null);
  }

  /**
   * Creates a new wrapper with the given client, live nodes, config, and interceptor.
   *
   * @param client the underlying DynamoDbClient
   * @param liveNodes the AlternatorLiveNodes instance managing node discovery
   * @param config the AlternatorConfig used for this client
   * @param affinityInterceptor the AffinityQueryPlanInterceptor (may be null)
   */
  public AlternatorDynamoDbClientWrapper(
      DynamoDbClient client,
      AlternatorLiveNodes liveNodes,
      AlternatorConfig config,
      AffinityQueryPlanInterceptor affinityInterceptor) {
    this.client = client;
    this.liveNodes = liveNodes;
    this.config = config;
    this.affinityInterceptor = affinityInterceptor;

    // Enable auto-discovery by providing the built client to the interceptor
    if (affinityInterceptor != null) {
      affinityInterceptor.setClientForDiscovery(client);
    }
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
   * Returns the AlternatorConfig used to create this client.
   *
   * <p>The config contains all settings including TLS session cache configuration, compression
   * settings, header optimization, and routing scope.
   *
   * @return the {@link AlternatorConfig} instance, or null if not available
   */
  public AlternatorConfig getAlternatorConfig() {
    return config;
  }

  /**
   * Closes the underlying DynamoDbClient and releases associated resources.
   *
   * <p>This includes shutting down the partition key resolver's discovery executor if key route
   * affinity is enabled.
   */
  @Override
  public void close() {
    if (affinityInterceptor != null) {
      affinityInterceptor.getPartitionKeyResolver().shutdown();
    }
    client.close();
  }
}
