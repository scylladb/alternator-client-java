package com.scylladb.alternator.keyrouting;

import com.scylladb.alternator.internal.AlternatorLiveNodes;
import com.scylladb.alternator.internal.LazyQueryPlan;
import java.net.URI;
import software.amazon.awssdk.core.SdkRequest;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * Execution interceptor that implements key-based route affinity.
 *
 * <p>This interceptor examines DynamoDB requests before execution, extracts the partition key when
 * applicable, hashes it, and selects a target node deterministically. The target node is stored in
 * {@link KeyRouteAffinityContext} for the endpoint provider to use.
 *
 * <p>The interceptor is automatically configured when key route affinity is enabled via {@link
 * com.scylladb.alternator.AlternatorDynamoDbClient.AlternatorDynamoDbClientBuilder#withKeyRouteAffinity}.
 *
 * @author dmitry.kropachev
 * @since 1.0.7
 */
public class KeyRouteAffinityInterceptor implements ExecutionInterceptor {

  private final KeyRouteAffinityConfig config;
  private final AlternatorLiveNodes liveNodes;
  private final PartitionKeyResolver pkResolver;
  private volatile DynamoDbClient clientForDiscovery;

  /**
   * Creates a new interceptor with the given configuration and live nodes.
   *
   * @param config the key route affinity configuration
   * @param liveNodes the live nodes manager
   * @param clientForDiscovery the DynamoDB client to use for PK discovery (may be null)
   */
  public KeyRouteAffinityInterceptor(
      KeyRouteAffinityConfig config,
      AlternatorLiveNodes liveNodes,
      DynamoDbClient clientForDiscovery) {
    this.config = config;
    this.liveNodes = liveNodes;
    this.pkResolver = new PartitionKeyResolver(config.getPkInfoPerTable());
    this.clientForDiscovery = clientForDiscovery;
  }

  /**
   * Creates a new interceptor without auto-discovery support.
   *
   * <p>Note: To enable auto-discovery, call {@link #setClientForDiscovery(DynamoDbClient)} after
   * the client is built.
   *
   * @param config the key route affinity configuration
   * @param liveNodes the live nodes manager
   */
  public KeyRouteAffinityInterceptor(KeyRouteAffinityConfig config, AlternatorLiveNodes liveNodes) {
    this(config, liveNodes, null);
  }

  /**
   * Sets the DynamoDB client used for partition key auto-discovery.
   *
   * <p>This method should be called after the client is built to enable auto-discovery of partition
   * key names for tables not pre-configured via {@link
   * KeyRouteAffinityConfig.Builder#withPkInfo(String, String)}.
   *
   * <p>Thread-safe: This method can be called from any thread after construction.
   *
   * @param client the DynamoDB client to use for DescribeTable calls
   */
  public void setClientForDiscovery(DynamoDbClient client) {
    this.clientForDiscovery = client;
  }

  @Override
  public void beforeExecution(
      Context.BeforeExecution context, ExecutionAttributes executionAttributes) {
    // Clear any previous context
    KeyRouteAffinityContext.clear();

    if (!config.isEnabled()) {
      return;
    }

    SdkRequest request = context.request();

    // Check if this request qualifies for key affinity
    if (!KeyAffinityRequestClassifier.shouldApply(config.getType(), request)) {
      return;
    }

    // Extract table name
    String tableName = KeyAffinityRequestClassifier.extractTableName(request);
    if (tableName == null) {
      return;
    }

    // Get partition key name
    String pkName = pkResolver.getPartitionKeyName(tableName);
    if (pkName == null) {
      // Trigger async discovery if we have a client
      if (clientForDiscovery != null) {
        pkResolver.triggerDiscovery(tableName, clientForDiscovery);
      }
      // Fall back to random routing for this request
      return;
    }

    // Extract partition key value
    AttributeValue pkValue = KeyAffinityRequestClassifier.extractPartitionKey(request, pkName);
    if (pkValue == null) {
      return;
    }

    // Hash the partition key and select a node
    long hash = AttributeValueHasher.hash(pkValue);
    LazyQueryPlan plan = liveNodes.newQueryPlan(hash);

    if (plan.hasNext()) {
      URI targetUri = plan.next();
      KeyRouteAffinityContext.setTargetNode(targetUri);
    }
  }

  @Override
  public void afterExecution(
      Context.AfterExecution context, ExecutionAttributes executionAttributes) {
    // Clean up the context
    KeyRouteAffinityContext.clear();
  }

  @Override
  public void onExecutionFailure(
      Context.FailedExecution context, ExecutionAttributes executionAttributes) {
    // Clean up the context on failure as well
    KeyRouteAffinityContext.clear();
  }

  /**
   * Returns the partition key resolver used by this interceptor.
   *
   * @return the partition key resolver
   */
  public PartitionKeyResolver getPartitionKeyResolver() {
    return pkResolver;
  }
}
