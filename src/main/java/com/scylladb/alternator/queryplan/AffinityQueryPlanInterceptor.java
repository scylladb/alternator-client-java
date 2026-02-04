package com.scylladb.alternator.queryplan;

import com.scylladb.alternator.internal.AlternatorLiveNodes;
import com.scylladb.alternator.internal.LazyQueryPlan;
import com.scylladb.alternator.keyrouting.AttributeValueHasher;
import com.scylladb.alternator.keyrouting.KeyAffinityRequestClassifier;
import com.scylladb.alternator.keyrouting.KeyRouteAffinityConfig;
import com.scylladb.alternator.keyrouting.PartitionKeyResolver;
import software.amazon.awssdk.core.SdkRequest;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * Execution interceptor that implements key-based route affinity.
 *
 * <p>This interceptor extends {@link BasicQueryPlanInterceptor} to provide deterministic routing
 * based on partition key values. When key affinity conditions are met, it creates a {@link
 * LazyQueryPlan} with a seed derived from the partition key hash, ensuring that requests for the
 * same partition key are routed to the same node.
 *
 * <p>When key affinity conditions are not met (e.g., request type doesn't qualify, partition key
 * not found), the interceptor falls back to the random plan created by the base class.
 *
 * <p>The interceptor is automatically configured when key route affinity is enabled via {@link
 * com.scylladb.alternator.AlternatorDynamoDbClient.AlternatorDynamoDbClientBuilder#withKeyRouteAffinity}.
 *
 * @author dmitry.kropachev
 * @since 2.0.0
 */
public class AffinityQueryPlanInterceptor extends BasicQueryPlanInterceptor {

  private final KeyRouteAffinityConfig config;
  private final PartitionKeyResolver pkResolver;
  private volatile DynamoDbClient clientForDiscovery;

  /**
   * Creates a new interceptor with the given configuration and live nodes.
   *
   * @param config the key route affinity configuration
   * @param liveNodes the live nodes manager
   * @param clientForDiscovery the DynamoDB client to use for PK discovery (may be null)
   */
  public AffinityQueryPlanInterceptor(
      KeyRouteAffinityConfig config,
      AlternatorLiveNodes liveNodes,
      DynamoDbClient clientForDiscovery) {
    super(liveNodes);
    this.config = config;
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
  public AffinityQueryPlanInterceptor(
      KeyRouteAffinityConfig config, AlternatorLiveNodes liveNodes) {
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

  private LazyQueryPlan getQueryPlan(SdkRequest request) {
    if (!config.isEnabled()) {
      return null;
    }

    // Check if this request qualifies for key affinity
    if (!KeyAffinityRequestClassifier.shouldApply(config.getType(), request)) {
      // Keep the random plan from base class
      return null;
    }

    // Extract table name
    String tableName = KeyAffinityRequestClassifier.extractTableName(request);
    if (tableName == null) {
      // Keep the random plan from base class
      return null;
    }

    // Get partition key name
    String pkName = pkResolver.getPartitionKeyName(tableName);
    if (pkName == null) {
      // Trigger async discovery if we have a client
      if (clientForDiscovery != null) {
        pkResolver.triggerDiscovery(tableName, clientForDiscovery);
      }
      // Keep the random plan from base class for this request
      return null;
    }

    // Extract partition key value
    AttributeValue pkValue = KeyAffinityRequestClassifier.extractPartitionKey(request, pkName);
    if (pkValue == null) {
      // Keep the random plan from base class
      return null;
    }

    // Hash the partition key and create a deterministic query plan
    long hash = AttributeValueHasher.hash(pkValue);
    return new LazyQueryPlan(liveNodes, hash);
  }

  @Override
  public void beforeExecution(
      Context.BeforeExecution context, ExecutionAttributes executionAttributes) {
    LazyQueryPlan plan = getQueryPlan(context.request());
    if (plan == null) {
      plan = new LazyQueryPlan(liveNodes);
    }

    // Override the random plan with the deterministic one
    executionAttributes.putAttribute(QUERY_PLAN, plan);
  }

  /**
   * Returns the partition key resolver used by this interceptor.
   *
   * @return the partition key resolver
   */
  public PartitionKeyResolver getPartitionKeyResolver() {
    return pkResolver;
  }

  /**
   * Returns the key route affinity configuration.
   *
   * @return the configuration
   */
  public KeyRouteAffinityConfig getConfig() {
    return config;
  }
}
