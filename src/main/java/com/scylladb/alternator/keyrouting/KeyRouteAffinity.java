package com.scylladb.alternator.keyrouting;

/**
 * Specifies the type of operations that should use key-based route affinity.
 *
 * <p>Key route affinity ensures that all requests for the same partition key are routed to the same
 * Alternator node, which improves performance for Lightweight Transactions (LWT) that use Paxos
 * consensus.
 *
 * @author dmitry.kropachev
 * @since 1.0.7
 */
public enum KeyRouteAffinity {
  /**
   * No route affinity optimization - use random load balancing for all requests. This is the
   * default behavior.
   */
  NONE,

  /**
   * Optimize only read-before-write (RMW) operations.
   *
   * <p>This includes:
   *
   * <ul>
   *   <li>UpdateItem with ConditionExpression, Expected, or non-NONE ReturnValues
   *   <li>PutItem with ConditionExpression, Expected, or non-NONE ReturnValues
   *   <li>DeleteItem with ConditionExpression, Expected, or non-NONE ReturnValues
   * </ul>
   *
   * <p>Note: BatchWriteItem is intentionally excluded because it does not support conditional
   * expressions and would require single-table batch handling for proper affinity routing.
   */
  RMW,

  /**
   * Optimize all write operations regardless of conditions.
   *
   * <p>This includes all PutItem, UpdateItem, DeleteItem, and BatchWriteItem requests.
   */
  ANY_WRITE
}
