/*
 * Copyright ScyllaDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
   * <p>Note: BatchWriteItem is intentionally excluded because it is write-only and does not use LWT
   * in {@code only_rmw_uses_lwt} mode.
   */
  RMW,

  /**
   * Optimize all write operations regardless of conditions.
   *
   * <p>This includes all PutItem, UpdateItem, DeleteItem, and BatchWriteItem requests.
   */
  ANY_WRITE
}
