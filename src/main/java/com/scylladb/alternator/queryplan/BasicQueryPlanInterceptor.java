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
package com.scylladb.alternator.queryplan;

import com.scylladb.alternator.internal.AlternatorLiveNodes;
import com.scylladb.alternator.internal.LazyQueryPlan;
import com.scylladb.alternator.routing.ClusterScope;
import com.scylladb.alternator.routing.RoutingScope;
import java.net.URI;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttribute;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.http.SdkHttpRequest;

/**
 * Execution interceptor that creates a {@link LazyQueryPlan} for each request.
 *
 * <p>This interceptor creates a query plan with a random seed during {@link #beforeExecution},
 * which provides pseudo-random load balancing across available nodes. The plan is stored in {@link
 * ExecutionAttributes} and applied during {@link #modifyHttpRequest} to route the request to the
 * selected node.
 *
 * <p>This approach correctly handles both synchronous and asynchronous clients, as {@link
 * ExecutionAttributes} travel with the request throughout its lifecycle.
 *
 * @author dmitry.kropachev
 * @since 2.0.0
 */
public class BasicQueryPlanInterceptor implements ExecutionInterceptor {

  private static final int MAX_ROUTING_SCOPE_DEPTH = 64;

  /** Execution attribute key for storing the LazyQueryPlan per request. */
  protected static final ExecutionAttribute<LazyQueryPlan> QUERY_PLAN =
      new ExecutionAttribute<>("QueryPlanInterceptor.queryPlan");

  protected final AlternatorLiveNodes liveNodes;

  /**
   * Creates a new interceptor with the given live nodes manager.
   *
   * @param liveNodes the live nodes manager
   */
  public BasicQueryPlanInterceptor(AlternatorLiveNodes liveNodes) {
    this.liveNodes = liveNodes;
  }

  @Override
  public void beforeExecution(
      Context.BeforeExecution context, ExecutionAttributes executionAttributes) {
    liveNodes.recordRequestActivity();
    // Create a query plan with random seed for pseudo-random load balancing
    executionAttributes.putAttribute(QUERY_PLAN, new LazyQueryPlan(liveNodes));
  }

  @Override
  public SdkHttpRequest modifyHttpRequest(
      Context.ModifyHttpRequest context, ExecutionAttributes executionAttributes) {
    LazyQueryPlan plan = executionAttributes.getAttribute(QUERY_PLAN);
    if (plan == null || !plan.hasNext()) {
      if (liveNodes.getLiveNodes().isEmpty()
          || !permitsClusterRouting(liveNodes.getRoutingScope())) {
        throw new IllegalStateException(
            "No eligible live nodes remain for the configured routing scope");
      }
      // The request already exhausted a non-empty plan; preserve the established SDK retry
      // behavior by returning the original endpoint.
      return context.httpRequest();
    }

    URI targetUri = plan.next();
    SdkHttpRequest originalRequest = context.httpRequest();

    // Build new request with the target node's host and port
    return originalRequest.toBuilder()
        .protocol(targetUri.getScheme())
        .host(targetUri.getHost())
        .port(targetUri.getPort())
        .putHeader("Connection", "keep-alive")
        .build();
  }

  private static boolean permitsClusterRouting(RoutingScope scope) {
    Set<RoutingScope> seen = Collections.newSetFromMap(new IdentityHashMap<>());
    RoutingScope current = scope;
    for (int depth = 0; current != null && depth < MAX_ROUTING_SCOPE_DEPTH; depth++) {
      if (!seen.add(current)) {
        return false;
      }
      if (current instanceof ClusterScope) {
        return true;
      }
      current = current.getFallback();
    }
    return false;
  }

  /**
   * Returns the live nodes manager used by this interceptor.
   *
   * @return the live nodes manager
   */
  public AlternatorLiveNodes getLiveNodes() {
    return liveNodes;
  }
}
