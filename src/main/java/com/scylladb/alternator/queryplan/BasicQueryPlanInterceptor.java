package com.scylladb.alternator.queryplan;

import com.scylladb.alternator.internal.AlternatorLiveNodes;
import com.scylladb.alternator.internal.LazyQueryPlan;
import java.net.URI;
import java.util.List;
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
    // Always install a plan: even single-node clusters need it because the inbound request
    // points at a placeholder host and modifyHttpRequest rewrites it to the real node URL.
    // The plan's first next() call is O(1) for single-node clusters (see LazyQueryPlan).
    executionAttributes.putAttribute(QUERY_PLAN, new LazyQueryPlan(liveNodes));
  }

  @Override
  public SdkHttpRequest modifyHttpRequest(
      Context.ModifyHttpRequest context, ExecutionAttributes executionAttributes) {
    LazyQueryPlan plan = executionAttributes.getAttribute(QUERY_PLAN);
    if (plan == null || !plan.hasNext()) {
      // No plan available, return original request
      return context.httpRequest();
    }

    URI targetUri = plan.next();
    SdkHttpRequest originalRequest = context.httpRequest();

    // Build new request with the target node's host and port. Only set keep-alive if the
    // SDK hasn't already (the Apache and CRT clients add it by default); writing the same
    // header again forces the SDK to recopy the header list.
    SdkHttpRequest.Builder builder =
        originalRequest.toBuilder()
            .protocol(targetUri.getScheme())
            .host(targetUri.getHost())
            .port(targetUri.getPort());
    List<String> existing = originalRequest.matchingHeaders("Connection");
    if (existing == null || existing.isEmpty()) {
      builder.putHeader("Connection", "keep-alive");
    }
    return builder.build();
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
