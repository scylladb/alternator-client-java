package com.scylladb.alternator.queryplan;

import com.scylladb.alternator.NodeHealthObservation;
import com.scylladb.alternator.internal.AlternatorLiveNodes;
import com.scylladb.alternator.internal.LazyQueryPlan;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.Set;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttribute;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.SdkHttpResponse;

/**
 * Execution interceptor that creates a {@link LazyQueryPlan} for each request.
 *
 * <p>This interceptor creates a non-deterministic query plan during {@link #beforeExecution}, which
 * provides load balancing over query-plan nodes supplied by {@link AlternatorLiveNodes}. The plan
 * is stored in {@link ExecutionAttributes} and applied during {@link #modifyHttpRequest}.
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

  private static final ExecutionAttribute<URI> IN_FLIGHT_NODE =
      new ExecutionAttribute<>("QueryPlanInterceptor.inFlightNode");

  private static final ExecutionAttribute<URI> PENDING_HTTP_RESPONSE_NODE =
      new ExecutionAttribute<>("QueryPlanInterceptor.pendingHttpResponseNode");

  private static final ExecutionAttribute<AlternatorLiveNodes.QueryPlanNodeFilter>
      ROUTING_NODE_FILTER = new ExecutionAttribute<>("QueryPlanInterceptor.routingNodeFilter");

  private static final ExecutionAttribute<Deque<URI>> SKIPPED_ROUTE_CANDIDATES =
      new ExecutionAttribute<>("QueryPlanInterceptor.skippedRouteCandidates");

  private static final int HTTP_UNAUTHORIZED = 401;
  private static final int HTTP_FORBIDDEN = 403;
  private static final Set<String> AUTHENTICATION_ERROR_CODES =
      Collections.unmodifiableSet(
          new HashSet<>(
              Arrays.asList(
                  "InvalidSignatureException",
                  "MissingAuthenticationTokenException",
                  "UnrecognizedClientException",
                  "SignatureDoesNotMatch",
                  "IncompleteSignatureException",
                  "InvalidClientTokenId",
                  "InvalidAccessKeyId",
                  "ExpiredTokenException",
                  "RequestExpired",
                  "TokenRefreshRequired")));

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
    // Create a non-deterministic query plan for load balancing over known nodes.
    executionAttributes.putAttribute(QUERY_PLAN, new LazyQueryPlan(liveNodes));
    executionAttributes.putAttribute(ROUTING_NODE_FILTER, liveNodes.newQueryPlanNodeFilter());
    executionAttributes.putAttribute(SKIPPED_ROUTE_CANDIDATES, new ArrayDeque<>());
  }

  @Override
  public SdkHttpRequest modifyHttpRequest(
      Context.ModifyHttpRequest context, ExecutionAttributes executionAttributes) {
    LazyQueryPlan plan = executionAttributes.getAttribute(QUERY_PLAN);
    SdkHttpRequest originalRequest = context.httpRequest();
    if (plan == null) {
      return originalRequest;
    }

    AlternatorLiveNodes.QueryPlanNodeFilter routingFilter =
        executionAttributes.getAttribute(ROUTING_NODE_FILTER);
    if (routingFilter == null) {
      routingFilter = liveNodes.newQueryPlanNodeFilter();
      executionAttributes.putAttribute(ROUTING_NODE_FILTER, routingFilter);
    }

    URI targetUri =
        routingFilter.nextRouteCandidate(plan, skippedRouteCandidates(executionAttributes));
    if (targetUri == null) {
      URI originalEndpoint = requestEndpoint(originalRequest);
      if (routingFilter.shouldRouteTo(originalEndpoint)) {
        return originalRequest;
      }
      throw new IllegalStateException("No live nodes available");
    }

    // Build new request with the target node's host and port
    return originalRequest.toBuilder()
        .protocol(targetUri.getScheme())
        .host(targetUri.getHost())
        .port(targetUri.getPort())
        .putHeader("Connection", "keep-alive")
        .build();
  }

  private Deque<URI> skippedRouteCandidates(ExecutionAttributes executionAttributes) {
    Deque<URI> skippedCandidates = executionAttributes.getAttribute(SKIPPED_ROUTE_CANDIDATES);
    if (skippedCandidates == null) {
      skippedCandidates = new ArrayDeque<>();
      executionAttributes.putAttribute(SKIPPED_ROUTE_CANDIDATES, skippedCandidates);
    }
    return skippedCandidates;
  }

  private URI requestEndpoint(SdkHttpRequest request) {
    try {
      return new URI(request.protocol(), null, request.host(), request.port(), null, null, null);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid request endpoint: " + request.getUri(), e);
    }
  }

  private void reportInFlightTransportFailure(ExecutionAttributes executionAttributes) {
    reportPendingHttpResponseResult(executionAttributes, NodeHealthObservation.TRAFFIC_SUCCESS);
    reportInFlightNodeResult(executionAttributes, NodeHealthObservation.TRAFFIC_FAILURE);
  }

  private void reportInFlightNodeResult(
      ExecutionAttributes executionAttributes, NodeHealthObservation observation) {
    URI node = executionAttributes.getAttribute(IN_FLIGHT_NODE);
    if (node == null) {
      return;
    }
    executionAttributes.putAttribute(IN_FLIGHT_NODE, null);
    liveNodes.reportNodeResult(node, observation);
  }

  private void deferInFlightHttpResponse(ExecutionAttributes executionAttributes) {
    URI node = executionAttributes.getAttribute(IN_FLIGHT_NODE);
    if (node == null) {
      return;
    }
    executionAttributes.putAttribute(IN_FLIGHT_NODE, null);
    executionAttributes.putAttribute(PENDING_HTTP_RESPONSE_NODE, node);
  }

  private void reportPendingHttpResponseResult(
      ExecutionAttributes executionAttributes, NodeHealthObservation observation) {
    URI node = executionAttributes.getAttribute(PENDING_HTTP_RESPONSE_NODE);
    if (node == null) {
      return;
    }
    executionAttributes.putAttribute(PENDING_HTTP_RESPONSE_NODE, null);
    liveNodes.reportNodeResult(node, observation);
  }

  private void reportHttpResponseResult(
      ExecutionAttributes executionAttributes, NodeHealthObservation observation) {
    URI pendingNode = executionAttributes.getAttribute(PENDING_HTTP_RESPONSE_NODE);
    if (pendingNode != null) {
      reportPendingHttpResponseResult(executionAttributes, observation);
      return;
    }
    reportInFlightNodeResult(executionAttributes, observation);
  }

  private boolean isAuthenticationStatus(SdkHttpResponse response) {
    int statusCode = response.statusCode();
    return statusCode == HTTP_UNAUTHORIZED || statusCode == HTTP_FORBIDDEN;
  }

  private boolean shouldDeferHttpResponseObservation(SdkHttpResponse response) {
    return response.statusCode() >= 400 && !isAuthenticationStatus(response);
  }

  private NodeHealthObservation httpResponseObservation(SdkHttpResponse response) {
    return isAuthenticationStatus(response)
        ? NodeHealthObservation.TRAFFIC_FAILURE
        : NodeHealthObservation.TRAFFIC_SUCCESS;
  }

  private NodeHealthObservation failedExecutionObservation(Context.FailedExecution context) {
    if (context.httpResponse().isPresent()
        && isAuthenticationStatus(context.httpResponse().get())) {
      return NodeHealthObservation.TRAFFIC_FAILURE;
    }
    return isAuthenticationException(context.exception())
        ? NodeHealthObservation.TRAFFIC_FAILURE
        : NodeHealthObservation.TRAFFIC_SUCCESS;
  }

  private boolean isAuthenticationException(Throwable exception) {
    Throwable current = exception;
    while (current != null) {
      if (current instanceof AwsServiceException
          && isAuthenticationErrorCode(((AwsServiceException) current).awsErrorDetails())) {
        return true;
      }

      Throwable cause = current.getCause();
      if (cause == current) {
        return false;
      }
      current = cause;
    }
    return false;
  }

  private boolean isAuthenticationErrorCode(AwsErrorDetails details) {
    if (details == null || details.errorCode() == null) {
      return false;
    }

    return AUTHENTICATION_ERROR_CODES.contains(errorCodeName(details.errorCode()));
  }

  private String errorCodeName(String errorCode) {
    int separator = Math.max(errorCode.lastIndexOf('#'), errorCode.lastIndexOf(':'));
    if (separator >= 0 && separator + 1 < errorCode.length()) {
      return errorCode.substring(separator + 1);
    }
    return errorCode;
  }

  @Override
  public void beforeTransmission(
      Context.BeforeTransmission context, ExecutionAttributes executionAttributes) {
    reportInFlightTransportFailure(executionAttributes);
    executionAttributes.putAttribute(IN_FLIGHT_NODE, requestEndpoint(context.httpRequest()));
  }

  @Override
  public void afterTransmission(
      Context.AfterTransmission context, ExecutionAttributes executionAttributes) {
    if (shouldDeferHttpResponseObservation(context.httpResponse())) {
      deferInFlightHttpResponse(executionAttributes);
      return;
    }
    reportHttpResponseResult(executionAttributes, httpResponseObservation(context.httpResponse()));
  }

  @Override
  public void onExecutionFailure(
      Context.FailedExecution context, ExecutionAttributes executionAttributes) {
    if (context.httpResponse().isPresent()) {
      reportHttpResponseResult(executionAttributes, failedExecutionObservation(context));
      return;
    }
    if (isAuthenticationException(context.exception())) {
      reportHttpResponseResult(executionAttributes, NodeHealthObservation.TRAFFIC_FAILURE);
      return;
    }
    reportInFlightTransportFailure(executionAttributes);
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
