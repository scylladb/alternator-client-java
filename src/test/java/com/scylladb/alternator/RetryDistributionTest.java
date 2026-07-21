package com.scylladb.alternator;

import static org.junit.Assert.*;

import com.scylladb.alternator.internal.AlternatorLiveNodes;
import com.scylladb.alternator.internal.LazyQueryPlan;
import com.scylladb.alternator.queryplan.BasicQueryPlanInterceptor;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;
import org.junit.Test;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.core.SdkRequest;
import software.amazon.awssdk.core.SdkResponse;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.http.SdkHttpFullResponse;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;

/**
 * Tests that verify retry distribution across nodes via {@link
 * com.scylladb.alternator.internal.LazyQueryPlan}.
 *
 * <p>When a request fails and is retried, the {@link BasicQueryPlanInterceptor} calls {@code
 * plan.next()} on each invocation of {@code modifyHttpRequest}. Since {@link
 * com.scylladb.alternator.internal.LazyQueryPlan} returns nodes without duplicates, each retry
 * attempt is routed to a different node. This test verifies that behavior by:
 *
 * <ul>
 *   <li>Using a mock AlternatorLiveNodes with a controlled set of nodes
 *   <li>Simulating the interceptor lifecycle: one {@code beforeExecution} call followed by multiple
 *       {@code modifyHttpRequest} calls (one per attempt)
 *   <li>Verifying that captured request URIs show correct node distribution across retries
 * </ul>
 *
 * @author dmitry.kropachev
 */
public class RetryDistributionTest {

  /**
   * Helper that simulates the interceptor lifecycle for a single SDK call with retries.
   *
   * <p>The SDK calls {@code beforeExecution} once per API call, which creates a {@link
   * com.scylladb.alternator.internal.LazyQueryPlan} stored in {@link ExecutionAttributes}. Then
   * {@code modifyHttpRequest} is called for each attempt (initial + retries), and each call to
   * {@code plan.next()} returns a different node.
   *
   * @param interceptor the query plan interceptor
   * @param totalAttempts the total number of attempts (1 = initial only, 2 = initial + 1 retry,
   *     etc.)
   * @return the list of URIs selected for each attempt
   */
  private List<URI> simulateRequestWithRetries(
      BasicQueryPlanInterceptor interceptor, int totalAttempts) throws Exception {
    ExecutionAttributes executionAttributes = ExecutionAttributes.builder().build();

    // beforeExecution is called once per SDK call - creates the LazyQueryPlan
    interceptor.beforeExecution(null, executionAttributes);

    // Build a base HTTP request to be modified by the interceptor
    SdkHttpRequest baseRequest =
        SdkHttpRequest.builder()
            .protocol("http")
            .host("placeholder")
            .port(8000)
            .method(software.amazon.awssdk.http.SdkHttpMethod.POST)
            .encodedPath("/")
            .build();

    // Create a mock context that returns the base request
    MockModifyHttpRequestContext context = new MockModifyHttpRequestContext(baseRequest);

    List<URI> selectedNodes = new ArrayList<>();
    for (int attempt = 0; attempt < totalAttempts; attempt++) {
      // modifyHttpRequest is called for each attempt (initial + retries)
      SdkHttpRequest modifiedRequest = interceptor.modifyHttpRequest(context, executionAttributes);
      selectedNodes.add(
          new URI(
              modifiedRequest.protocol(),
              null,
              modifiedRequest.host(),
              modifiedRequest.port(),
              null,
              null,
              null));
    }
    return selectedNodes;
  }

  /** Helper to create a list of test node URIs. */
  private List<URI> createNodes(int count) throws Exception {
    List<URI> nodes = new ArrayList<>();
    for (int i = 1; i <= count; i++) {
      nodes.add(new URI("http://127.0.0." + i + ":8000"));
    }
    return nodes;
  }

  // ========== Test: 1 node, 0 retries -> 1 request to single node ==========

  /**
   * With a single node and no retries, exactly one request should be made to the only available
   * node.
   */
  @Test
  public void testSingleNodeNoRetries() throws Exception {
    List<URI> nodes = createNodes(1);
    MockAlternatorLiveNodes liveNodes = new MockAlternatorLiveNodes(nodes);
    BasicQueryPlanInterceptor interceptor = new BasicQueryPlanInterceptor(liveNodes);

    List<URI> selectedNodes = simulateRequestWithRetries(interceptor, 1);

    assertEquals("Should select exactly 1 node", 1, selectedNodes.size());
    assertEquals(
        "Request should go to the single available node",
        "127.0.0.1",
        selectedNodes.get(0).getHost());
    assertEquals("Request should use the correct port", 8000, selectedNodes.get(0).getPort());
  }

  // ========== Test: 2 nodes, 1 retry -> request to node1, retry to node2 ==========

  /**
   * With two nodes and one retry, the initial request goes to one node and the retry goes to the
   * other node. LazyQueryPlan ensures no duplicates.
   */
  @Test
  public void testTwoNodesOneRetry() throws Exception {
    List<URI> nodes = createNodes(2);
    MockAlternatorLiveNodes liveNodes = new MockAlternatorLiveNodes(nodes);
    BasicQueryPlanInterceptor interceptor = new BasicQueryPlanInterceptor(liveNodes);

    List<URI> selectedNodes = simulateRequestWithRetries(interceptor, 2);

    assertEquals("Should select 2 nodes (initial + 1 retry)", 2, selectedNodes.size());

    Set<String> uniqueHosts = selectedNodes.stream().map(URI::getHost).collect(Collectors.toSet());
    assertEquals(
        "Retry should go to a different node than the initial request", 2, uniqueHosts.size());

    // Verify both nodes were used
    assertTrue("Should include node 127.0.0.1", uniqueHosts.contains("127.0.0.1"));
    assertTrue("Should include node 127.0.0.2", uniqueHosts.contains("127.0.0.2"));
  }

  // ========== Test: 3 nodes, 2 retries -> request fans across 3 nodes ==========

  /**
   * With three nodes and two retries, each attempt should go to a different node. The initial
   * request and both retries should each target a unique node.
   */
  @Test
  public void testThreeNodesTwoRetries() throws Exception {
    List<URI> nodes = createNodes(3);
    MockAlternatorLiveNodes liveNodes = new MockAlternatorLiveNodes(nodes);
    BasicQueryPlanInterceptor interceptor = new BasicQueryPlanInterceptor(liveNodes);

    List<URI> selectedNodes = simulateRequestWithRetries(interceptor, 3);

    assertEquals("Should select 3 nodes (initial + 2 retries)", 3, selectedNodes.size());

    Set<String> uniqueHosts = selectedNodes.stream().map(URI::getHost).collect(Collectors.toSet());
    assertEquals("Each attempt should go to a different node", 3, uniqueHosts.size());

    // Verify all three nodes were used
    assertTrue("Should include node 127.0.0.1", uniqueHosts.contains("127.0.0.1"));
    assertTrue("Should include node 127.0.0.2", uniqueHosts.contains("127.0.0.2"));
    assertTrue("Should include node 127.0.0.3", uniqueHosts.contains("127.0.0.3"));
  }

  // ========== Test: 1 node, 2 retries -> all retries to same node ==========

  /**
   * With only one node and two retries, all attempts must go to the same node since it is the only
   * one available. After the LazyQueryPlan is exhausted, modifyHttpRequest returns the original
   * request only when that endpoint is still eligible for routing.
   */
  @Test
  public void testSingleNodeMultipleRetries() throws Exception {
    List<URI> nodes = createNodes(1);
    MockAlternatorLiveNodes liveNodes = new MockAlternatorLiveNodes(nodes);
    BasicQueryPlanInterceptor interceptor = new BasicQueryPlanInterceptor(liveNodes);

    // First attempt gets the only node from the plan
    // Subsequent attempts: plan is exhausted (hasNext() = false), so interceptor
    // returns the original request unchanged, which still points to the placeholder host.
    // In a real SDK scenario, the endpointOverride would point to the seed node.
    List<URI> selectedNodes = simulateRequestWithRetries(interceptor, 1);

    assertEquals("Should select 1 node on initial attempt", 1, selectedNodes.size());
    assertEquals(
        "Request should go to the single node", "127.0.0.1", selectedNodes.get(0).getHost());

    // Verify that after exhaustion, the plan reports no more nodes
    ExecutionAttributes attrs = ExecutionAttributes.builder().build();
    interceptor.beforeExecution(null, attrs);

    SdkHttpRequest baseRequest =
        SdkHttpRequest.builder()
            .protocol("http")
            .host("127.0.0.1")
            .port(8000)
            .method(software.amazon.awssdk.http.SdkHttpMethod.POST)
            .encodedPath("/")
            .build();
    MockModifyHttpRequestContext context = new MockModifyHttpRequestContext(baseRequest);

    // First call consumes the only node
    SdkHttpRequest first = interceptor.modifyHttpRequest(context, attrs);
    assertEquals("First attempt goes to the node", "127.0.0.1", first.host());

    // Second call: plan exhausted, returns original request (same host)
    SdkHttpRequest second = interceptor.modifyHttpRequest(context, attrs);
    assertEquals(
        "When plan is exhausted, returns original request host", "127.0.0.1", second.host());

    // Third call: still exhausted, same behavior
    SdkHttpRequest third = interceptor.modifyHttpRequest(context, attrs);
    assertEquals(
        "Repeated calls after exhaustion return original request host", "127.0.0.1", third.host());
  }

  // ========== Test: verify actual node distribution via captured request URIs ==========

  /**
   * Verifies retry distribution by running multiple independent requests and checking that each
   * request's retry attempts are spread across different nodes. This uses the full mock HTTP client
   * to capture actual request URIs sent by the interceptor.
   */
  @Test
  public void testRetryDistributionAcrossMultipleRequests() throws Exception {
    List<URI> nodes = createNodes(5);
    MockAlternatorLiveNodes liveNodes = new MockAlternatorLiveNodes(nodes);
    BasicQueryPlanInterceptor interceptor = new BasicQueryPlanInterceptor(liveNodes);

    // Simulate 10 independent requests, each with up to 4 retries (5 total attempts)
    for (int request = 0; request < 10; request++) {
      List<URI> selectedNodes = simulateRequestWithRetries(interceptor, 5);

      assertEquals(
          "Request " + request + ": should select 5 nodes (all available)",
          5,
          selectedNodes.size());

      Set<String> uniqueHosts =
          selectedNodes.stream().map(URI::getHost).collect(Collectors.toSet());
      assertEquals(
          "Request " + request + ": all 5 attempts should go to different nodes",
          5,
          uniqueHosts.size());
    }
  }

  /**
   * Verifies that with more retry attempts than available nodes, the plan is exhausted and
   * subsequent attempts return the original request when that endpoint is still eligible. This
   * tests the boundary condition where retries exceed the node count.
   */
  @Test
  public void testRetriesExceedNodeCount() throws Exception {
    List<URI> nodes = createNodes(2);
    MockAlternatorLiveNodes liveNodes = new MockAlternatorLiveNodes(nodes);
    BasicQueryPlanInterceptor interceptor = new BasicQueryPlanInterceptor(liveNodes);

    ExecutionAttributes attrs = ExecutionAttributes.builder().build();
    interceptor.beforeExecution(null, attrs);

    SdkHttpRequest baseRequest =
        SdkHttpRequest.builder()
            .protocol("http")
            .host("fallback-host")
            .port(9999)
            .method(software.amazon.awssdk.http.SdkHttpMethod.POST)
            .encodedPath("/")
            .build();
    MockModifyHttpRequestContext context = new MockModifyHttpRequestContext(baseRequest);

    // First two calls get different nodes from the plan
    SdkHttpRequest first = interceptor.modifyHttpRequest(context, attrs);
    SdkHttpRequest second = interceptor.modifyHttpRequest(context, attrs);
    assertNotEquals("First two attempts should go to different nodes", first.host(), second.host());

    // Third call: plan exhausted, falls back to original request
    SdkHttpRequest third = interceptor.modifyHttpRequest(context, attrs);
    assertEquals(
        "After plan exhaustion, returns original request host", "fallback-host", third.host());
    assertEquals("After plan exhaustion, returns original request port", 9999, third.port());
  }

  @Test
  public void testRetryReportsPreviousAttemptTransportFailure() throws Exception {
    List<URI> nodes = createNodes(2);
    MockAlternatorLiveNodes liveNodes = new MockAlternatorLiveNodes(nodes);
    BasicQueryPlanInterceptor interceptor = new BasicQueryPlanInterceptor(liveNodes);

    ExecutionAttributes attrs = ExecutionAttributes.builder().build();
    interceptor.beforeExecution(null, attrs);
    MockModifyHttpRequestContext context = new MockModifyHttpRequestContext(baseRequest());

    SdkHttpRequest first = interceptor.modifyHttpRequest(context, attrs);
    interceptor.beforeTransmission(new MockBeforeTransmissionContext(first), attrs);
    assertTrue(liveNodes.reports.isEmpty());

    interceptor.beforeTransmission(new MockBeforeTransmissionContext(first), attrs);

    URI firstNode = endpoint(first);
    assertEquals(1, liveNodes.reports.size());
    assertEquals(
        new NodeReport(firstNode, NodeHealthObservation.TRAFFIC_FAILURE), liveNodes.reports.get(0));
  }

  @Test
  public void testDynamoDbHttpErrorResponseClearsPendingTransportFailure() throws Exception {
    List<URI> nodes = createNodes(2);
    MockAlternatorLiveNodes liveNodes = new MockAlternatorLiveNodes(nodes);
    BasicQueryPlanInterceptor interceptor = new BasicQueryPlanInterceptor(liveNodes);

    ExecutionAttributes attrs = ExecutionAttributes.builder().build();
    interceptor.beforeExecution(null, attrs);
    MockModifyHttpRequestContext context = new MockModifyHttpRequestContext(baseRequest());

    SdkHttpRequest first = interceptor.modifyHttpRequest(context, attrs);
    interceptor.beforeTransmission(new MockBeforeTransmissionContext(first), attrs);
    interceptor.afterTransmission(new MockAfterTransmissionContext(first, 500), attrs);
    assertTrue(liveNodes.reports.isEmpty());
    interceptor.onExecutionFailure(
        new MockFailedExecutionContext(first, 500, dynamoDbException(500, "InternalServerError")),
        attrs);

    SdkHttpRequest second = interceptor.modifyHttpRequest(context, attrs);
    interceptor.beforeTransmission(new MockBeforeTransmissionContext(second), attrs);

    URI firstNode = endpoint(first);
    URI secondNode = endpoint(second);
    assertNotEquals(firstNode, secondNode);
    assertEquals(1, liveNodes.reports.size());
    assertEquals(
        new NodeReport(firstNode, NodeHealthObservation.TRAFFIC_SUCCESS), liveNodes.reports.get(0));
  }

  @Test
  public void testRetryableHttpResponseClearsBeforeNextRetryWithoutFailedExecution()
      throws Exception {
    List<URI> nodes = createNodes(2);
    MockAlternatorLiveNodes liveNodes = new MockAlternatorLiveNodes(nodes);
    BasicQueryPlanInterceptor interceptor = new BasicQueryPlanInterceptor(liveNodes);

    ExecutionAttributes attrs = ExecutionAttributes.builder().build();
    interceptor.beforeExecution(null, attrs);
    MockModifyHttpRequestContext context = new MockModifyHttpRequestContext(baseRequest());

    SdkHttpRequest first = interceptor.modifyHttpRequest(context, attrs);
    interceptor.beforeTransmission(new MockBeforeTransmissionContext(first), attrs);
    interceptor.afterTransmission(new MockAfterTransmissionContext(first, 500), attrs);
    assertTrue(liveNodes.reports.isEmpty());

    SdkHttpRequest second = interceptor.modifyHttpRequest(context, attrs);
    interceptor.beforeTransmission(new MockBeforeTransmissionContext(second), attrs);

    URI firstNode = endpoint(first);
    URI secondNode = endpoint(second);
    assertNotEquals(firstNode, secondNode);
    assertEquals(1, liveNodes.reports.size());
    assertEquals(
        new NodeReport(firstNode, NodeHealthObservation.TRAFFIC_SUCCESS), liveNodes.reports.get(0));
  }

  @Test
  public void testDynamoDbAuthenticationErrorCodeCountsAsFailure() throws Exception {
    List<URI> nodes = createNodes(2);
    MockAlternatorLiveNodes liveNodes = new MockAlternatorLiveNodes(nodes);
    BasicQueryPlanInterceptor interceptor = new BasicQueryPlanInterceptor(liveNodes);

    ExecutionAttributes attrs = ExecutionAttributes.builder().build();
    interceptor.beforeExecution(null, attrs);
    MockModifyHttpRequestContext context = new MockModifyHttpRequestContext(baseRequest());

    SdkHttpRequest first = interceptor.modifyHttpRequest(context, attrs);
    interceptor.beforeTransmission(new MockBeforeTransmissionContext(first), attrs);
    interceptor.afterTransmission(new MockAfterTransmissionContext(first, 400), attrs);
    assertTrue(liveNodes.reports.isEmpty());
    interceptor.onExecutionFailure(
        new MockFailedExecutionContext(
            first,
            400,
            dynamoDbException(
                400, "com.amazonaws.dynamodb.v20120810#MissingAuthenticationTokenException")),
        attrs);

    URI firstNode = endpoint(first);
    assertEquals(1, liveNodes.reports.size());
    assertEquals(
        new NodeReport(firstNode, NodeHealthObservation.TRAFFIC_FAILURE), liveNodes.reports.get(0));
  }

  @Test
  public void testDynamoDbAuthenticationHttpStatusCountsAsFailure() throws Exception {
    List<URI> nodes = createNodes(2);
    MockAlternatorLiveNodes liveNodes = new MockAlternatorLiveNodes(nodes);
    BasicQueryPlanInterceptor interceptor = new BasicQueryPlanInterceptor(liveNodes);

    ExecutionAttributes attrs = ExecutionAttributes.builder().build();
    interceptor.beforeExecution(null, attrs);
    MockModifyHttpRequestContext context = new MockModifyHttpRequestContext(baseRequest());

    SdkHttpRequest first = interceptor.modifyHttpRequest(context, attrs);
    interceptor.beforeTransmission(new MockBeforeTransmissionContext(first), attrs);
    interceptor.afterTransmission(new MockAfterTransmissionContext(first, 403), attrs);

    URI firstNode = endpoint(first);
    assertEquals(1, liveNodes.reports.size());
    assertEquals(
        new NodeReport(firstNode, NodeHealthObservation.TRAFFIC_FAILURE), liveNodes.reports.get(0));
  }

  @Test
  public void testModifyHttpRequestSkipsDownCandidateAtFinalGate() throws Exception {
    List<URI> nodes = createNodes(2);
    URI down = nodes.get(0);
    URI active = nodes.get(1);
    MockAlternatorLiveNodes liveNodes =
        new MockAlternatorLiveNodes(
            nodes, NodeHealthConfig.builder().withConsecutiveFailureThreshold(1).build());
    liveNodes.reportNodeResult(down, NodeHealthObservation.TRAFFIC_FAILURE);
    liveNodes.reports.clear();
    TestableBasicQueryPlanInterceptor interceptor =
        new TestableBasicQueryPlanInterceptor(liveNodes);

    ExecutionAttributes attrs = ExecutionAttributes.builder().build();
    interceptor.setQueryPlan(
        attrs, new LazyQueryPlan(liveNodes, seedWhereFirstNodeIs(nodes, down)));

    SdkHttpRequest routed =
        interceptor.modifyHttpRequest(new MockModifyHttpRequestContext(baseRequest()), attrs);

    assertEquals(active, endpoint(routed));
  }

  @Test
  public void testModifyHttpRequestRoutesToKnownDownNodeWhenNoActiveNodes() throws Exception {
    List<URI> nodes = createNodes(1);
    URI down = nodes.get(0);
    MockAlternatorLiveNodes liveNodes =
        new MockAlternatorLiveNodes(
            nodes, NodeHealthConfig.builder().withConsecutiveFailureThreshold(1).build());
    liveNodes.reportNodeResult(down, NodeHealthObservation.TRAFFIC_FAILURE);
    TestableBasicQueryPlanInterceptor interceptor =
        new TestableBasicQueryPlanInterceptor(liveNodes);

    ExecutionAttributes attrs = ExecutionAttributes.builder().build();
    interceptor.setQueryPlan(attrs, new LazyQueryPlan(liveNodes, 0));

    SdkHttpRequest originalRequest =
        SdkHttpRequest.builder()
            .protocol(down.getScheme())
            .host(down.getHost())
            .port(down.getPort())
            .method(software.amazon.awssdk.http.SdkHttpMethod.POST)
            .encodedPath("/")
            .build();

    SdkHttpRequest routed =
        interceptor.modifyHttpRequest(new MockModifyHttpRequestContext(originalRequest), attrs);

    assertEquals(down, endpoint(routed));
    assertEquals(NodeHealthState.DOWN, liveNodes.getNodeHealthStatus(down).getState());
  }

  @Test
  public void testModifyHttpRequestRetriesAcrossAllKnownDownNodesWhenNoActiveNodes()
      throws Exception {
    List<URI> nodes = createNodes(2);
    MockAlternatorLiveNodes liveNodes =
        new MockAlternatorLiveNodes(
            nodes, NodeHealthConfig.builder().withConsecutiveFailureThreshold(1).build());
    liveNodes.reportNodeResult(nodes.get(0), NodeHealthObservation.TRAFFIC_FAILURE);
    liveNodes.reportNodeResult(nodes.get(1), NodeHealthObservation.TRAFFIC_FAILURE);
    TestableBasicQueryPlanInterceptor interceptor =
        new TestableBasicQueryPlanInterceptor(liveNodes);

    ExecutionAttributes attrs = ExecutionAttributes.builder().build();
    interceptor.setQueryPlan(attrs, new LazyQueryPlan(liveNodes, 0));
    MockModifyHttpRequestContext context = new MockModifyHttpRequestContext(baseRequest());

    SdkHttpRequest first = interceptor.modifyHttpRequest(context, attrs);
    SdkHttpRequest second = interceptor.modifyHttpRequest(context, attrs);

    assertEquals(
        new HashSet<>(nodes), new HashSet<>(Arrays.asList(endpoint(first), endpoint(second))));
    assertEquals(NodeHealthState.DOWN, liveNodes.getNodeHealthStatus(nodes.get(0)).getState());
    assertEquals(NodeHealthState.DOWN, liveNodes.getNodeHealthStatus(nodes.get(1)).getState());
  }

  @Test
  public void testModifyHttpRequestSamplesQuarantinedCandidateAtFinalGate() throws Exception {
    List<URI> nodes = createNodes(2);
    URI recovering = nodes.get(0);
    URI active = nodes.get(1);
    MockAlternatorLiveNodes liveNodes =
        new MockAlternatorLiveNodes(
            nodes,
            NodeHealthConfig.builder()
                .withConsecutiveFailureThreshold(1)
                .withDownNodeRecoverySuccessThreshold(1)
                .withQuarantineTrafficInterval(2)
                .build());
    liveNodes.reportNodeResult(recovering, NodeHealthObservation.TRAFFIC_FAILURE);
    liveNodes.reportNodeResult(recovering, NodeHealthObservation.PROBE_SUCCESS);
    liveNodes.reports.clear();
    long seed = seedWhereFirstNodeIs(nodes, recovering);
    TestableBasicQueryPlanInterceptor interceptor =
        new TestableBasicQueryPlanInterceptor(liveNodes);

    ExecutionAttributes firstAttrs = ExecutionAttributes.builder().build();
    interceptor.setQueryPlan(firstAttrs, new LazyQueryPlan(liveNodes, seed));
    SdkHttpRequest first =
        interceptor.modifyHttpRequest(new MockModifyHttpRequestContext(baseRequest()), firstAttrs);

    ExecutionAttributes secondAttrs = ExecutionAttributes.builder().build();
    interceptor.setQueryPlan(secondAttrs, new LazyQueryPlan(liveNodes, seed));
    SdkHttpRequest second =
        interceptor.modifyHttpRequest(new MockModifyHttpRequestContext(baseRequest()), secondAttrs);

    assertEquals(active, endpoint(first));
    assertEquals(recovering, endpoint(second));
  }

  @Test
  public void testModifyHttpRequestSamplesQuarantinedCandidateAfterActiveCandidate()
      throws Exception {
    List<URI> nodes = createNodes(2);
    URI recovering = nodes.get(0);
    URI active = nodes.get(1);
    MockAlternatorLiveNodes liveNodes =
        new MockAlternatorLiveNodes(
            nodes,
            NodeHealthConfig.builder()
                .withConsecutiveFailureThreshold(1)
                .withDownNodeRecoverySuccessThreshold(1)
                .withQuarantineTrafficInterval(2)
                .build());
    liveNodes.reportNodeResult(recovering, NodeHealthObservation.TRAFFIC_FAILURE);
    liveNodes.reportNodeResult(recovering, NodeHealthObservation.PROBE_SUCCESS);
    liveNodes.reports.clear();
    long seed = seedWhereFirstNodeIs(nodes, active);
    TestableBasicQueryPlanInterceptor interceptor =
        new TestableBasicQueryPlanInterceptor(liveNodes);

    ExecutionAttributes firstAttrs = ExecutionAttributes.builder().build();
    interceptor.setQueryPlan(firstAttrs, new LazyQueryPlan(liveNodes, seed));
    SdkHttpRequest first =
        interceptor.modifyHttpRequest(new MockModifyHttpRequestContext(baseRequest()), firstAttrs);

    ExecutionAttributes secondAttrs = ExecutionAttributes.builder().build();
    interceptor.setQueryPlan(secondAttrs, new LazyQueryPlan(liveNodes, seed));
    SdkHttpRequest second =
        interceptor.modifyHttpRequest(new MockModifyHttpRequestContext(baseRequest()), secondAttrs);

    assertEquals(active, endpoint(first));
    assertEquals(recovering, endpoint(second));
  }

  @Test
  public void testModifyHttpRequestPreservesSkippedActiveCandidateAfterQuarantineSample()
      throws Exception {
    List<URI> nodes = createNodes(2);
    URI active = nodes.get(0);
    URI recovering = nodes.get(1);
    MockAlternatorLiveNodes liveNodes =
        new MockAlternatorLiveNodes(
            nodes,
            NodeHealthConfig.builder()
                .withConsecutiveFailureThreshold(1)
                .withDownNodeRecoverySuccessThreshold(1)
                .withQuarantineTrafficInterval(1)
                .build());
    liveNodes.reportNodeResult(recovering, NodeHealthObservation.TRAFFIC_FAILURE);
    liveNodes.reportNodeResult(recovering, NodeHealthObservation.PROBE_SUCCESS);
    liveNodes.reports.clear();
    long seed = seedWhereFirstNodeIs(nodes, active);
    TestableBasicQueryPlanInterceptor interceptor =
        new TestableBasicQueryPlanInterceptor(liveNodes);

    ExecutionAttributes attrs = ExecutionAttributes.builder().build();
    interceptor.setQueryPlan(attrs, new LazyQueryPlan(liveNodes, seed));
    MockModifyHttpRequestContext context = new MockModifyHttpRequestContext(baseRequest());

    SdkHttpRequest first = interceptor.modifyHttpRequest(context, attrs);
    liveNodes.reportNodeResult(recovering, NodeHealthObservation.TRAFFIC_FAILURE);
    SdkHttpRequest second = interceptor.modifyHttpRequest(context, attrs);

    assertEquals(recovering, endpoint(first));
    assertEquals(active, endpoint(second));
  }

  // ========== Mock implementations ==========

  private SdkHttpRequest baseRequest() {
    return SdkHttpRequest.builder()
        .protocol("http")
        .host("placeholder")
        .port(8000)
        .method(software.amazon.awssdk.http.SdkHttpMethod.POST)
        .encodedPath("/")
        .build();
  }

  private URI endpoint(SdkHttpRequest request) throws Exception {
    return new URI(request.protocol(), null, request.host(), request.port(), null, null, null);
  }

  private long seedWhereFirstNodeIs(List<URI> candidates, URI expected) throws Exception {
    for (long seed = -1000; seed < 1000; seed++) {
      if (expected.equals(firstNodeForSeed(candidates, seed))) {
        return seed;
      }
    }
    fail("Could not find seed for " + expected);
    return 0;
  }

  private URI firstNodeForSeed(List<URI> candidates, long seed) {
    LazyQueryPlan plan = new LazyQueryPlan(new MockAlternatorLiveNodes(candidates), seed);
    return plan.hasNext() ? plan.next() : null;
  }

  private Throwable dynamoDbException(int statusCode, String errorCode) {
    return DynamoDbException.builder()
        .message(errorCode)
        .statusCode(statusCode)
        .awsErrorDetails(
            AwsErrorDetails.builder()
                .errorCode(errorCode)
                .errorMessage(errorCode)
                .sdkHttpResponse(SdkHttpFullResponse.builder().statusCode(statusCode).build())
                .build())
        .build();
  }

  /**
   * Mock AlternatorLiveNodes that provides a fixed list of nodes without network calls.
   *
   * <p>This mock:
   *
   * <ul>
   *   <li>Does not start any background threads
   *   <li>Provides a fixed list of test nodes
   *   <li>Supports LazyQueryPlan creation (via base class)
   * </ul>
   */
  private static class MockAlternatorLiveNodes extends AlternatorLiveNodes {
    private final List<URI> nodes;
    private final List<NodeReport> reports = new ArrayList<>();

    MockAlternatorLiveNodes(List<URI> nodes) {
      this(nodes, NodeHealthConfig.getDefault());
    }

    MockAlternatorLiveNodes(List<URI> nodes, NodeHealthConfig nodeHealthConfig) {
      super(
          AlternatorConfig.builder()
              .withSeedHosts(nodes.stream().map(URI::getHost).collect(Collectors.toList()))
              .withScheme(nodes.get(0).getScheme())
              .withPort(nodes.get(0).getPort())
              .withNodeHealthConfig(nodeHealthConfig)
              .build());
      this.nodes = new ArrayList<>(nodes);
    }

    @Override
    protected List<URI> getDiscoveredNodesInternal() {
      return nodes;
    }

    @Override
    public void start() {}

    @Override
    public void reportNodeResult(URI node, NodeHealthObservation observation) {
      super.reportNodeResult(node, observation);
      reports.add(new NodeReport(node, observation));
    }
  }

  private static class TestableBasicQueryPlanInterceptor extends BasicQueryPlanInterceptor {
    TestableBasicQueryPlanInterceptor(AlternatorLiveNodes liveNodes) {
      super(liveNodes);
    }

    void setQueryPlan(ExecutionAttributes attrs, LazyQueryPlan plan) {
      attrs.putAttribute(QUERY_PLAN, plan);
    }
  }

  private static class NodeReport {
    private final URI node;
    private final NodeHealthObservation observation;

    NodeReport(URI node, NodeHealthObservation observation) {
      this.node = node;
      this.observation = observation;
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof NodeReport)) {
        return false;
      }
      NodeReport that = (NodeReport) other;
      return Objects.equals(node, that.node) && observation == that.observation;
    }

    @Override
    public int hashCode() {
      return Objects.hash(node, observation);
    }
  }

  /**
   * Minimal mock implementation of {@link
   * software.amazon.awssdk.core.interceptor.Context.ModifyHttpRequest} that returns a fixed {@link
   * SdkHttpRequest}.
   *
   * <p>This allows calling {@code modifyHttpRequest} on the interceptor without a full SDK request
   * pipeline.
   */
  private static class MockModifyHttpRequestContext
      implements software.amazon.awssdk.core.interceptor.Context.ModifyHttpRequest {

    private final SdkHttpRequest httpRequest;

    MockModifyHttpRequestContext(SdkHttpRequest httpRequest) {
      this.httpRequest = httpRequest;
    }

    @Override
    public SdkHttpRequest httpRequest() {
      return httpRequest;
    }

    @Override
    public software.amazon.awssdk.core.SdkRequest request() {
      // Not needed for modifyHttpRequest tests
      return null;
    }

    @Override
    public Optional<software.amazon.awssdk.core.sync.RequestBody> requestBody() {
      return Optional.empty();
    }

    @Override
    public Optional<software.amazon.awssdk.core.async.AsyncRequestBody> asyncRequestBody() {
      return Optional.empty();
    }
  }

  private static class MockBeforeTransmissionContext extends MockModifyHttpRequestContext
      implements software.amazon.awssdk.core.interceptor.Context.BeforeTransmission {

    MockBeforeTransmissionContext(SdkHttpRequest httpRequest) {
      super(httpRequest);
    }
  }

  private static class MockAfterTransmissionContext extends MockModifyHttpRequestContext
      implements software.amazon.awssdk.core.interceptor.Context.AfterTransmission {

    private final SdkHttpResponse response;

    MockAfterTransmissionContext(SdkHttpRequest httpRequest, int statusCode) {
      super(httpRequest);
      this.response = SdkHttpFullResponse.builder().statusCode(statusCode).build();
    }

    @Override
    public SdkHttpResponse httpResponse() {
      return response;
    }

    @Override
    public Optional<org.reactivestreams.Publisher<java.nio.ByteBuffer>> responsePublisher() {
      return Optional.empty();
    }

    @Override
    public Optional<java.io.InputStream> responseBody() {
      return Optional.empty();
    }
  }

  private static class MockFailedExecutionContext
      implements software.amazon.awssdk.core.interceptor.Context.FailedExecution {

    private final SdkHttpRequest httpRequest;
    private final SdkHttpResponse httpResponse;
    private final Throwable exception;

    MockFailedExecutionContext(SdkHttpRequest httpRequest, int statusCode, Throwable exception) {
      this.httpRequest = httpRequest;
      this.httpResponse = SdkHttpFullResponse.builder().statusCode(statusCode).build();
      this.exception = exception;
    }

    @Override
    public Throwable exception() {
      return exception;
    }

    @Override
    public SdkRequest request() {
      return null;
    }

    @Override
    public Optional<SdkHttpRequest> httpRequest() {
      return Optional.of(httpRequest);
    }

    @Override
    public Optional<SdkHttpResponse> httpResponse() {
      return Optional.of(httpResponse);
    }

    @Override
    public Optional<SdkResponse> response() {
      return Optional.empty();
    }
  }
}
