package com.scylladb.alternator.queryplan;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import com.scylladb.alternator.AlternatorConfig;
import com.scylladb.alternator.internal.AlternatorLiveNodes;
import com.scylladb.alternator.internal.LazyQueryPlan;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.core.SdkRequest;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.services.dynamodb.model.ListTablesRequest;

/**
 * Unit tests for {@link BasicQueryPlanInterceptor#modifyHttpRequest}.
 *
 * <p>Focuses on the changed behaviour: the {@code Connection: keep-alive} header is added only when
 * absent. Adding it unconditionally on every retry forces the SDK to reallocate the full header map
 * even when the underlying transport has already set it.
 */
public class BasicQueryPlanInterceptorTest {

  private static final URI NODE = URI.create("http://10.0.0.1:8000");

  private AlternatorLiveNodes liveNodes;
  private BasicQueryPlanInterceptor interceptor;

  @Before
  public void setUp() {
    liveNodes = new AlternatorLiveNodes(AlternatorConfig.builder().withSeedNode(NODE).build());
    interceptor = new BasicQueryPlanInterceptor(liveNodes);
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private SdkHttpRequest baseRequest() {
    return SdkHttpRequest.builder()
        .protocol("http")
        .host("placeholder")
        .port(9999)
        .method(SdkHttpMethod.POST)
        .encodedPath("/")
        .build();
  }

  private SdkHttpRequest requestWithHeader(String name, String value) {
    return baseRequest().toBuilder().putHeader(name, value).build();
  }

  private Context.ModifyHttpRequest ctx(SdkHttpRequest req) {
    return new Context.ModifyHttpRequest() {
      @Override
      public SdkHttpRequest httpRequest() {
        return req;
      }

      @Override
      public Optional<RequestBody> requestBody() {
        return Optional.empty();
      }

      @Override
      public Optional<AsyncRequestBody> asyncRequestBody() {
        return Optional.empty();
      }

      @Override
      public SdkRequest request() {
        return ListTablesRequest.builder().build();
      }
    };
  }

  private ExecutionAttributes attributesWithPlan() {
    ExecutionAttributes attrs = new ExecutionAttributes();
    interceptor.beforeExecution(null, attrs);
    return attrs;
  }

  @Test
  public void keepAliveHeaderAddedWhenAbsent() {
    ExecutionAttributes attrs = attributesWithPlan();
    SdkHttpRequest result = interceptor.modifyHttpRequest(ctx(baseRequest()), attrs);

    List<String> connection = result.matchingHeaders("Connection");
    assertFalse(
        "Connection header must be present when absent from original", connection.isEmpty());
    assertTrue(
        "Connection header must contain keep-alive",
        connection.stream().anyMatch(v -> v.equalsIgnoreCase("keep-alive")));
  }

  @Test
  public void keepAliveHeaderNotDuplicatedWhenAlreadyPresent() {
    ExecutionAttributes attrs = attributesWithPlan();
    SdkHttpRequest incoming = requestWithHeader("Connection", "keep-alive");
    SdkHttpRequest result = interceptor.modifyHttpRequest(ctx(incoming), attrs);

    List<String> connection = result.matchingHeaders("Connection");
    long count = connection.stream().filter(v -> v.equalsIgnoreCase("keep-alive")).count();
    assertEquals("keep-alive must appear exactly once, not duplicated", 1, count);
  }

  @Test
  public void existingConnectionHeaderNotOverriddenWithDifferentValue() {
    ExecutionAttributes attrs = attributesWithPlan();
    SdkHttpRequest incoming = requestWithHeader("Connection", "close");
    SdkHttpRequest result = interceptor.modifyHttpRequest(ctx(incoming), attrs);

    // The interceptor should leave the existing Connection header alone, adding nothing.
    List<String> connection = result.matchingHeaders("Connection");
    assertEquals("only the original Connection value expected", 1, connection.size());
    assertEquals("close", connection.get(0));
  }

  @Test
  public void requestIsRewrittenToLiveNodeHostAndPort() {
    ExecutionAttributes attrs = attributesWithPlan();
    SdkHttpRequest result = interceptor.modifyHttpRequest(ctx(baseRequest()), attrs);

    assertEquals("host must be rewritten to live node", NODE.getHost(), result.host());
    assertEquals("port must be rewritten to live node", NODE.getPort(), result.port());
    assertEquals("scheme must be rewritten to live node", NODE.getScheme(), result.protocol());
  }

  @Test
  public void noPlanAttributeReturnOriginalRequest() {
    SdkHttpRequest original = baseRequest();
    SdkHttpRequest result = interceptor.modifyHttpRequest(ctx(original), new ExecutionAttributes());
    assertSame("original request returned when no plan attribute is present", original, result);
  }

  @Test
  public void exhaustedPlanReturnsOriginalRequest() {
    ExecutionAttributes attrs = attributesWithPlan();
    LazyQueryPlan plan = attrs.getAttribute(BasicQueryPlanInterceptor.QUERY_PLAN);
    while (plan.hasNext()) {
      plan.next();
    }
    SdkHttpRequest original = baseRequest();
    SdkHttpRequest result = interceptor.modifyHttpRequest(ctx(original), attrs);
    assertSame("original request returned when plan is exhausted", original, result);
  }

  // -------------------------------------------------------------------------
  // beforeExecution installs a plan
  // -------------------------------------------------------------------------

  @Test
  public void beforeExecutionInstallsQueryPlan() {
    ExecutionAttributes attrs = new ExecutionAttributes();
    interceptor.beforeExecution(null, attrs);

    com.scylladb.alternator.internal.LazyQueryPlan plan =
        attrs.getAttribute(BasicQueryPlanInterceptor.QUERY_PLAN);
    assertNotNull("beforeExecution must install a query plan", plan);
    assertTrue("installed plan must have at least one node", plan.hasNext());
  }
}
