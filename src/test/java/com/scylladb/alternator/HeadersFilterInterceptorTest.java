package com.scylladb.alternator;

import static org.junit.Assert.*;

import java.net.URI;
import java.util.*;
import org.junit.Test;
import org.reactivestreams.Publisher;
import software.amazon.awssdk.core.SdkRequest;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;

/**
 * Unit tests for HeadersFilterInterceptor.
 *
 * @author dmitry.kropachev
 */
public class HeadersFilterInterceptorTest {

  /** Simple implementation of Context.ModifyHttpRequest for testing. */
  private static class TestModifyHttpRequestContext implements Context.ModifyHttpRequest {
    private final SdkHttpRequest httpRequest;

    TestModifyHttpRequestContext(SdkHttpRequest httpRequest) {
      this.httpRequest = httpRequest;
    }

    @Override
    public SdkHttpRequest httpRequest() {
      return httpRequest;
    }

    @Override
    public Optional<RequestBody> requestBody() {
      return Optional.empty();
    }

    @Override
    public SdkRequest request() {
      return null;
    }

    @Override
    public Optional<AsyncRequestBody> asyncRequestBody() {
      return Optional.empty();
    }
  }

  @Test
  public void testFiltersNonWhitelistedHeaders() {
    Set<String> whitelist = new HashSet<>(Arrays.asList("Host", "Authorization"));
    HeadersFilterInterceptor interceptor = new HeadersFilterInterceptor(whitelist);

    SdkHttpRequest originalRequest =
        SdkHttpRequest.builder()
            .method(SdkHttpMethod.POST)
            .uri(URI.create("https://localhost:8043"))
            .appendHeader("Host", "localhost:8043")
            .appendHeader("Authorization", "AWS4-HMAC-SHA256...")
            .appendHeader("User-Agent", "aws-sdk-java/2.x")
            .appendHeader("X-Amz-Sdk-Invocation-Id", "some-id")
            .build();

    Context.ModifyHttpRequest context = new TestModifyHttpRequestContext(originalRequest);

    SdkHttpRequest filteredRequest =
        interceptor.modifyHttpRequest(context, new ExecutionAttributes());

    // Whitelisted headers should be present
    assertTrue(filteredRequest.headers().containsKey("Host"));
    assertTrue(filteredRequest.headers().containsKey("Authorization"));

    // Non-whitelisted headers should be removed
    assertFalse(filteredRequest.headers().containsKey("User-Agent"));
    assertFalse(filteredRequest.headers().containsKey("X-Amz-Sdk-Invocation-Id"));
  }

  @Test
  public void testCaseInsensitiveMatching() {
    Set<String> whitelist = new HashSet<>(Arrays.asList("Content-Type", "authorization"));
    HeadersFilterInterceptor interceptor = new HeadersFilterInterceptor(whitelist);

    SdkHttpRequest originalRequest =
        SdkHttpRequest.builder()
            .method(SdkHttpMethod.POST)
            .uri(URI.create("https://localhost:8043"))
            .appendHeader("content-type", "application/json")
            .appendHeader("AUTHORIZATION", "Bearer token")
            .appendHeader("Content-Length", "100")
            .build();

    Context.ModifyHttpRequest context = new TestModifyHttpRequestContext(originalRequest);

    SdkHttpRequest filteredRequest =
        interceptor.modifyHttpRequest(context, new ExecutionAttributes());

    // Headers should be preserved with original case
    assertTrue(filteredRequest.headers().containsKey("content-type"));
    assertTrue(filteredRequest.headers().containsKey("AUTHORIZATION"));

    // Non-whitelisted should be removed
    assertFalse(filteredRequest.headers().containsKey("Content-Length"));
  }

  @Test
  public void testPreservesMultiValueHeaders() {
    Set<String> whitelist = new HashSet<>(Arrays.asList("Accept-Encoding"));
    HeadersFilterInterceptor interceptor = new HeadersFilterInterceptor(whitelist);

    SdkHttpRequest originalRequest =
        SdkHttpRequest.builder()
            .method(SdkHttpMethod.POST)
            .uri(URI.create("https://localhost:8043"))
            .appendHeader("Accept-Encoding", "gzip")
            .appendHeader("Accept-Encoding", "deflate")
            .build();

    Context.ModifyHttpRequest context = new TestModifyHttpRequestContext(originalRequest);

    SdkHttpRequest filteredRequest =
        interceptor.modifyHttpRequest(context, new ExecutionAttributes());

    List<String> values = filteredRequest.headers().get("Accept-Encoding");
    assertNotNull(values);
    assertEquals(2, values.size());
    assertTrue(values.contains("gzip"));
    assertTrue(values.contains("deflate"));
  }

  @Test
  public void testEmptyWhitelistFiltersAllHeaders() {
    // Note: Empty whitelist should be prevented by AlternatorConfig validation,
    // but we test the interceptor's behavior anyway
    Set<String> whitelist = new HashSet<>();
    HeadersFilterInterceptor interceptor = new HeadersFilterInterceptor(whitelist);

    SdkHttpRequest originalRequest =
        SdkHttpRequest.builder()
            .method(SdkHttpMethod.POST)
            .uri(URI.create("https://localhost:8043"))
            .appendHeader("Host", "localhost:8043")
            .appendHeader("Authorization", "token")
            .build();

    Context.ModifyHttpRequest context = new TestModifyHttpRequestContext(originalRequest);

    SdkHttpRequest filteredRequest =
        interceptor.modifyHttpRequest(context, new ExecutionAttributes());

    assertTrue(filteredRequest.headers().isEmpty());
  }

  @Test
  public void testNullWhitelistFiltersAllHeaders() {
    HeadersFilterInterceptor interceptor = new HeadersFilterInterceptor(null);

    SdkHttpRequest originalRequest =
        SdkHttpRequest.builder()
            .method(SdkHttpMethod.POST)
            .uri(URI.create("https://localhost:8043"))
            .appendHeader("Host", "localhost:8043")
            .build();

    Context.ModifyHttpRequest context = new TestModifyHttpRequestContext(originalRequest);

    SdkHttpRequest filteredRequest =
        interceptor.modifyHttpRequest(context, new ExecutionAttributes());

    assertTrue(filteredRequest.headers().isEmpty());
  }

  @Test
  public void testDefaultWhitelistPreservesRequiredHeaders() {
    HeadersFilterInterceptor interceptor =
        new HeadersFilterInterceptor(AlternatorConfig.DEFAULT_HEADERS_WHITELIST);

    SdkHttpRequest originalRequest =
        SdkHttpRequest.builder()
            .method(SdkHttpMethod.POST)
            .uri(URI.create("https://localhost:8043"))
            .appendHeader("Host", "localhost:8043")
            .appendHeader("X-Amz-Target", "DynamoDB_20120810.GetItem")
            .appendHeader("Content-Type", "application/x-amz-json-1.0")
            .appendHeader("Content-Length", "123")
            .appendHeader("Authorization", "AWS4-HMAC-SHA256...")
            .appendHeader("X-Amz-Date", "20240101T000000Z")
            .appendHeader("User-Agent", "aws-sdk-java/2.x") // Should be filtered
            .appendHeader("X-Amz-Sdk-Invocation-Id", "some-id") // Should be filtered
            .appendHeader("amz-sdk-request", "attempt=1") // Should be filtered
            .build();

    Context.ModifyHttpRequest context = new TestModifyHttpRequestContext(originalRequest);

    SdkHttpRequest filteredRequest =
        interceptor.modifyHttpRequest(context, new ExecutionAttributes());

    // Required headers should be preserved
    assertTrue(filteredRequest.headers().containsKey("Host"));
    assertTrue(filteredRequest.headers().containsKey("X-Amz-Target"));
    assertTrue(filteredRequest.headers().containsKey("Content-Type"));
    assertTrue(filteredRequest.headers().containsKey("Content-Length"));
    assertTrue(filteredRequest.headers().containsKey("Authorization"));
    assertTrue(filteredRequest.headers().containsKey("X-Amz-Date"));

    // SDK metadata headers should be filtered out
    assertFalse(filteredRequest.headers().containsKey("User-Agent"));
    assertFalse(filteredRequest.headers().containsKey("X-Amz-Sdk-Invocation-Id"));
    assertFalse(filteredRequest.headers().containsKey("amz-sdk-request"));
  }

  @Test
  public void testNoAuthWhitelistExcludesAuthHeaders() {
    HeadersFilterInterceptor interceptor =
        new HeadersFilterInterceptor(AlternatorConfig.DEFAULT_HEADERS_WHITELIST_NO_AUTH);

    SdkHttpRequest originalRequest =
        SdkHttpRequest.builder()
            .method(SdkHttpMethod.POST)
            .uri(URI.create("https://localhost:8043"))
            .appendHeader("Host", "localhost:8043")
            .appendHeader("X-Amz-Target", "DynamoDB_20120810.GetItem")
            .appendHeader("Content-Type", "application/x-amz-json-1.0")
            .appendHeader("Content-Length", "123")
            .appendHeader("Authorization", "AWS4-HMAC-SHA256...") // Should be filtered
            .appendHeader("X-Amz-Date", "20240101T000000Z") // Should be filtered
            .build();

    Context.ModifyHttpRequest context = new TestModifyHttpRequestContext(originalRequest);

    SdkHttpRequest filteredRequest =
        interceptor.modifyHttpRequest(context, new ExecutionAttributes());

    // Non-auth headers should be preserved
    assertTrue(filteredRequest.headers().containsKey("Host"));
    assertTrue(filteredRequest.headers().containsKey("X-Amz-Target"));
    assertTrue(filteredRequest.headers().containsKey("Content-Type"));
    assertTrue(filteredRequest.headers().containsKey("Content-Length"));

    // Auth headers should be filtered out
    assertFalse(filteredRequest.headers().containsKey("Authorization"));
    assertFalse(filteredRequest.headers().containsKey("X-Amz-Date"));
  }

  @Test
  public void testPreservesRequestUriAndMethod() {
    Set<String> whitelist = new HashSet<>(Arrays.asList("Host"));
    HeadersFilterInterceptor interceptor = new HeadersFilterInterceptor(whitelist);

    URI originalUri = URI.create("https://localhost:8043/path?query=value");
    SdkHttpRequest originalRequest =
        SdkHttpRequest.builder()
            .method(SdkHttpMethod.POST)
            .uri(originalUri)
            .appendHeader("Host", "localhost:8043")
            .appendHeader("User-Agent", "test")
            .build();

    Context.ModifyHttpRequest context = new TestModifyHttpRequestContext(originalRequest);

    SdkHttpRequest filteredRequest =
        interceptor.modifyHttpRequest(context, new ExecutionAttributes());

    // URI and method should be preserved
    assertEquals(originalUri, filteredRequest.getUri());
    assertEquals(SdkHttpMethod.POST, filteredRequest.method());
  }

  @Test
  public void testIgnoresNullAndEmptyHeaderNamesInWhitelist() {
    Set<String> whitelist = new HashSet<>(Arrays.asList("Host", null, "", "Authorization"));
    HeadersFilterInterceptor interceptor = new HeadersFilterInterceptor(whitelist);

    SdkHttpRequest originalRequest =
        SdkHttpRequest.builder()
            .method(SdkHttpMethod.POST)
            .uri(URI.create("https://localhost:8043"))
            .appendHeader("Host", "localhost:8043")
            .appendHeader("Authorization", "token")
            .appendHeader("User-Agent", "test")
            .build();

    Context.ModifyHttpRequest context = new TestModifyHttpRequestContext(originalRequest);

    SdkHttpRequest filteredRequest =
        interceptor.modifyHttpRequest(context, new ExecutionAttributes());

    // Valid whitelist entries should work
    assertTrue(filteredRequest.headers().containsKey("Host"));
    assertTrue(filteredRequest.headers().containsKey("Authorization"));

    // Non-whitelisted should be removed
    assertFalse(filteredRequest.headers().containsKey("User-Agent"));
  }
}
