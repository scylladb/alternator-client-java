package com.scylladb.alternator;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import org.junit.Test;
import software.amazon.awssdk.http.ContentStreamProvider;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;

/** Unit tests for UserAgentSdkHttpClient. */
public class UserAgentSdkHttpClientTest {
  private static class MockSdkHttpClient implements SdkHttpClient {
    SdkHttpRequest capturedRequest;
    HttpExecuteRequest capturedExecuteRequest;
    boolean closeCalled;

    @Override
    public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
      this.capturedRequest = request.httpRequest();
      this.capturedExecuteRequest = request;
      return new ExecutableHttpRequest() {
        @Override
        public HttpExecuteResponse call() {
          return null;
        }

        @Override
        public void abort() {}
      };
    }

    @Override
    public void close() {
      closeCalled = true;
    }

    @Override
    public String clientName() {
      return "mock";
    }
  }

  @Test
  public void testDefaultUserAgentReplacesAwsSdkUserAgent() {
    MockSdkHttpClient mockClient = new MockSdkHttpClient();
    UserAgentSdkHttpClient client =
        new UserAgentSdkHttpClient(mockClient, AlternatorUserAgent.defaultUserAgent());

    client.prepareRequest(
        HttpExecuteRequest.builder().request(request("aws-sdk-java/2.x")).build());

    assertEquals(
        AlternatorUserAgent.userAgentToken(),
        mockClient.capturedRequest.firstMatchingHeader("User-Agent").get());
    assertFalse(
        mockClient
            .capturedRequest
            .firstMatchingHeader("User-Agent")
            .get()
            .contains("aws-sdk-java"));
  }

  @Test
  public void testReplacesUserAgent() {
    MockSdkHttpClient mockClient = new MockSdkHttpClient();
    UserAgentSdkHttpClient client =
        new UserAgentSdkHttpClient(mockClient, AlternatorUserAgent.replaceWith("custom/1"));

    client.prepareRequest(
        HttpExecuteRequest.builder().request(request("aws-sdk-java/2.x")).build());

    assertEquals("custom/1", mockClient.capturedRequest.firstMatchingHeader("User-Agent").get());
    assertEquals("localhost:8043", mockClient.capturedRequest.firstMatchingHeader("Host").get());
  }

  @Test
  public void testTransformsUserAgent() {
    MockSdkHttpClient mockClient = new MockSdkHttpClient();
    UserAgentSdkHttpClient client =
        new UserAgentSdkHttpClient(
            mockClient,
            AlternatorUserAgent.transformDefault(userAgent -> "prefix " + userAgent + " suffix"));

    client.prepareRequest(
        HttpExecuteRequest.builder().request(request("aws-sdk-java/2.x")).build());

    assertEquals(
        "prefix " + AlternatorUserAgent.userAgentToken() + " suffix",
        mockClient.capturedRequest.firstMatchingHeader("User-Agent").get());
  }

  @Test
  public void testRemovesUserAgentWhenTransformerReturnsNull() {
    MockSdkHttpClient mockClient = new MockSdkHttpClient();
    UserAgentSdkHttpClient client =
        new UserAgentSdkHttpClient(mockClient, AlternatorUserAgent.disable());

    client.prepareRequest(
        HttpExecuteRequest.builder().request(request("aws-sdk-java/2.x")).build());

    assertFalse(mockClient.capturedRequest.headers().containsKey("User-Agent"));
    assertEquals("localhost:8043", mockClient.capturedRequest.firstMatchingHeader("Host").get());
  }

  @Test
  public void testDefaultUserAgentWithHeaderOptimizationReplacesAwsSdkUserAgent() {
    MockSdkHttpClient mockClient = new MockSdkHttpClient();
    Set<String> whitelist = AlternatorConfig.builder().getRequiredHeaders();
    SdkHttpClient client =
        new HeadersFilteringSdkHttpClient(
            new UserAgentSdkHttpClient(mockClient, AlternatorUserAgent.defaultUserAgent()),
            whitelist);

    client.prepareRequest(
        HttpExecuteRequest.builder().request(requestWithSdkMetadata("aws-sdk-java/2.x")).build());

    assertEquals(
        AlternatorUserAgent.userAgentToken(),
        mockClient.capturedRequest.firstMatchingHeader("User-Agent").get());
    assertFalse(mockClient.capturedRequest.headers().containsKey("X-Amz-Sdk-Invocation-Id"));
  }

  @Test
  public void testDisabledUserAgentWithHeaderOptimizationRemovesAwsSdkUserAgent() {
    MockSdkHttpClient mockClient = new MockSdkHttpClient();
    Set<String> whitelist =
        new HashSet<>(AlternatorConfig.builder().withUserAgentEnabled(false).getRequiredHeaders());
    SdkHttpClient client =
        new HeadersFilteringSdkHttpClient(
            new UserAgentSdkHttpClient(mockClient, AlternatorUserAgent.disable()), whitelist);

    client.prepareRequest(
        HttpExecuteRequest.builder().request(requestWithSdkMetadata("aws-sdk-java/2.x")).build());

    assertFalse(mockClient.capturedRequest.headers().containsKey("User-Agent"));
    assertFalse(mockClient.capturedRequest.headers().containsKey("X-Amz-Sdk-Invocation-Id"));
  }

  @Test
  public void testPreservesContentStreamProvider() {
    MockSdkHttpClient mockClient = new MockSdkHttpClient();
    UserAgentSdkHttpClient client =
        new UserAgentSdkHttpClient(mockClient, AlternatorUserAgent.replaceWith("custom/1"));
    ContentStreamProvider contentProvider = () -> new ByteArrayInputStream("test".getBytes());

    client.prepareRequest(
        HttpExecuteRequest.builder()
            .request(request("aws-sdk-java/2.x"))
            .contentStreamProvider(contentProvider)
            .build());

    assertTrue(mockClient.capturedExecuteRequest.contentStreamProvider().isPresent());
    assertSame(contentProvider, mockClient.capturedExecuteRequest.contentStreamProvider().get());
  }

  @Test
  public void testCloseDelegates() {
    MockSdkHttpClient mockClient = new MockSdkHttpClient();
    UserAgentSdkHttpClient client =
        new UserAgentSdkHttpClient(mockClient, AlternatorUserAgent.replaceWith("custom/1"));

    client.close();

    assertTrue(mockClient.closeCalled);
  }

  private SdkHttpRequest request(String userAgent) {
    return SdkHttpRequest.builder()
        .method(SdkHttpMethod.POST)
        .uri(URI.create("https://localhost:8043"))
        .appendHeader("Host", "localhost:8043")
        .appendHeader("User-Agent", userAgent)
        .build();
  }

  private SdkHttpRequest requestWithSdkMetadata(String userAgent) {
    return SdkHttpRequest.builder()
        .method(SdkHttpMethod.POST)
        .uri(URI.create("https://localhost:8043"))
        .appendHeader("Host", "localhost:8043")
        .appendHeader("X-Amz-Target", "DynamoDB_20120810.GetItem")
        .appendHeader("Content-Type", "application/x-amz-json-1.0")
        .appendHeader("Content-Length", "123")
        .appendHeader("Accept-Encoding", "gzip")
        .appendHeader("User-Agent", userAgent)
        .appendHeader("X-Amz-Sdk-Invocation-Id", "some-id")
        .build();
  }
}
