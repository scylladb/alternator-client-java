package com.scylladb.alternator;

import static org.junit.Assert.*;

import java.net.URI;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.junit.Test;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;

/**
 * Unit tests for HeadersFilteringSdkHttpClient.
 *
 * @author dmitry.kropachev
 */
public class HeadersFilteringSdkHttpClientTest {

  /** Mock SdkHttpClient that captures the request for inspection. */
  private static class MockSdkHttpClient implements SdkHttpClient {
    SdkHttpRequest capturedRequest;
    HttpExecuteRequest capturedExecuteRequest;
    boolean closeCalled = false;
    String clientNameValue = "MockSdkHttpClient";

    @Override
    public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
      this.capturedRequest = request.httpRequest();
      this.capturedExecuteRequest = request;
      return new ExecutableHttpRequest() {
        @Override
        public HttpExecuteResponse call() {
          return null; // Not needed for this test
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
      return clientNameValue;
    }
  }

  @Test
  public void testFiltersNonWhitelistedHeaders() {
    Set<String> whitelist = new HashSet<>(Arrays.asList("Host", "Authorization"));
    MockSdkHttpClient mockClient = new MockSdkHttpClient();
    HeadersFilteringSdkHttpClient filteringClient =
        new HeadersFilteringSdkHttpClient(mockClient, whitelist);

    SdkHttpRequest originalRequest =
        SdkHttpRequest.builder()
            .method(SdkHttpMethod.POST)
            .uri(URI.create("https://localhost:8043"))
            .appendHeader("Host", "localhost:8043")
            .appendHeader("Authorization", "AWS4-HMAC-SHA256...")
            .appendHeader("User-Agent", "aws-sdk-java/2.x")
            .appendHeader("X-Amz-Sdk-Invocation-Id", "some-id")
            .appendHeader("amz-sdk-request", "attempt=1")
            .build();

    HttpExecuteRequest executeRequest =
        HttpExecuteRequest.builder().request(originalRequest).build();

    filteringClient.prepareRequest(executeRequest);

    // Whitelisted headers should be present
    assertTrue(mockClient.capturedRequest.headers().containsKey("Host"));
    assertTrue(mockClient.capturedRequest.headers().containsKey("Authorization"));

    // Non-whitelisted headers should be removed
    assertFalse(mockClient.capturedRequest.headers().containsKey("User-Agent"));
    assertFalse(mockClient.capturedRequest.headers().containsKey("X-Amz-Sdk-Invocation-Id"));
    assertFalse(mockClient.capturedRequest.headers().containsKey("amz-sdk-request"));
  }

  @Test
  public void testCaseInsensitiveMatching() {
    Set<String> whitelist = new HashSet<>(Arrays.asList("Content-Type", "authorization"));
    MockSdkHttpClient mockClient = new MockSdkHttpClient();
    HeadersFilteringSdkHttpClient filteringClient =
        new HeadersFilteringSdkHttpClient(mockClient, whitelist);

    SdkHttpRequest originalRequest =
        SdkHttpRequest.builder()
            .method(SdkHttpMethod.POST)
            .uri(URI.create("https://localhost:8043"))
            .appendHeader("content-type", "application/json")
            .appendHeader("AUTHORIZATION", "Bearer token")
            .appendHeader("User-Agent", "test")
            .build();

    HttpExecuteRequest executeRequest =
        HttpExecuteRequest.builder().request(originalRequest).build();

    filteringClient.prepareRequest(executeRequest);

    // Headers should be preserved with original case (case-insensitive matching)
    assertTrue(mockClient.capturedRequest.headers().containsKey("content-type"));
    assertTrue(mockClient.capturedRequest.headers().containsKey("AUTHORIZATION"));
    assertFalse(mockClient.capturedRequest.headers().containsKey("User-Agent"));
  }

  @Test
  public void testPreservesMultiValueHeaders() {
    Set<String> whitelist = new HashSet<>(Arrays.asList("Accept-Encoding"));
    MockSdkHttpClient mockClient = new MockSdkHttpClient();
    HeadersFilteringSdkHttpClient filteringClient =
        new HeadersFilteringSdkHttpClient(mockClient, whitelist);

    SdkHttpRequest originalRequest =
        SdkHttpRequest.builder()
            .method(SdkHttpMethod.POST)
            .uri(URI.create("https://localhost:8043"))
            .appendHeader("Accept-Encoding", "gzip")
            .appendHeader("Accept-Encoding", "deflate")
            .build();

    HttpExecuteRequest executeRequest =
        HttpExecuteRequest.builder().request(originalRequest).build();

    filteringClient.prepareRequest(executeRequest);

    java.util.List<String> values = mockClient.capturedRequest.headers().get("Accept-Encoding");
    assertNotNull(values);
    assertEquals(2, values.size());
    assertTrue(values.contains("gzip"));
    assertTrue(values.contains("deflate"));
  }

  @Test
  public void testPreservesRequestUriAndMethod() {
    Set<String> whitelist = new HashSet<>(Arrays.asList("Host"));
    MockSdkHttpClient mockClient = new MockSdkHttpClient();
    HeadersFilteringSdkHttpClient filteringClient =
        new HeadersFilteringSdkHttpClient(mockClient, whitelist);

    URI originalUri = URI.create("https://localhost:8043/path?query=value");
    SdkHttpRequest originalRequest =
        SdkHttpRequest.builder()
            .method(SdkHttpMethod.POST)
            .uri(originalUri)
            .appendHeader("Host", "localhost:8043")
            .appendHeader("User-Agent", "test")
            .build();

    HttpExecuteRequest executeRequest =
        HttpExecuteRequest.builder().request(originalRequest).build();

    filteringClient.prepareRequest(executeRequest);

    // URI and method should be preserved
    assertEquals(originalUri, mockClient.capturedRequest.getUri());
    assertEquals(SdkHttpMethod.POST, mockClient.capturedRequest.method());
  }

  @Test
  public void testFiltersAllSdkMetadataHeaders() {
    // Use the default required headers whitelist
    AlternatorConfig config =
        AlternatorConfig.builder().withAuthenticationEnabled(true).build();
    Set<String> whitelist = config.getRequiredHeaders();

    MockSdkHttpClient mockClient = new MockSdkHttpClient();
    HeadersFilteringSdkHttpClient filteringClient =
        new HeadersFilteringSdkHttpClient(mockClient, whitelist);

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
            .appendHeader("Accept-Encoding", "gzip")
            // SDK metadata headers that should be filtered
            .appendHeader("User-Agent", "aws-sdk-java/2.x")
            .appendHeader("X-Amz-Sdk-Invocation-Id", "some-id")
            .appendHeader("amz-sdk-request", "attempt=1")
            .build();

    HttpExecuteRequest executeRequest =
        HttpExecuteRequest.builder().request(originalRequest).build();

    filteringClient.prepareRequest(executeRequest);

    // Required headers should be preserved
    assertTrue(mockClient.capturedRequest.headers().containsKey("Host"));
    assertTrue(mockClient.capturedRequest.headers().containsKey("X-Amz-Target"));
    assertTrue(mockClient.capturedRequest.headers().containsKey("Content-Type"));
    assertTrue(mockClient.capturedRequest.headers().containsKey("Content-Length"));
    assertTrue(mockClient.capturedRequest.headers().containsKey("Authorization"));
    assertTrue(mockClient.capturedRequest.headers().containsKey("X-Amz-Date"));
    assertTrue(mockClient.capturedRequest.headers().containsKey("Accept-Encoding"));

    // SDK metadata headers should be filtered
    assertFalse(mockClient.capturedRequest.headers().containsKey("User-Agent"));
    assertFalse(mockClient.capturedRequest.headers().containsKey("X-Amz-Sdk-Invocation-Id"));
    assertFalse(mockClient.capturedRequest.headers().containsKey("amz-sdk-request"));
  }

  @Test
  public void testCloseDelegatesToWrappedClient() {
    MockSdkHttpClient mockClient = new MockSdkHttpClient();
    HeadersFilteringSdkHttpClient filteringClient =
        new HeadersFilteringSdkHttpClient(mockClient, new HashSet<>());

    assertFalse(mockClient.closeCalled);
    filteringClient.close();
    assertTrue(mockClient.closeCalled);
  }

  @Test
  public void testClientNameDelegatesToWrappedClient() {
    MockSdkHttpClient mockClient = new MockSdkHttpClient();
    mockClient.clientNameValue = "CustomClientName";
    HeadersFilteringSdkHttpClient filteringClient =
        new HeadersFilteringSdkHttpClient(mockClient, new HashSet<>());

    assertEquals("CustomClientName", filteringClient.clientName());
  }

  @Test
  public void testEmptyWhitelistFiltersAllHeaders() {
    Set<String> whitelist = new HashSet<>();
    MockSdkHttpClient mockClient = new MockSdkHttpClient();
    HeadersFilteringSdkHttpClient filteringClient =
        new HeadersFilteringSdkHttpClient(mockClient, whitelist);

    SdkHttpRequest originalRequest =
        SdkHttpRequest.builder()
            .method(SdkHttpMethod.POST)
            .uri(URI.create("https://localhost:8043"))
            .appendHeader("Host", "localhost:8043")
            .appendHeader("Authorization", "test")
            .appendHeader("User-Agent", "test")
            .build();

    HttpExecuteRequest executeRequest =
        HttpExecuteRequest.builder().request(originalRequest).build();

    filteringClient.prepareRequest(executeRequest);

    // All headers should be filtered
    assertTrue(mockClient.capturedRequest.headers().isEmpty());
  }

  @Test
  public void testNullWhitelistFiltersAllHeaders() {
    MockSdkHttpClient mockClient = new MockSdkHttpClient();
    HeadersFilteringSdkHttpClient filteringClient =
        new HeadersFilteringSdkHttpClient(mockClient, null);

    SdkHttpRequest originalRequest =
        SdkHttpRequest.builder()
            .method(SdkHttpMethod.POST)
            .uri(URI.create("https://localhost:8043"))
            .appendHeader("Host", "localhost:8043")
            .appendHeader("Authorization", "test")
            .build();

    HttpExecuteRequest executeRequest =
        HttpExecuteRequest.builder().request(originalRequest).build();

    filteringClient.prepareRequest(executeRequest);

    // All headers should be filtered
    assertTrue(mockClient.capturedRequest.headers().isEmpty());
  }

  @Test
  public void testRequestWithNoHeaders() {
    Set<String> whitelist = new HashSet<>(Arrays.asList("Host", "Authorization"));
    MockSdkHttpClient mockClient = new MockSdkHttpClient();
    HeadersFilteringSdkHttpClient filteringClient =
        new HeadersFilteringSdkHttpClient(mockClient, whitelist);

    SdkHttpRequest originalRequest =
        SdkHttpRequest.builder()
            .method(SdkHttpMethod.POST)
            .uri(URI.create("https://localhost:8043"))
            .build();

    HttpExecuteRequest executeRequest =
        HttpExecuteRequest.builder().request(originalRequest).build();

    filteringClient.prepareRequest(executeRequest);

    // Should handle empty headers gracefully
    assertTrue(mockClient.capturedRequest.headers().isEmpty());
  }

  @Test
  public void testWhitelistIgnoresNullAndEmptyEntries() {
    Set<String> whitelist = new HashSet<>(Arrays.asList("Host", null, "", "Authorization"));
    MockSdkHttpClient mockClient = new MockSdkHttpClient();
    HeadersFilteringSdkHttpClient filteringClient =
        new HeadersFilteringSdkHttpClient(mockClient, whitelist);

    SdkHttpRequest originalRequest =
        SdkHttpRequest.builder()
            .method(SdkHttpMethod.POST)
            .uri(URI.create("https://localhost:8043"))
            .appendHeader("Host", "localhost:8043")
            .appendHeader("Authorization", "test")
            .appendHeader("User-Agent", "test")
            .build();

    HttpExecuteRequest executeRequest =
        HttpExecuteRequest.builder().request(originalRequest).build();

    filteringClient.prepareRequest(executeRequest);

    // Valid whitelist entries should work
    assertTrue(mockClient.capturedRequest.headers().containsKey("Host"));
    assertTrue(mockClient.capturedRequest.headers().containsKey("Authorization"));
    // Non-whitelisted headers should be filtered
    assertFalse(mockClient.capturedRequest.headers().containsKey("User-Agent"));
  }

  @Test
  public void testPreservesContentStreamProvider() {
    Set<String> whitelist = new HashSet<>(Arrays.asList("Host"));
    MockSdkHttpClient mockClient = new MockSdkHttpClient();
    HeadersFilteringSdkHttpClient filteringClient =
        new HeadersFilteringSdkHttpClient(mockClient, whitelist);

    SdkHttpRequest originalRequest =
        SdkHttpRequest.builder()
            .method(SdkHttpMethod.POST)
            .uri(URI.create("https://localhost:8043"))
            .appendHeader("Host", "localhost:8043")
            .build();

    software.amazon.awssdk.http.ContentStreamProvider contentProvider =
        () -> new java.io.ByteArrayInputStream("test".getBytes());

    HttpExecuteRequest executeRequest =
        HttpExecuteRequest.builder()
            .request(originalRequest)
            .contentStreamProvider(contentProvider)
            .build();

    filteringClient.prepareRequest(executeRequest);

    // Content stream provider should be preserved
    assertTrue(mockClient.capturedExecuteRequest.contentStreamProvider().isPresent());
  }

  @Test
  public void testHandlesNullContentStreamProvider() {
    Set<String> whitelist = new HashSet<>(Arrays.asList("Host"));
    MockSdkHttpClient mockClient = new MockSdkHttpClient();
    HeadersFilteringSdkHttpClient filteringClient =
        new HeadersFilteringSdkHttpClient(mockClient, whitelist);

    SdkHttpRequest originalRequest =
        SdkHttpRequest.builder()
            .method(SdkHttpMethod.POST)
            .uri(URI.create("https://localhost:8043"))
            .appendHeader("Host", "localhost:8043")
            .build();

    HttpExecuteRequest executeRequest =
        HttpExecuteRequest.builder().request(originalRequest).build();

    filteringClient.prepareRequest(executeRequest);

    // Should handle null content stream provider gracefully
    assertFalse(mockClient.capturedExecuteRequest.contentStreamProvider().isPresent());
  }
}
