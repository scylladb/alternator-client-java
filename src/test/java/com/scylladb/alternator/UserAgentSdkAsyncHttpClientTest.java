package com.scylladb.alternator;

import static org.junit.Assert.*;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.http.async.AsyncExecuteRequest;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.async.SdkAsyncHttpResponseHandler;
import software.amazon.awssdk.http.async.SdkHttpContentPublisher;
import software.amazon.awssdk.metrics.MetricCollector;

/** Unit tests for UserAgentSdkAsyncHttpClient. */
public class UserAgentSdkAsyncHttpClientTest {
  private static class MockSdkAsyncHttpClient implements SdkAsyncHttpClient {
    SdkHttpRequest capturedRequest;
    AsyncExecuteRequest capturedExecuteRequest;
    boolean closeCalled;

    @Override
    public CompletableFuture<Void> execute(AsyncExecuteRequest request) {
      this.capturedRequest = request.request();
      this.capturedExecuteRequest = request;
      return CompletableFuture.completedFuture(null);
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

  private static class MockResponseHandler implements SdkAsyncHttpResponseHandler {
    @Override
    public void onHeaders(SdkHttpResponse response) {}

    @Override
    public void onStream(Publisher<ByteBuffer> stream) {}

    @Override
    public void onError(Throwable error) {}
  }

  @Test
  public void testReplacesUserAgent() {
    MockSdkAsyncHttpClient mockClient = new MockSdkAsyncHttpClient();
    UserAgentSdkAsyncHttpClient client =
        new UserAgentSdkAsyncHttpClient(mockClient, AlternatorUserAgent.replaceWith("custom/1"));

    client.execute(createRequest(request("aws-sdk-java/2.x")));

    assertEquals("custom/1", mockClient.capturedRequest.firstMatchingHeader("User-Agent").get());
    assertEquals("localhost:8043", mockClient.capturedRequest.firstMatchingHeader("Host").get());
  }

  @Test
  public void testTransformsUserAgent() {
    MockSdkAsyncHttpClient mockClient = new MockSdkAsyncHttpClient();
    UserAgentSdkAsyncHttpClient client =
        new UserAgentSdkAsyncHttpClient(mockClient, userAgent -> userAgent + " app/2");

    client.execute(createRequest(request("aws-sdk-java/2.x")));

    assertEquals(
        "aws-sdk-java/2.x app/2",
        mockClient.capturedRequest.firstMatchingHeader("User-Agent").get());
  }

  @Test
  public void testRemovesUserAgentWhenTransformerReturnsNull() {
    MockSdkAsyncHttpClient mockClient = new MockSdkAsyncHttpClient();
    UserAgentSdkAsyncHttpClient client =
        new UserAgentSdkAsyncHttpClient(mockClient, AlternatorUserAgent.disable());

    client.execute(createRequest(request("aws-sdk-java/2.x")));

    assertFalse(mockClient.capturedRequest.headers().containsKey("User-Agent"));
    assertEquals("localhost:8043", mockClient.capturedRequest.firstMatchingHeader("Host").get());
  }

  @Test
  public void testPreservesAsyncExecuteRequestFields() {
    MockSdkAsyncHttpClient mockClient = new MockSdkAsyncHttpClient();
    UserAgentSdkAsyncHttpClient client =
        new UserAgentSdkAsyncHttpClient(mockClient, AlternatorUserAgent.replaceWith("custom/1"));
    MockResponseHandler handler = new MockResponseHandler();
    MetricCollector metricCollector = MetricCollector.create("test");
    SdkHttpContentPublisher publisher =
        new SdkHttpContentPublisher() {
          @Override
          public Optional<Long> contentLength() {
            return Optional.of(4L);
          }

          @Override
          public void subscribe(Subscriber<? super ByteBuffer> subscriber) {}
        };

    client.execute(
        AsyncExecuteRequest.builder()
            .request(request("aws-sdk-java/2.x"))
            .responseHandler(handler)
            .requestContentPublisher(publisher)
            .metricCollector(metricCollector)
            .fullDuplex(true)
            .build());

    assertSame(handler, mockClient.capturedExecuteRequest.responseHandler());
    assertSame(publisher, mockClient.capturedExecuteRequest.requestContentPublisher());
    assertEquals(metricCollector, mockClient.capturedExecuteRequest.metricCollector().get());
    assertTrue(mockClient.capturedExecuteRequest.fullDuplex());
  }

  @Test
  public void testCloseDelegates() {
    MockSdkAsyncHttpClient mockClient = new MockSdkAsyncHttpClient();
    UserAgentSdkAsyncHttpClient client =
        new UserAgentSdkAsyncHttpClient(mockClient, AlternatorUserAgent.replaceWith("custom/1"));

    client.close();

    assertTrue(mockClient.closeCalled);
  }

  private AsyncExecuteRequest createRequest(SdkHttpRequest request) {
    return AsyncExecuteRequest.builder()
        .request(request)
        .responseHandler(new MockResponseHandler())
        .build();
  }

  private SdkHttpRequest request(String userAgent) {
    return SdkHttpRequest.builder()
        .method(SdkHttpMethod.POST)
        .uri(URI.create("https://localhost:8043"))
        .appendHeader("Host", "localhost:8043")
        .appendHeader("User-Agent", userAgent)
        .build();
  }
}
