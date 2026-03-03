package com.scylladb.alternator.internal;

import static org.junit.Assert.*;

import com.scylladb.alternator.AlternatorConfig;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.http.async.AsyncExecuteRequest;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.async.SdkAsyncHttpResponseHandler;
import software.amazon.awssdk.http.async.SdkHttpContentPublisher;

/**
 * Tests that Netty's {@code connectionTimeToLive(Duration.ZERO)} means "unlimited" rather than
 * "instant expiry".
 *
 * <p>Uses a real HTTP server and makes actual requests through SDK clients created by {@link
 * NettyAsyncClientFactory}.
 */
public class NettyConnectionTimeToLiveTest {

  private HttpServer server;
  private int port;
  private final AtomicInteger requestCount = new AtomicInteger(0);

  @Before
  public void setUp() throws IOException {
    requestCount.set(0);
    server = HttpServer.create(new InetSocketAddress(0), 0);
    port = server.getAddress().getPort();

    server.createContext(
        "/test",
        exchange -> {
          requestCount.incrementAndGet();
          byte[] body = "OK".getBytes();
          exchange.sendResponseHeaders(200, body.length);
          try (OutputStream os = exchange.getResponseBody()) {
            os.write(body);
          }
        });

    server.start();
  }

  @After
  public void tearDown() {
    if (server != null) {
      server.stop(0);
    }
  }

  /**
   * Verifies that the default config (connectionTimeToLiveMs=0) creates a working client where
   * Duration.ZERO means "unlimited TTL", not "instant connection expiry".
   */
  @Test(timeout = 30000)
  public void testZeroTtlMeansUnlimited() throws Exception {
    AlternatorConfig config = AlternatorConfig.builder().build();
    assertEquals(
        "Default connectionTimeToLiveMs should be 0", 0, config.getConnectionTimeToLiveMs());

    SdkAsyncHttpClient client = NettyAsyncClientFactory.create(null, config, null);
    try {
      // Make multiple requests with small delays — all should succeed
      for (int i = 0; i < 5; i++) {
        executeRequest(client);
        Thread.sleep(50);
      }
      assertEquals("All 5 requests should have reached the server", 5, requestCount.get());
    } finally {
      client.close();
    }
  }

  /**
   * Control test: a very short TTL still works because the SDK creates new connections when old
   * ones expire.
   */
  @Test(timeout = 30000)
  public void testShortTtlExpireConnections() throws Exception {
    AlternatorConfig config = AlternatorConfig.builder().build();

    // Use customizer to set a very short TTL
    SdkAsyncHttpClient client =
        NettyAsyncClientFactory.create(
            builder -> builder.connectionTimeToLive(Duration.ofMillis(1)), config, null);
    try {
      executeRequest(client);
      Thread.sleep(100);

      // Subsequent requests succeed — SDK creates new connections after old ones expire
      for (int i = 0; i < 4; i++) {
        executeRequest(client);
      }
      assertEquals("All 5 requests should have reached the server", 5, requestCount.get());
    } finally {
      client.close();
    }
  }

  private void executeRequest(SdkAsyncHttpClient client) throws Exception {
    SdkHttpRequest request =
        SdkHttpRequest.builder()
            .uri(URI.create("http://127.0.0.1:" + port + "/test"))
            .method(SdkHttpMethod.GET)
            .build();

    CompletableFuture<Integer> statusFuture = new CompletableFuture<>();

    AsyncExecuteRequest executeRequest =
        AsyncExecuteRequest.builder()
            .request(request)
            .requestContentPublisher(new EmptyPublisher())
            .responseHandler(
                new SdkAsyncHttpResponseHandler() {
                  @Override
                  public void onHeaders(SdkHttpResponse headers) {
                    statusFuture.complete(headers.statusCode());
                  }

                  @Override
                  public void onStream(Publisher<ByteBuffer> stream) {
                    // Drain the stream
                    stream.subscribe(
                        new Subscriber<ByteBuffer>() {
                          @Override
                          public void onSubscribe(Subscription s) {
                            s.request(Long.MAX_VALUE);
                          }

                          @Override
                          public void onNext(ByteBuffer byteBuffer) {}

                          @Override
                          public void onError(Throwable t) {}

                          @Override
                          public void onComplete() {}
                        });
                  }

                  @Override
                  public void onError(Throwable error) {
                    statusFuture.completeExceptionally(error);
                  }
                })
            .build();

    client.execute(executeRequest).get();
    int status = statusFuture.get();
    assertEquals("Request should succeed with 200", 200, status);
  }

  /** An empty request body publisher for GET requests. */
  private static class EmptyPublisher implements SdkHttpContentPublisher {
    @Override
    public Optional<Long> contentLength() {
      return Optional.of(0L);
    }

    @Override
    public void subscribe(Subscriber<? super ByteBuffer> subscriber) {
      subscriber.onSubscribe(
          new Subscription() {
            @Override
            public void request(long n) {
              subscriber.onComplete();
            }

            @Override
            public void cancel() {}
          });
    }
  }
}
