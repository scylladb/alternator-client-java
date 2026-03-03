package com.scylladb.alternator.internal;

import static org.junit.Assert.*;

import com.scylladb.alternator.AlternatorConfig;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
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
 * Tests that {@code connectionAcquisitionTimeoutMs} triggers a timeout when the CRT async
 * connection pool is exhausted.
 */
public class CrtAsyncConnectionPoolExhaustionTest {

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
          try {
            Thread.sleep(2000);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
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

  @Test(timeout = 30000)
  public void testAcquisitionTimeoutWhenPoolExhausted() throws Exception {
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withMaxConnections(1)
            .withConnectionAcquisitionTimeoutMs(500)
            .build();

    SdkAsyncHttpClient client = CrtAsyncClientFactory.create(null, config, null);
    try {
      // First request holds the only connection for 2s
      CompletableFuture<Void> firstRequest = executeRequestAsync(client);
      Thread.sleep(200);

      // Second request should fail with acquisition timeout
      try {
        executeRequestAsync(client).get();
        fail("Expected an exception due to connection pool exhaustion");
      } catch (ExecutionException e) {
        assertTimeoutException(e.getCause());
      }

      // Wait for the first request to finish
      try {
        firstRequest.get();
      } catch (Exception e) {
        // May fail if server handler was interrupted; that's fine
      }
    } finally {
      client.close();
    }
  }

  @Test(timeout = 30000)
  public void testNoTimeoutWithSufficientConnections() throws Exception {
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withMaxConnections(2)
            .withConnectionAcquisitionTimeoutMs(10000)
            .build();

    SdkAsyncHttpClient client = CrtAsyncClientFactory.create(null, config, null);
    try {
      CompletableFuture<Void> first = executeRequestAsync(client);
      CompletableFuture<Void> second = executeRequestAsync(client);

      first.get();
      second.get();
      assertEquals("Both requests should have reached the server", 2, requestCount.get());
    } finally {
      client.close();
    }
  }

  private CompletableFuture<Void> executeRequestAsync(SdkAsyncHttpClient client) {
    SdkHttpRequest request =
        SdkHttpRequest.builder()
            .uri(URI.create("http://127.0.0.1:" + port + "/test"))
            .method(SdkHttpMethod.GET)
            .build();

    CompletableFuture<Void> resultFuture = new CompletableFuture<>();

    AsyncExecuteRequest executeRequest =
        AsyncExecuteRequest.builder()
            .request(request)
            .requestContentPublisher(new EmptyPublisher())
            .responseHandler(
                new SdkAsyncHttpResponseHandler() {
                  @Override
                  public void onHeaders(SdkHttpResponse headers) {
                    // Headers received
                  }

                  @Override
                  public void onStream(Publisher<ByteBuffer> stream) {
                    stream.subscribe(
                        new Subscriber<ByteBuffer>() {
                          @Override
                          public void onSubscribe(Subscription s) {
                            s.request(Long.MAX_VALUE);
                          }

                          @Override
                          public void onNext(ByteBuffer byteBuffer) {}

                          @Override
                          public void onError(Throwable t) {
                            resultFuture.completeExceptionally(t);
                          }

                          @Override
                          public void onComplete() {
                            resultFuture.complete(null);
                          }
                        });
                  }

                  @Override
                  public void onError(Throwable error) {
                    resultFuture.completeExceptionally(error);
                  }
                })
            .build();

    client
        .execute(executeRequest)
        .whenComplete(
            (v, ex) -> {
              if (ex != null) {
                resultFuture.completeExceptionally(ex);
              }
            });

    return resultFuture;
  }

  private void assertTimeoutException(Throwable e) {
    String message = getFullExceptionChain(e).toLowerCase();
    assertTrue(
        "Exception should indicate timeout or pool exhaustion, but was: " + message,
        message.contains("timeout")
            || message.contains("pool")
            || message.contains("acquire")
            || message.contains("connection"));
  }

  private String getFullExceptionChain(Throwable e) {
    StringBuilder sb = new StringBuilder();
    Throwable current = e;
    while (current != null) {
      if (current.getMessage() != null) {
        sb.append(current.getMessage()).append(" ");
      }
      sb.append(current.getClass().getSimpleName()).append(" ");
      current = current.getCause();
    }
    return sb.toString();
  }

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
