package com.scylladb.alternator.internal;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.scylladb.alternator.AlternatorConfig;
import java.io.ByteArrayInputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpFullResponse;

public class AlternatorLiveNodesShutdownTest {

  @Test
  public void testShutdownPathPollingFailureDoesNotEscapeThread() throws Exception {
    BlockingShutdownHttpClient client = new BlockingShutdownHttpClient();
    AlternatorConfig config =
        AlternatorConfig.builder().withSeedNode(URI.create("http://127.0.0.1:8000")).build();
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, client);
    AtomicReference<Throwable> uncaught = new AtomicReference<>();
    liveNodes.setUncaughtExceptionHandler((thread, throwable) -> uncaught.set(throwable));

    liveNodes.start();
    assertTrue("polling request should start", client.callStarted.await(5, TimeUnit.SECONDS));

    liveNodes.shutdown();
    client.releaseCall.countDown();

    assertTrue("live-node thread should stop", liveNodes.shutdownAndWait(5_000));
    assertNull("shutdown-path polling exception should not escape", uncaught.get());
  }

  @Test
  public void testRuntimePollingFailureDoesNotStopThread() throws Exception {
    RuntimeThenSuccessHttpClient client = new RuntimeThenSuccessHttpClient();
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedNode(URI.create("http://127.0.0.1:8000"))
            .withIdleRefreshIntervalMs(25)
            .build();
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, client);
    AtomicReference<Throwable> uncaught = new AtomicReference<>();
    liveNodes.setUncaughtExceptionHandler((thread, throwable) -> uncaught.set(throwable));

    liveNodes.start();

    assertTrue(
        "recovery polling request should run", client.successfulCall.await(5, TimeUnit.SECONDS));
    assertTrue("live-node thread should still be running", liveNodes.isRunning());
    assertNull("runtime polling failure should not escape", uncaught.get());
    assertTrue("live-node thread should stop", liveNodes.shutdownAndWait(5_000));
  }

  private static final class BlockingShutdownHttpClient implements SdkHttpClient {
    private final CountDownLatch callStarted = new CountDownLatch(1);
    private final CountDownLatch releaseCall = new CountDownLatch(1);

    @Override
    public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
      return new ExecutableHttpRequest() {
        @Override
        public HttpExecuteResponse call() {
          callStarted.countDown();
          while (releaseCall.getCount() > 0) {
            try {
              releaseCall.await(100, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          }
          throw new IllegalStateException("Connection pool shut down");
        }

        @Override
        public void abort() {}
      };
    }

    @Override
    public void close() {}

    @Override
    public String clientName() {
      return "blocking-shutdown";
    }
  }

  private static final class RuntimeThenSuccessHttpClient implements SdkHttpClient {
    private final AtomicInteger calls = new AtomicInteger();
    private final CountDownLatch successfulCall = new CountDownLatch(1);

    @Override
    public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
      int callNumber = calls.incrementAndGet();
      return new ExecutableHttpRequest() {
        @Override
        public HttpExecuteResponse call() {
          if (callNumber == 1) {
            throw new IllegalStateException("unexpected polling failure");
          }
          successfulCall.countDown();
          byte[] body = "[\"127.0.0.2\"]".getBytes(StandardCharsets.UTF_8);
          return HttpExecuteResponse.builder()
              .response(SdkHttpFullResponse.builder().statusCode(200).build())
              .responseBody(AbortableInputStream.create(new ByteArrayInputStream(body)))
              .build();
        }

        @Override
        public void abort() {}
      };
    }

    @Override
    public void close() {}

    @Override
    public String clientName() {
      return "runtime-then-success";
    }
  }
}
