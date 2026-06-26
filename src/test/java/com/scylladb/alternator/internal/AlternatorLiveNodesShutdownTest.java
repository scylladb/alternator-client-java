package com.scylladb.alternator.internal;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.scylladb.alternator.AlternatorConfig;
import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;

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
}
