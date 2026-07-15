package com.scylladb.alternator.internal;

import static org.junit.Assert.assertTrue;

import com.scylladb.alternator.AlternatorConfig;
import java.io.ByteArrayInputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpFullResponse;

public class AlternatorLiveNodesInterruptTest {

  @Test
  public void testExternalInterruptDoesNotStopThread() throws Exception {
    CountingSuccessHttpClient client = new CountingSuccessHttpClient();
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedNode(URI.create("http://127.0.0.1:8000"))
            .withIdleRefreshIntervalMs(60_000)
            .build();
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, client);

    liveNodes.start();
    assertTrue("initial polling request should run", client.firstCall.await(5, TimeUnit.SECONDS));

    liveNodes.interrupt();

    assertTrue(
        "polling should continue after external interrupt",
        client.secondCall.await(5, TimeUnit.SECONDS));
    assertTrue("live-node thread should still be running", liveNodes.isRunning());
    assertTrue("live-node thread should stop", liveNodes.shutdownAndWait(5_000));
  }

  private static final class CountingSuccessHttpClient implements SdkHttpClient {
    private final AtomicInteger calls = new AtomicInteger();
    private final CountDownLatch firstCall = new CountDownLatch(1);
    private final CountDownLatch secondCall = new CountDownLatch(1);

    @Override
    public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
      int callNumber = calls.incrementAndGet();
      return new ExecutableHttpRequest() {
        @Override
        public HttpExecuteResponse call() {
          if (callNumber == 1) {
            firstCall.countDown();
          } else {
            secondCall.countDown();
          }
          byte[] body = "[\"127.0.0.1\"]".getBytes(StandardCharsets.UTF_8);
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
      return "counting-success";
    }
  }
}
