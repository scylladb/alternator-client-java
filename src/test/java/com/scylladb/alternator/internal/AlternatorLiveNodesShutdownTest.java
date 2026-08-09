/*
 * Copyright ScyllaDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.scylladb.alternator.internal;

import static org.junit.Assert.*;

import com.scylladb.alternator.AlternatorConfig;
import java.io.ByteArrayInputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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

  @Test(timeout = 5000)
  public void testShutdownStopsBeforeWalkingRemainingCandidates() throws Exception {
    AtomicBoolean slowPhase = new AtomicBoolean();
    AtomicInteger slowPreparations = new AtomicInteger();
    CountDownLatch slowPreparationStarted = new CountDownLatch(1);
    StringBuilder discovered = new StringBuilder("[");
    for (int i = 0; i < 100; i++) {
      if (i > 0) {
        discovered.append(',');
      }
      discovered.append("\"node-").append(i).append(".test\"");
    }
    discovered.append(']');
    SdkHttpClient client =
        new SdkHttpClient() {
          @Override
          public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
            if (slowPhase.get()) {
              slowPreparations.incrementAndGet();
              slowPreparationStarted.countDown();
              sleepIgnoringInterrupts(100);
            }
            return new ExecutableHttpRequest() {
              @Override
              public HttpExecuteResponse call() {
                String body = slowPhase.get() ? "unavailable" : discovered.toString();
                int status = slowPhase.get() ? 503 : 200;
                return response(status, body);
              }

              @Override
              public void abort() {}
            };
          }

          @Override
          public void close() {}

          @Override
          public String clientName() {
            return "many-candidate-shutdown";
          }
        };
    LiveNodesPollingLimits limits =
        new LiveNodesPollingLimits(100, 2, 100, 100, 100, 2_000, 2_000, 1_024, 64 * 1_024);
    AlternatorLiveNodes liveNodes =
        new AlternatorLiveNodes(
            AlternatorConfig.builder().withSeedHost("seed.test").withScheme("http").build(),
            client,
            limits);
    liveNodes.updateLiveNodes();
    slowPhase.set(true);
    liveNodes.start();

    assertTrue(slowPreparationStarted.await(1, TimeUnit.SECONDS));
    assertTrue(
        "shutdown should stop after the in-progress preparation", liveNodes.shutdownAndWait(500));
    assertEquals(1, slowPreparations.get());
  }

  private static HttpExecuteResponse response(int status, String body) {
    return HttpExecuteResponse.builder()
        .response(SdkHttpFullResponse.builder().statusCode(status).build())
        .responseBody(
            AbortableInputStream.create(
                new ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_8))))
        .build();
  }

  private static void sleepIgnoringInterrupts(long millis) {
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(millis);
    while (System.nanoTime() < deadline) {
      try {
        TimeUnit.NANOSECONDS.sleep(deadline - System.nanoTime());
      } catch (InterruptedException ignored) {
        // Model synchronous preparation that cannot be cancelled mid-call.
      }
    }
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
