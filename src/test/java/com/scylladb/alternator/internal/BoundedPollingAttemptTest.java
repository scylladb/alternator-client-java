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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpFullResponse;

/** Resource-bound tests for whole-attempt polling execution. */
public class BoundedPollingAttemptTest {

  @Test(timeout = 10000)
  public void testQueuedTimeoutsAreRemovedAndHealthyAttemptCanRecover() throws Exception {
    CountDownLatch releaseWorkers = new CountDownLatch(1);
    ExecutorService callers = Executors.newFixedThreadPool(4);
    List<BlockingRequest> blockingRequests = new ArrayList<>();
    List<Future<BoundedPollingAttempt.Result>> blockingAttempts = new ArrayList<>();
    try {
      for (int i = 0; i < 4; i++) {
        BlockingRequest request = new BlockingRequest(releaseWorkers);
        blockingRequests.add(request);
        blockingAttempts.add(
            callers.submit(() -> BoundedPollingAttempt.execute(request, 5_000, 16)));
      }
      for (BlockingRequest request : blockingRequests) {
        assertTrue(request.entered.await(1, TimeUnit.SECONDS));
      }

      AtomicInteger queuedCalls = new AtomicInteger();
      for (int i = 0; i < 40; i++) {
        try {
          BoundedPollingAttempt.execute(successfulRequest(queuedCalls), 2, 16);
          fail("Expected queued polling attempt to time out");
        } catch (SocketTimeoutException expected) {
          // Every timed-out queued task must be removed instead of consuming queue capacity.
        }
      }
      assertEquals("queued requests must never reach a saturated worker", 0, queuedCalls.get());

      releaseWorkers.countDown();
      for (Future<BoundedPollingAttempt.Result> attempt : blockingAttempts) {
        assertTrue(attempt.get(1, TimeUnit.SECONDS).isSuccess());
      }

      AtomicInteger healthyCalls = new AtomicInteger();
      BoundedPollingAttempt.Result healthy =
          BoundedPollingAttempt.execute(successfulRequest(healthyCalls), 1_000, 16);
      assertTrue(healthy.isSuccess());
      assertEquals(1, healthyCalls.get());
    } finally {
      releaseWorkers.countDown();
      callers.shutdownNow();
    }
  }

  @Test
  public void testMaximumResponseBytesMustBePositive() throws Exception {
    AtomicBoolean aborted = new AtomicBoolean();
    ExecutableHttpRequest request =
        new ExecutableHttpRequest() {
          @Override
          public HttpExecuteResponse call() {
            return response();
          }

          @Override
          public void abort() {
            aborted.set(true);
          }
        };

    try {
      BoundedPollingAttempt.execute(request, 100, 0);
      fail("Expected a positive maximum response size to be required");
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("maximumResponseBytes"));
    }
    assertTrue(aborted.get());
  }

  @Test
  public void testWorkerErrorStillAbortsRequestOwnership() throws Exception {
    AtomicBoolean aborted = new AtomicBoolean();
    ExecutableHttpRequest request =
        new ExecutableHttpRequest() {
          @Override
          public HttpExecuteResponse call() {
            throw new AssertionError("simulated transport error");
          }

          @Override
          public void abort() {
            aborted.set(true);
          }
        };

    try {
      BoundedPollingAttempt.execute(request, 100, 16);
      fail("Expected worker error to fail the polling attempt");
    } catch (IOException expected) {
      assertTrue(expected.getCause().toString(), expected.getCause() instanceof AssertionError);
    }
    assertTrue(aborted.get());
  }

  private static ExecutableHttpRequest successfulRequest(AtomicInteger calls) {
    return new ExecutableHttpRequest() {
      @Override
      public HttpExecuteResponse call() {
        calls.incrementAndGet();
        return response();
      }

      @Override
      public void abort() {}
    };
  }

  private static HttpExecuteResponse response() {
    return HttpExecuteResponse.builder()
        .response(SdkHttpFullResponse.builder().statusCode(200).build())
        .responseBody(
            AbortableInputStream.create(
                new ByteArrayInputStream("[]".getBytes(StandardCharsets.UTF_8))))
        .build();
  }

  private static final class BlockingRequest implements ExecutableHttpRequest {
    private final CountDownLatch release;
    private final CountDownLatch entered = new CountDownLatch(1);

    BlockingRequest(CountDownLatch release) {
      this.release = release;
    }

    @Override
    public HttpExecuteResponse call() {
      entered.countDown();
      awaitIgnoringInterrupts(release);
      return response();
    }

    @Override
    public void abort() {
      // Model a transport that does not unblock promptly when cancellation is requested.
    }
  }

  private static void awaitIgnoringInterrupts(CountDownLatch latch) {
    boolean complete = false;
    while (!complete) {
      try {
        complete = latch.await(20, TimeUnit.MILLISECONDS);
      } catch (InterruptedException ignored) {
        // Keep blocking until the test explicitly releases the simulated transport.
      }
    }
  }
}
