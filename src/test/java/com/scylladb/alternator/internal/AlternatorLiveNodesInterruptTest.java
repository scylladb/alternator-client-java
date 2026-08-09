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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.scylladb.alternator.AlternatorConfig;
import com.scylladb.alternator.queryplan.BasicQueryPlanInterceptor;
import java.io.ByteArrayInputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
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

  @Test
  public void testSdkRequestWakesIdleDiscoveryImmediately() throws Exception {
    CountingSuccessHttpClient client = new CountingSuccessHttpClient();
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedNode(URI.create("http://127.0.0.1:8000"))
            .withActiveRefreshIntervalMs(60_000)
            .withIdleRefreshIntervalMs(60_000)
            .build();
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, client);

    liveNodes.start();
    assertTrue("initial polling request should run", client.firstCall.await(5, TimeUnit.SECONDS));

    new BasicQueryPlanInterceptor(liveNodes)
        .beforeExecution(null, ExecutionAttributes.builder().build());

    assertTrue(
        "an SDK request should wake the idle discovery thread",
        client.secondCall.await(2, TimeUnit.SECONDS));
    assertFalse(
        "one SDK request should not cause two immediate refreshes",
        client.thirdCall.await(250, TimeUnit.MILLISECONDS));
    assertTrue("live-node thread should stop", liveNodes.shutdownAndWait(5_000));
  }

  private static final class CountingSuccessHttpClient implements SdkHttpClient {
    private final AtomicInteger calls = new AtomicInteger();
    private final CountDownLatch firstCall = new CountDownLatch(1);
    private final CountDownLatch secondCall = new CountDownLatch(1);
    private final CountDownLatch thirdCall = new CountDownLatch(1);

    @Override
    public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
      int callNumber = calls.incrementAndGet();
      return new ExecutableHttpRequest() {
        @Override
        public HttpExecuteResponse call() {
          if (callNumber == 1) {
            firstCall.countDown();
          } else if (callNumber == 2) {
            secondCall.countDown();
          } else {
            thirdCall.countDown();
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
