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
          if (callNumber == 2) {
            firstCall.countDown();
          } else if (callNumber == 4) {
            secondCall.countDown();
          }
          byte[] body = scanResponse("127.0.0.1").getBytes(StandardCharsets.UTF_8);
          return HttpExecuteResponse.builder()
              .response(
                  SdkHttpFullResponse.builder()
                      .statusCode(200)
                      .putHeader("Content-Type", "application/x-amz-json-1.0")
                      .build())
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

  private static String scanResponse(String address) {
    return "{\"Items\":[{\"rpc_address\":{\"S\":\""
        + address
        + "\"},\"data_center\":{\"S\":\"dc1\"},\"rack\":{\"S\":\"rack1\"}}],"
        + "\"Count\":1,\"ScannedCount\":1}";
  }
}
