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
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.http.conn.DnsResolver;
import org.junit.Test;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpFullResponse;

/** Common response-size and whole-attempt deadline tests for polling transports. */
public class AlternatorLiveNodesResponseBoundsTest {

  private static final LiveNodesPollingLimits TEST_LIMITS =
      new LiveNodesPollingLimits(100, 2, 100, 100, 150, 500, 1_000, 1024, 64);

  @Test(timeout = 5000)
  public void testGenericSlowSuccessfulBodyIsAbortedThenLaterSeedSucceeds() throws Exception {
    AtomicBoolean slowBodyAborted = new AtomicBoolean();
    SdkHttpClient client =
        new SdkHttpClient() {
          @Override
          public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
            boolean slow = "slow.test".equals(request.httpRequest().host());
            return responseRequest(
                slow
                    ? response(200, new SlowInputStream(slowBodyAborted), slowBodyAborted)
                    : response(200, "[\"learned.test\"]"));
          }

          @Override
          public void close() {}

          @Override
          public String clientName() {
            return "GenericBoundedResponseClient";
          }
        };
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedHosts(Arrays.asList("slow.test", "good.test"))
            .withScheme("http")
            .build();
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, client, TEST_LIMITS);

    long started = System.nanoTime();
    liveNodes.updateLiveNodes();

    assertEquals("learned.test", liveNodes.nextAsURI().getHost());
    assertTrue("deadline must abort the slow successful body", slowBodyAborted.get());
    assertTrue(elapsedMillis(started) < 1_000);
  }

  @Test(timeout = 5000)
  public void testApacheOversizedSuccessfulBodyIsAbortedThenLaterAddressSucceeds()
      throws Exception {
    AtomicBoolean oversizedBodyAborted = new AtomicBoolean();
    AddressClientHarness harness =
        new AddressClientHarness(
            address ->
                lastByte(address) == 1
                    ? response(
                        200,
                        new ByteArrayInputStream("x".repeat(65).getBytes(StandardCharsets.UTF_8)),
                        oversizedBodyAborted)
                    : response(200, "[\"learned.test\"]"));

    AlternatorLiveNodes liveNodes = harness.liveNodes(TEST_LIMITS);
    liveNodes.updateLiveNodes();

    assertEquals("learned.test", liveNodes.nextAsURI().getHost());
    assertEquals(2, harness.addressClients.size());
    assertTrue(oversizedBodyAborted.get());
    assertTrue(harness.addressClients.get(0).closed.get());
    harness.close();
  }

  @Test(timeout = 5000)
  public void testApacheEndlessErrorBodyIsNotDrainedThenLaterAddressSucceeds() throws Exception {
    AtomicInteger errorBodyReads = new AtomicInteger();
    AtomicBoolean errorBodyAborted = new AtomicBoolean();
    AddressClientHarness harness =
        new AddressClientHarness(
            address -> {
              if (lastByte(address) == 1) {
                return response(
                    503,
                    new CountingEndlessInputStream(errorBodyReads, errorBodyAborted),
                    errorBodyAborted);
              }
              return response(200, "[\"learned.test\"]");
            });

    long started = System.nanoTime();
    AlternatorLiveNodes liveNodes = harness.liveNodes(TEST_LIMITS);
    liveNodes.updateLiveNodes();

    assertEquals("learned.test", liveNodes.nextAsURI().getHost());
    assertEquals("non-success response bodies must not be drained", 0, errorBodyReads.get());
    assertTrue(errorBodyAborted.get());
    assertTrue(elapsedMillis(started) < 1_000);
    harness.close();
  }

  @Test(timeout = 5000)
  public void testApacheSlowSuccessfulBodyDeadlineLeavesLaterAddressBudget() throws Exception {
    AtomicBoolean slowBodyAborted = new AtomicBoolean();
    AddressClientHarness harness =
        new AddressClientHarness(
            address ->
                lastByte(address) == 1
                    ? response(200, new SlowInputStream(slowBodyAborted), slowBodyAborted)
                    : response(200, "[\"learned.test\"]"));

    AlternatorLiveNodes liveNodes = harness.liveNodes(TEST_LIMITS);
    liveNodes.updateLiveNodes();

    assertEquals("learned.test", liveNodes.nextAsURI().getHost());
    assertTrue(slowBodyAborted.get());
    harness.close();
  }

  @Test(timeout = 5000)
  public void testExpiredDeadlineAfterApachePreparationClosesAddressClient() throws Exception {
    InetAddress address = InetAddress.getByName("192.0.2.5");
    AtomicReference<ResponseClient> addressed = new AtomicReference<>();
    ApacheDnsFallbackSdkHttpClient client =
        new ApacheDnsFallbackSdkHttpClient(
            new ResponseClient(response(200, "[]")),
            hostname -> new InetAddress[] {address},
            (hostname, resolvedAddress) -> {
              ResponseClient created = new ResponseClient(response(200, "[]"), 30);
              addressed.set(created);
              return created;
            });
    LiveNodesPollingLimits limits =
        new LiveNodesPollingLimits(100, 1, 100, 100, 5, 20, 40, 1024, 64);
    AlternatorLiveNodes liveNodes =
        new AlternatorLiveNodes(
            AlternatorConfig.builder().withSeedHost("logical.test").withScheme("http").build(),
            client,
            limits);
    try {
      liveNodes.updateLiveNodes();

      assertNotNull(addressed.get());
      assertTrue("expired prepared request must be aborted", addressed.get().requestAborted.get());
      assertTrue("one-shot Apache address client must be closed", addressed.get().closed.get());
    } finally {
      client.close();
    }
  }

  @Test(timeout = 5000)
  public void testExpiredCycleStopsBeforeWalkingAllRetainedCandidates() throws Exception {
    StringBuilder discovered = new StringBuilder("[");
    for (int i = 0; i < 1_000; i++) {
      if (i > 0) {
        discovered.append(',');
      }
      discovered.append("\"node-").append(i).append(".test\"");
    }
    discovered.append(']');
    AtomicBoolean slowPreparation = new AtomicBoolean();
    AtomicInteger slowPreparations = new AtomicInteger();
    AtomicInteger abortedPreparations = new AtomicInteger();
    SdkHttpClient client =
        new SdkHttpClient() {
          @Override
          public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
            if (slowPreparation.get()) {
              slowPreparations.incrementAndGet();
              sleepIgnoringInterrupts(80);
            }
            return new ExecutableHttpRequest() {
              @Override
              public HttpExecuteResponse call() {
                return response(200, discovered.toString());
              }

              @Override
              public void abort() {
                abortedPreparations.incrementAndGet();
              }
            };
          }

          @Override
          public void close() {}

          @Override
          public String clientName() {
            return "ManyCandidateDeadlineClient";
          }
        };
    LiveNodesPollingLimits limits =
        new LiveNodesPollingLimits(100, 2, 100, 100, 100, 100, 50, 1024, 64 * 1024);
    AlternatorLiveNodes liveNodes =
        new AlternatorLiveNodes(
            AlternatorConfig.builder().withSeedHost("seed.test").withScheme("http").build(),
            client,
            limits);
    liveNodes.updateLiveNodes();
    assertEquals(1_000, liveNodes.getLiveNodes().size());

    slowPreparation.set(true);
    long started = System.nanoTime();
    liveNodes.updateLiveNodes();

    assertEquals(
        "expired cycle must stop after the in-progress candidate", 1, slowPreparations.get());
    assertEquals(1, abortedPreparations.get());
    assertTrue(elapsedMillis(started) < 500);
  }

  private interface AddressResponseFactory {
    HttpExecuteResponse create(InetAddress address) throws Exception;
  }

  private static final class AddressClientHarness implements AutoCloseable {
    final List<ResponseClient> addressClients = new ArrayList<>();
    private final ApacheDnsFallbackSdkHttpClient client;

    AddressClientHarness(AddressResponseFactory responses) throws Exception {
      InetAddress bad = InetAddress.getByName("192.0.2.1");
      InetAddress good = InetAddress.getByName("192.0.2.2");
      DnsResolver resolver = hostname -> new InetAddress[] {bad, good};
      client =
          new ApacheDnsFallbackSdkHttpClient(
              new ResponseClient(response(200, "[]")),
              resolver,
              (hostname, address) -> {
                try {
                  ResponseClient created = new ResponseClient(responses.create(address));
                  addressClients.add(created);
                  return created;
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              });
    }

    AlternatorLiveNodes liveNodes(LiveNodesPollingLimits limits) {
      AlternatorConfig config =
          AlternatorConfig.builder().withSeedHost("logical.test").withScheme("http").build();
      return new AlternatorLiveNodes(config, client, limits);
    }

    @Override
    public void close() {
      client.close();
    }
  }

  private static final class ResponseClient implements SdkHttpClient {
    private final HttpExecuteResponse response;
    private final long prepareDelayMillis;
    final AtomicBoolean closed = new AtomicBoolean();
    final AtomicBoolean requestAborted = new AtomicBoolean();

    ResponseClient(HttpExecuteResponse response) {
      this(response, 0);
    }

    ResponseClient(HttpExecuteResponse response, long prepareDelayMillis) {
      this.response = response;
      this.prepareDelayMillis = prepareDelayMillis;
    }

    @Override
    public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
      sleepIgnoringInterrupts(prepareDelayMillis);
      return new ExecutableHttpRequest() {
        @Override
        public HttpExecuteResponse call() {
          return response;
        }

        @Override
        public void abort() {
          requestAborted.set(true);
          response.responseBody().ifPresent(AbortableInputStream::abort);
        }
      };
    }

    @Override
    public void close() {
      closed.set(true);
    }

    @Override
    public String clientName() {
      return "ResponseClient";
    }
  }

  private static ExecutableHttpRequest responseRequest(HttpExecuteResponse response) {
    AtomicReference<AbortableInputStream> body =
        new AtomicReference<>(response.responseBody().orElse(null));
    return new ExecutableHttpRequest() {
      @Override
      public HttpExecuteResponse call() {
        return response;
      }

      @Override
      public void abort() {
        AbortableInputStream stream = body.get();
        if (stream != null) {
          stream.abort();
        }
      }
    };
  }

  private static HttpExecuteResponse response(int status, String body) {
    return response(
        status,
        new ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_8)),
        new AtomicBoolean());
  }

  private static HttpExecuteResponse response(int status, InputStream body, AtomicBoolean aborted) {
    return HttpExecuteResponse.builder()
        .response(SdkHttpFullResponse.builder().statusCode(status).build())
        .responseBody(AbortableInputStream.create(body, () -> aborted.set(true)))
        .build();
  }

  private static int lastByte(InetAddress address) {
    byte[] bytes = address.getAddress();
    return bytes[bytes.length - 1] & 0xff;
  }

  private static long elapsedMillis(long started) {
    return java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
  }

  private static void sleepIgnoringInterrupts(long millis) {
    long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(millis);
    while (System.nanoTime() < deadline) {
      try {
        java.util.concurrent.TimeUnit.NANOSECONDS.sleep(deadline - System.nanoTime());
      } catch (InterruptedException ignored) {
        // Model synchronous request preparation that does not honor interruption.
      }
    }
  }

  private static final class SlowInputStream extends InputStream {
    private final AtomicBoolean aborted;

    SlowInputStream(AtomicBoolean aborted) {
      this.aborted = aborted;
    }

    @Override
    public int read() throws IOException {
      if (aborted.get()) {
        throw new IOException("aborted");
      }
      try {
        Thread.sleep(25);
      } catch (InterruptedException ignored) {
        // The transport abort signal, not interrupt alone, ends this simulated slow response.
      }
      return 'x';
    }

    @Override
    public int read(byte[] bytes, int offset, int length) throws IOException {
      int value = read();
      bytes[offset] = (byte) value;
      return 1;
    }
  }

  private static final class CountingEndlessInputStream extends InputStream {
    private final AtomicInteger reads;
    private final AtomicBoolean aborted;

    CountingEndlessInputStream(AtomicInteger reads, AtomicBoolean aborted) {
      this.reads = reads;
      this.aborted = aborted;
    }

    @Override
    public int read() throws IOException {
      reads.incrementAndGet();
      if (aborted.get()) {
        throw new IOException("aborted");
      }
      return 'x';
    }
  }
}
