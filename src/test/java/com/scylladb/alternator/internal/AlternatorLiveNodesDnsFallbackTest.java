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
import java.net.InetAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
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
import software.amazon.awssdk.http.SdkHttpRequest;

/** Deterministic negative DNS fallback tests for DRIVER-819 and GitHub issue #156. */
public class AlternatorLiveNodesDnsFallbackTest {

  @Test
  public void testLiteralSeedBypassesResolverAndDnsSeedUsesReservedCapacity() throws Exception {
    AtomicInteger resolutions = new AtomicInteger();
    TestDnsFallbackClient literalClient =
        new TestDnsFallbackClient(
            hostname -> {
              resolutions.incrementAndGet();
              throw new IOException("literal address must not enter the DNS worker pool");
            },
            (request, address) -> response(200, "[\"127.0.0.1\"]"));
    AlternatorLiveNodes literalNodes = new AlternatorLiveNodes(config("127.0.0.1"), literalClient);

    literalNodes.updateLiveNodes();

    assertEquals(0, resolutions.get());
    assertEquals("127.0.0.1", literalNodes.nextAsURI().getHost());

    TestDnsFallbackClient dnsClient =
        new TestDnsFallbackClient(
            hostname -> Collections.singletonList(address(1)),
            (request, address) -> response(200, "[\"learned.test\"]"));
    AlternatorLiveNodes dnsNodes = new AlternatorLiveNodes(config("seed.test"), dnsClient);

    dnsNodes.updateLiveNodes();

    assertEquals(Collections.singletonList(true), dnsClient.seedResolutionPriorities);
  }

  @Test
  public void testTriesEveryUniqueAddressUntilLocalNodesIsUsable() throws Exception {
    List<InetAddress> addresses = new ArrayList<>();
    for (int i = 1; i <= 7; i++) {
      addresses.add(address(i));
      if (i == 1) {
        addresses.add(address(i));
      }
    }

    TestDnsFallbackClient client =
        new TestDnsFallbackClient(
            hostname -> addresses,
            (request, address) -> {
              switch (lastAddressByte(address)) {
                case 1:
                  throw new IOException("simulated connection reset");
                case 2:
                  return response(503, "temporarily unavailable");
                case 3:
                  return response(200, "not-json");
                case 4:
                  return response(200, "[]");
                case 5:
                  return response(200, "[\"bad..name\"]");
                case 6:
                  return responseWithoutBody(200);
                default:
                  return response(200, "[\"learned.test\"]");
              }
            });
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config("seed.test"), client);

    liveNodes.updateLiveNodes();

    assertEquals("learned.test", liveNodes.nextAsURI().getHost());
    assertEquals(Arrays.asList(1, 2, 3, 4, 5, 6, 7), attemptedAddressBytes(client));
    for (Attempt attempt : client.attempts) {
      assertEquals("seed.test", attempt.request.host());
      assertEquals("seed.test:8000", attempt.request.firstMatchingHeader("Host").get());
      assertEquals("http://seed.test:8000/localnodes", attempt.request.getUri().toString());
    }
  }

  @Test
  public void testFailedRefreshKeepsLastNodesThenReresolvesSeedAndRecovers() throws Exception {
    AtomicInteger phase = new AtomicInteger(0);
    AtomicInteger seedResolutions = new AtomicInteger(0);
    TestDnsFallbackClient client =
        new TestDnsFallbackClient(
            hostname -> {
              if ("seed.test".equals(hostname)) {
                seedResolutions.incrementAndGet();
                if (phase.get() == 0) {
                  return Arrays.asList(address(1));
                }
                if (phase.get() == 1) {
                  return Arrays.asList(address(2));
                }
                return Arrays.asList(address(2), address(3));
              }
              return Arrays.asList(address(4));
            },
            (request, address) -> {
              if (phase.get() == 0) {
                return response(200, "[\"learned-a.test\"]");
              }
              if ("learned-a.test".equals(request.host())) {
                throw new IOException("learned node is down");
              }
              if (phase.get() == 1 || lastAddressByte(address) == 2) {
                return response(200, "not-json");
              }
              return response(200, "[\"learned-b.test\"]");
            });
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config("seed.test"), client);

    liveNodes.updateLiveNodes();
    assertEquals("learned-a.test", liveNodes.nextAsURI().getHost());

    phase.set(1);
    liveNodes.updateLiveNodes();
    assertEquals(
        "failed refresh must retain the previous atomic node set",
        "learned-a.test",
        liveNodes.nextAsURI().getHost());

    phase.set(2);
    liveNodes.updateLiveNodes();
    assertEquals("learned-b.test", liveNodes.nextAsURI().getHost());
    assertEquals(
        "seed hostname must be resolved once per recovery cycle", 3, seedResolutions.get());
  }

  @Test
  public void testDnsResolutionFailureAndEmptyAnswerRecoverOnLaterCycle() throws Exception {
    AtomicInteger resolutionAttempt = new AtomicInteger(0);
    TestDnsFallbackClient client =
        new TestDnsFallbackClient(
            hostname -> {
              int attempt = resolutionAttempt.getAndIncrement();
              if (attempt == 0) {
                throw new IOException("simulated SERVFAIL");
              }
              if (attempt == 1) {
                return new ArrayList<>();
              }
              return Arrays.asList(address(1));
            },
            (request, address) -> response(200, "[\"recovered.test\"]"));
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config("seed.test"), client);

    liveNodes.updateLiveNodes();
    assertEquals("seed.test", liveNodes.nextAsURI().getHost());

    liveNodes.updateLiveNodes();
    assertEquals("seed.test", liveNodes.nextAsURI().getHost());

    liveNodes.updateLiveNodes();
    assertEquals("recovered.test", liveNodes.nextAsURI().getHost());
    assertEquals(3, resolutionAttempt.get());
  }

  @Test(timeout = 5000)
  public void testFailedAddressAnswersAreDeduplicatedAndCapped() throws Exception {
    List<InetAddress> resolved = new ArrayList<>();
    List<Integer> expectedAttempts = new ArrayList<>();
    for (int i = 1; i <= 32; i++) {
      resolved.add(address(i));
      resolved.add(address(i));
      if (i <= LiveNodesPollingLimits.DEFAULT.maxDnsAddresses) {
        expectedAttempts.add(i);
      }
    }
    AtomicInteger resolutions = new AtomicInteger();
    TestDnsFallbackClient client =
        new TestDnsFallbackClient(
            hostname -> {
              resolutions.incrementAndGet();
              return resolved;
            },
            (request, address) -> response(503, "temporarily unavailable"));
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config("seed.test"), client);

    liveNodes.updateLiveNodes();

    assertEquals("seed.test", liveNodes.nextAsURI().getHost());
    assertEquals("hostname must be resolved once for the cycle", 1, resolutions.get());
    assertEquals(expectedAttempts, attemptedAddressBytes(client));
  }

  @Test(timeout = 5000)
  public void testGlobalCycleBudgetLeavesSeedRecoveryAttemptAfterManyLearnedAddresses()
      throws Exception {
    AtomicInteger phase = new AtomicInteger();
    TestDnsFallbackClient client =
        new TestDnsFallbackClient(
            hostname ->
                "seed.test".equals(hostname)
                    ? Arrays.asList(address(9))
                    : Arrays.asList(address(1), address(2), address(3)),
            (request, address) -> {
              if (phase.get() == 0) {
                return response(
                    200,
                    "[\"learned-a.test\",\"learned-b.test\","
                        + "\"learned-c.test\",\"learned-d.test\"]");
              }
              if ("seed.test".equals(request.host())) {
                return response(200, "[\"recovered.test\"]");
              }
              try {
                Thread.sleep(30);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted simulated failed address", e);
              }
              return response(503, "temporarily unavailable");
            });
    LiveNodesPollingLimits limits =
        new LiveNodesPollingLimits(100, 3, 100, 100, 200, 500, 800, 1_024, 1_024);
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config("seed.test"), client, limits);
    liveNodes.updateLiveNodes();
    assertEquals(4, liveNodes.getLiveNodes().size());

    client.attempts.clear();
    phase.set(1);
    long started = System.nanoTime();
    liveNodes.updateLiveNodes();

    assertEquals("recovered.test", liveNodes.nextAsURI().getHost());
    assertEquals(13, client.attempts.size());
    assertEquals("seed.test@9", attemptedAddresses(client).get(12));
    assertTrue(
        "global discovery deadline must remain bounded",
        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started) < 1_500);
  }

  @Test
  public void testEmptyAndWhollyInvalidDataTryEveryAddressAndSeedWithoutPoisoningNodes()
      throws Exception {
    AtomicInteger phase = new AtomicInteger();
    TestDnsFallbackClient client =
        new TestDnsFallbackClient(
            hostname -> {
              if ("bad-seed.test".equals(hostname)) {
                return Arrays.asList(address(1), address(2));
              }
              if ("good-seed.test".equals(hostname)) {
                return Arrays.asList(address(3), address(4));
              }
              return Arrays.asList(address(5));
            },
            (request, address) -> {
              if (phase.get() == 0
                  && "good-seed.test".equals(request.host())
                  && lastAddressByte(address) == 3) {
                return response(200, "[]");
              }
              if (phase.get() == 0
                  && "good-seed.test".equals(request.host())
                  && lastAddressByte(address) == 4) {
                return response(200, "[\"learned.test\"]");
              }
              return response(200, "[\"bad host\"]");
            });
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedHosts(Arrays.asList("bad-seed.test", "good-seed.test"))
            .withScheme("http")
            .withPort(8000)
            .build();
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, client);

    liveNodes.updateLiveNodes();

    List<URI> expectedNodes =
        Arrays.asList(
            new URI("http://learned.test:8000"),
            new URI("http://bad-seed.test:8000"),
            new URI("http://good-seed.test:8000"));
    assertEquals(expectedNodes, liveNodes.getLiveNodes());
    assertEquals(
        Arrays.asList("bad-seed.test@1", "bad-seed.test@2", "good-seed.test@3", "good-seed.test@4"),
        attemptedAddresses(client));

    client.attempts.clear();
    phase.set(1);
    liveNodes.updateLiveNodes();

    assertEquals(expectedNodes, liveNodes.getLiveNodes());
    assertEquals(
        Arrays.asList(
            "learned.test@5",
            "bad-seed.test@1",
            "bad-seed.test@2",
            "good-seed.test@3",
            "good-seed.test@4"),
        attemptedAddresses(client));
  }

  @Test
  public void testConcurrentReaderSeesOldListUntilReplacementIsComplete() throws Exception {
    AtomicInteger phase = new AtomicInteger(0);
    CountDownLatch newResponseStarted = new CountDownLatch(1);
    CountDownLatch releaseNewResponse = new CountDownLatch(1);
    TestDnsFallbackClient client =
        new TestDnsFallbackClient(
            hostname -> Arrays.asList(address(1)),
            (request, address) -> {
              if (phase.get() == 0) {
                return response(200, "[\"old.test\"]");
              }
              newResponseStarted.countDown();
              try {
                if (!releaseNewResponse.await(5, TimeUnit.SECONDS)) {
                  throw new IOException("timed out waiting to release response");
                }
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted while waiting to release response", e);
              }
              return response(200, "[\"new.test\"]");
            });
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config("seed.test"), client);
    liveNodes.updateLiveNodes();

    phase.set(1);
    AtomicReference<Throwable> updateFailure = new AtomicReference<>();
    Thread updater =
        new Thread(
            () -> {
              try {
                liveNodes.updateLiveNodes();
              } catch (Throwable e) {
                updateFailure.set(e);
              }
            });
    updater.start();

    assertTrue(newResponseStarted.await(5, TimeUnit.SECONDS));
    assertEquals("old.test", liveNodes.nextAsURI().getHost());
    releaseNewResponse.countDown();
    updater.join(5000);

    assertFalse("refresh must complete within the test bound", updater.isAlive());
    assertNull(updateFailure.get());
    assertEquals("new.test", liveNodes.nextAsURI().getHost());
  }

  @Test
  public void testMalformedJsonCannotShortCircuitAddressFallback() throws Exception {
    TestDnsFallbackClient client =
        new TestDnsFallbackClient(
            hostname -> Arrays.asList(address(1), address(2)),
            (request, address) ->
                lastAddressByte(address) == 1
                    ? response(200, "[\f\"poison.test\"]")
                    : response(200, "[\"learned.test\"]"));
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config("seed.test"), client);

    liveNodes.updateLiveNodes();

    assertEquals("learned.test", liveNodes.nextAsURI().getHost());
    assertEquals(Arrays.asList(1, 2), attemptedAddressBytes(client));
  }

  @Test
  public void testMalformedUtf8CannotShortCircuitAddressFallback() throws Exception {
    byte[] malformedUtf8 =
        new byte[] {
          '[',
          '"',
          'p',
          'o',
          'i',
          's',
          'o',
          'n',
          '.',
          't',
          'e',
          's',
          't',
          '"',
          ',',
          '"',
          'b',
          'a',
          'd',
          (byte) 0xc3,
          '.',
          't',
          'e',
          's',
          't',
          '"',
          ']'
        };
    TestDnsFallbackClient client =
        new TestDnsFallbackClient(
            hostname -> Arrays.asList(address(1), address(2)),
            (request, address) ->
                lastAddressByte(address) == 1
                    ? response(200, malformedUtf8)
                    : response(200, "[\"learned.test\"]"));
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config("seed.test"), client);

    liveNodes.updateLiveNodes();

    assertEquals("learned.test", liveNodes.nextAsURI().getHost());
    assertEquals(Arrays.asList(1, 2), attemptedAddressBytes(client));
  }

  @Test
  public void testAuthorityInjectionCannotShortCircuitAddressFallback() throws Exception {
    TestDnsFallbackClient client =
        new TestDnsFallbackClient(
            hostname -> Arrays.asList(address(1), address(2)),
            (request, address) ->
                lastAddressByte(address) == 1
                    ? response(200, "[\"userinfo@poison.test\"]")
                    : response(200, "[\"learned.test\"]"));
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config("seed.test"), client);

    liveNodes.updateLiveNodes();

    assertEquals("learned.test", liveNodes.nextAsURI().getHost());
    assertEquals(Arrays.asList(1, 2), attemptedAddressBytes(client));
  }

  @Test
  public void testRuntimeRequestPreparationFailureTriesNextAddress() throws Exception {
    List<Integer> preparations = new CopyOnWriteArrayList<>();
    DnsFallbackSdkHttpClient client =
        new DnsFallbackSdkHttpClient() {
          @Override
          public boolean supportsDnsFallback(String scheme) {
            return true;
          }

          @Override
          public List<InetAddress> resolve(String hostname) throws IOException {
            return Arrays.asList(address(1), address(2));
          }

          @Override
          public List<InetAddress> resolve(String hostname, long timeoutMillis) throws IOException {
            return resolve(hostname);
          }

          @Override
          public ExecutableHttpRequest prepareRequestForAddress(
              HttpExecuteRequest request, InetAddress address) {
            int addressByte = lastAddressByte(address);
            preparations.add(addressByte);
            if (addressByte == 1) {
              throw new IllegalStateException("simulated transport preparation failure");
            }
            return new ExecutableHttpRequest() {
              @Override
              public HttpExecuteResponse call() {
                return response(200, "[\"learned.test\"]");
              }

              @Override
              public void abort() {}
            };
          }

          @Override
          public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
            throw new AssertionError("address-aware path must be used");
          }

          @Override
          public void close() {}

          @Override
          public String clientName() {
            return "RuntimePreparationFailureClient";
          }
        };
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config("seed.test"), client);

    liveNodes.updateLiveNodes();

    assertEquals("learned.test", liveNodes.nextAsURI().getHost());
    assertEquals(Arrays.asList(1, 2), preparations);
  }

  @Test
  public void testLaterStartedRefreshCannotBeOverwrittenByOlderRefresh() throws Exception {
    assertOverlappingRefreshes(true);
  }

  @Test
  public void testLaterStartedRefreshPublishesAfterOlderRefresh() throws Exception {
    assertOverlappingRefreshes(false);
  }

  private void assertOverlappingRefreshes(boolean newerCompletesFirst) throws Exception {
    CountDownLatch firstResponseStarted = new CountDownLatch(1);
    CountDownLatch releaseFirstResponse = new CountDownLatch(1);
    CountDownLatch secondResponseStarted = new CountDownLatch(1);
    CountDownLatch releaseSecondResponse = new CountDownLatch(1);
    AtomicInteger calls = new AtomicInteger();
    SdkHttpClient client =
        new SdkHttpClient() {
          @Override
          public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
            return new ExecutableHttpRequest() {
              @Override
              public HttpExecuteResponse call() throws IOException {
                int call = calls.incrementAndGet();
                if (call == 1) {
                  firstResponseStarted.countDown();
                  try {
                    CountDownLatch release =
                        newerCompletesFirst ? releaseFirstResponse : secondResponseStarted;
                    if (!release.await(5, TimeUnit.SECONDS)) {
                      throw new IOException("timed out waiting to complete older refresh");
                    }
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("interrupted waiting to release older refresh", e);
                  }
                  return response(200, "[\"stale.test\"]");
                }
                secondResponseStarted.countDown();
                if (!newerCompletesFirst) {
                  try {
                    if (!releaseSecondResponse.await(5, TimeUnit.SECONDS)) {
                      throw new IOException("timed out waiting to complete newer refresh");
                    }
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("interrupted waiting to release newer refresh", e);
                  }
                }
                return response(200, "[\"fresh.test\"]");
              }

              @Override
              public void abort() {}
            };
          }

          @Override
          public void close() {}

          @Override
          public String clientName() {
            return "OverlappingRefreshClient";
          }
        };
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config("seed.test"), client);
    AtomicReference<Throwable> failure = new AtomicReference<>();
    Thread older = updater(liveNodes, failure);
    Thread newer = updater(liveNodes, failure);

    older.start();
    assertTrue(firstResponseStarted.await(5, TimeUnit.SECONDS));
    newer.start();
    assertTrue(secondResponseStarted.await(5, TimeUnit.SECONDS));
    if (newerCompletesFirst) {
      newer.join(5_000);
      assertFalse(newer.isAlive());
      assertEquals("fresh.test", liveNodes.nextAsURI().getHost());
      releaseFirstResponse.countDown();
      older.join(5_000);
    } else {
      older.join(5_000);
      assertFalse(older.isAlive());
      assertEquals("stale.test", liveNodes.nextAsURI().getHost());
      releaseSecondResponse.countDown();
      newer.join(5_000);
    }

    assertFalse(older.isAlive());
    assertFalse(newer.isAlive());
    assertNull(failure.get());
    assertEquals("fresh.test", liveNodes.nextAsURI().getHost());
  }

  private static Thread updater(
      AlternatorLiveNodes liveNodes, AtomicReference<Throwable> updateFailure) {
    return new Thread(
        () -> {
          try {
            liveNodes.updateLiveNodes();
          } catch (Throwable e) {
            updateFailure.compareAndSet(null, e);
          }
        });
  }

  private static AlternatorConfig config(String seedHost) {
    return AlternatorConfig.builder()
        .withSeedHost(seedHost)
        .withScheme("http")
        .withPort(8000)
        .build();
  }

  private static InetAddress address(int lastByte) throws IOException {
    return InetAddress.getByAddress(
        "address-" + lastByte, new byte[] {(byte) 192, 0, 2, (byte) lastByte});
  }

  private static int lastAddressByte(InetAddress address) {
    byte[] bytes = address.getAddress();
    return bytes[bytes.length - 1] & 0xff;
  }

  private static List<Integer> attemptedAddressBytes(TestDnsFallbackClient client) {
    List<Integer> result = new ArrayList<>();
    for (Attempt attempt : client.attempts) {
      result.add(lastAddressByte(attempt.address));
    }
    return result;
  }

  private static List<String> attemptedAddresses(TestDnsFallbackClient client) {
    List<String> result = new ArrayList<>();
    for (Attempt attempt : client.attempts) {
      result.add(attempt.request.host() + "@" + lastAddressByte(attempt.address));
    }
    return result;
  }

  private static HttpExecuteResponse response(int status, String body) {
    return response(status, body.getBytes(StandardCharsets.UTF_8));
  }

  private static HttpExecuteResponse response(int status, byte[] body) {
    return HttpExecuteResponse.builder()
        .response(SdkHttpFullResponse.builder().statusCode(status).build())
        .responseBody(AbortableInputStream.create(new ByteArrayInputStream(body)))
        .build();
  }

  private static HttpExecuteResponse responseWithoutBody(int status) {
    return HttpExecuteResponse.builder()
        .response(SdkHttpFullResponse.builder().statusCode(status).build())
        .build();
  }

  private interface Resolver {
    List<InetAddress> resolve(String hostname) throws IOException;
  }

  private interface AddressHandler {
    HttpExecuteResponse execute(SdkHttpRequest request, InetAddress address) throws IOException;
  }

  private static final class Attempt {
    final SdkHttpRequest request;
    final InetAddress address;

    Attempt(SdkHttpRequest request, InetAddress address) {
      this.request = request;
      this.address = address;
    }
  }

  private static final class TestDnsFallbackClient implements DnsFallbackSdkHttpClient {
    final List<Attempt> attempts = new CopyOnWriteArrayList<>();
    final List<Boolean> seedResolutionPriorities = new CopyOnWriteArrayList<>();
    private final Resolver resolver;
    private final AddressHandler handler;

    TestDnsFallbackClient(Resolver resolver, AddressHandler handler) {
      this.resolver = resolver;
      this.handler = handler;
    }

    @Override
    public boolean supportsDnsFallback(String scheme) {
      return true;
    }

    @Override
    public List<InetAddress> resolve(String hostname) throws IOException {
      return resolver.resolve(hostname);
    }

    @Override
    public List<InetAddress> resolve(String hostname, long timeoutMillis) throws IOException {
      return resolver.resolve(hostname);
    }

    @Override
    public List<InetAddress> resolve(String hostname, long timeoutMillis, boolean seedCandidate)
        throws IOException {
      seedResolutionPriorities.add(seedCandidate);
      return resolver.resolve(hostname);
    }

    @Override
    public ExecutableHttpRequest prepareRequestForAddress(
        HttpExecuteRequest request, InetAddress address) {
      SdkHttpRequest httpRequest = request.httpRequest();
      return new ExecutableHttpRequest() {
        @Override
        public HttpExecuteResponse call() throws IOException {
          attempts.add(new Attempt(httpRequest, address));
          return handler.execute(httpRequest, address);
        }

        @Override
        public void abort() {}
      };
    }

    @Override
    public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
      throw new AssertionError("address-aware path must be used");
    }

    @Override
    public void close() {}

    @Override
    public String clientName() {
      return "TestDnsFallbackClient";
    }
  }
}
