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
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;

/** Deterministic negative DNS fallback tests for DRIVER-819 and GitHub issue #156. */
public class AlternatorLiveNodesDnsFallbackTest {

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
                  return response(200, "[\"bad host\"]");
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
  public void testDirectAddressWrapperPreservesLogicalHeadersAndRawQuery() throws Exception {
    AtomicReference<SdkHttpRequest> captured = new AtomicReference<>();
    SdkHttpClient delegate = capturingClient(captured);
    IpDnsFallbackSdkHttpClient client =
        new IpDnsFallbackSdkHttpClient(delegate, hostname -> Arrays.asList(address(9)));
    SdkHttpRequest logicalRequest =
        SdkHttpRequest.builder()
            .uri(URI.create("http://signed.test:8000/localnodes?dc=a%26b"))
            .method(SdkHttpMethod.GET)
            .putHeader("Host", "signed.test:8000")
            .putHeader("Authorization", "signature-over-logical-host")
            .build();

    HttpExecuteResponse executeResponse =
        client
            .prepareRequestForAddress(
                HttpExecuteRequest.builder().request(logicalRequest).build(), address(9))
            .call();
    executeResponse.responseBody().get().close();

    assertEquals("192.0.2.9", captured.get().host());
    assertEquals("/localnodes", captured.get().getUri().getRawPath());
    assertEquals("dc=a%26b", captured.get().getUri().getRawQuery());
    assertEquals("signed.test:8000", captured.get().firstMatchingHeader("Host").get());
    assertEquals(
        "signature-over-logical-host", captured.get().firstMatchingHeader("Authorization").get());
    assertTrue(client.supportsDnsFallback("http"));
    assertFalse(
        "numeric HTTPS endpoint would change TLS identity", client.supportsDnsFallback("https"));
    client.close();
  }

  @Test
  public void testDirectAddressWrapperBracketsIpv6Endpoint() throws Exception {
    AtomicReference<SdkHttpRequest> captured = new AtomicReference<>();
    IpDnsFallbackSdkHttpClient client = new IpDnsFallbackSdkHttpClient(capturingClient(captured));
    InetAddress address = InetAddress.getByName("2001:db8::9");
    SdkHttpRequest logicalRequest =
        SdkHttpRequest.builder()
            .uri(URI.create("http://logical.test:8000/localnodes"))
            .method(SdkHttpMethod.GET)
            .putHeader("Host", "logical.test:8000")
            .build();

    HttpExecuteResponse response =
        client
            .prepareRequestForAddress(
                HttpExecuteRequest.builder().request(logicalRequest).build(), address)
            .call();
    response.responseBody().get().close();

    URI addressedUri = captured.get().getUri();
    assertTrue(addressedUri.toString().startsWith("http://["));
    assertEquals(8000, addressedUri.getPort());
    assertEquals("/localnodes", addressedUri.getRawPath());
    assertEquals("logical.test:8000", captured.get().firstMatchingHeader("Host").get());
    client.close();
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

  private static HttpExecuteResponse response(int status, String body) {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    return HttpExecuteResponse.builder()
        .response(SdkHttpFullResponse.builder().statusCode(status).build())
        .responseBody(AbortableInputStream.create(new ByteArrayInputStream(bytes)))
        .build();
  }

  private static HttpExecuteResponse responseWithoutBody(int status) {
    return HttpExecuteResponse.builder()
        .response(SdkHttpFullResponse.builder().statusCode(status).build())
        .build();
  }

  private static SdkHttpClient capturingClient(AtomicReference<SdkHttpRequest> captured) {
    return new SdkHttpClient() {
      @Override
      public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
        captured.set(request.httpRequest());
        return new ExecutableHttpRequest() {
          @Override
          public HttpExecuteResponse call() {
            return response(200, "[]");
          }

          @Override
          public void abort() {}
        };
      }

      @Override
      public void close() {}

      @Override
      public String clientName() {
        return "CapturingClient";
      }
    };
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
