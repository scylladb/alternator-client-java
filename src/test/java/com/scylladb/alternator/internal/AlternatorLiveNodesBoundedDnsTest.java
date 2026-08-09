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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.scylladb.alternator.AlternatorConfig;
import com.sun.net.httpserver.HttpServer;
import java.io.ByteArrayInputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.SSLSocketFactory;
import org.apache.http.conn.DnsResolver;
import org.junit.Test;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpFullResponse;

/** End-to-end stalled-seed fairness tests for both DNS-fallback polling wrappers. */
public class AlternatorLiveNodesBoundedDnsTest {

  private static final LiveNodesPollingLimits TEST_LIMITS =
      new LiveNodesPollingLimits(100, 8, 500, 500, 500, 600, 2_000, 1024, 1024);

  @Test(timeout = 5000)
  public void testCrtStalledFirstSeedDoesNotBlockHealthySeedAndLaterRecovers() throws Exception {
    InetAddress loopback = InetAddress.getByName("127.0.0.1");
    HttpServer server = HttpServer.create(new InetSocketAddress(loopback, 0), 0);
    server.createContext(
        "/localnodes",
        exchange -> {
          byte[] response = "[\"learned.test\"]".getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(200, response.length);
          try (OutputStream output = exchange.getResponseBody()) {
            output.write(response);
          }
        });
    server.start();

    BoundedDnsResolver.Service service = new BoundedDnsResolver.Service(2, 4, 8);
    StallingResolver stallingResolver = new StallingResolver(loopback);
    IpDnsFallbackSdkHttpClient.Resolver resolver = stallingResolver::resolve;
    TrackingClient delegate = new TrackingClient();
    IpDnsFallbackSdkHttpClient client =
        new IpDnsFallbackSdkHttpClient(
            delegate,
            null,
            resolver,
            (SSLSocketFactory) SSLSocketFactory.getDefault(),
            TEST_LIMITS,
            service);
    try {
      assertStalledSeedFallbackAndRecovery(
          client, server.getAddress().getPort(), stallingResolver, service);
    } finally {
      stallingResolver.release();
      client.close();
      service.close();
      server.stop(0);
    }
    assertTrue(delegate.closed.get());
  }

  @Test(timeout = 5000)
  public void testApacheStalledFirstSeedDoesNotBlockHealthySeedAndLaterRecovers() throws Exception {
    InetAddress loopback = InetAddress.getByName("127.0.0.1");
    BoundedDnsResolver.Service service = new BoundedDnsResolver.Service(2, 4, 8);
    StallingResolver stallingResolver = new StallingResolver(loopback);
    DnsResolver resolver = stallingResolver::resolveArray;
    TrackingClient delegate = new TrackingClient();
    ApacheDnsFallbackSdkHttpClient client =
        new ApacheDnsFallbackSdkHttpClient(
            delegate,
            resolver,
            (hostname, address) -> new LearnedResponseClient(),
            TEST_LIMITS,
            service);
    try {
      assertStalledSeedFallbackAndRecovery(client, 8000, stallingResolver, service);
    } finally {
      stallingResolver.release();
      client.close();
      service.close();
    }
    assertTrue(delegate.closed.get());
  }

  private static void assertStalledSeedFallbackAndRecovery(
      DnsFallbackSdkHttpClient client,
      int port,
      StallingResolver resolver,
      BoundedDnsResolver.Service service)
      throws Exception {
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedHosts(Arrays.asList("seed-a.test", "seed-b.test"))
            .withScheme("http")
            .withPort(port)
            .build();
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, client, TEST_LIMITS);

    liveNodes.updateLiveNodes();

    assertTrue(resolver.stalledEntered.await(1, TimeUnit.SECONDS));
    assertEquals("learned.test", liveNodes.nextAsURI().getHost());
    assertEquals(1, resolver.stalledCalls.get());
    assertEquals(1, resolver.healthyCalls.get());
    assertTrue(service.largestPoolSize() <= 2);

    resolver.release();
    awaitNoInFlight(service);
    assertEquals(resolver.address, client.resolve("seed-a.test", 500).get(0));
    assertEquals(2, resolver.stalledCalls.get());
  }

  private static void awaitNoInFlight(BoundedDnsResolver.Service service) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
    while (service.inFlightCount() != 0 && System.nanoTime() < deadline) {
      Thread.yield();
    }
    assertEquals(0, service.inFlightCount());
  }

  private static final class StallingResolver {
    final InetAddress address;
    final CountDownLatch stalledEntered = new CountDownLatch(1);
    final CountDownLatch releaseStalled = new CountDownLatch(1);
    final AtomicInteger stalledCalls = new AtomicInteger();
    final AtomicInteger healthyCalls = new AtomicInteger();

    StallingResolver(InetAddress address) {
      this.address = address;
    }

    List<InetAddress> resolve(String hostname) {
      if ("seed-a.test".equals(hostname)) {
        stalledCalls.incrementAndGet();
        stalledEntered.countDown();
        boolean released = false;
        while (!released) {
          try {
            released = releaseStalled.await(20, TimeUnit.MILLISECONDS);
          } catch (InterruptedException ignored) {
            // Model a platform resolver that cannot be cancelled or interrupted.
          }
        }
      } else {
        healthyCalls.incrementAndGet();
      }
      return Collections.singletonList(address);
    }

    InetAddress[] resolveArray(String hostname) {
      List<InetAddress> resolved = resolve(hostname);
      return resolved.toArray(new InetAddress[0]);
    }

    void release() {
      releaseStalled.countDown();
    }
  }

  private static final class LearnedResponseClient implements SdkHttpClient {
    @Override
    public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
      return new ExecutableHttpRequest() {
        @Override
        public HttpExecuteResponse call() {
          return HttpExecuteResponse.builder()
              .response(SdkHttpFullResponse.builder().statusCode(200).build())
              .responseBody(
                  AbortableInputStream.create(
                      new ByteArrayInputStream(
                          "[\"learned.test\"]".getBytes(StandardCharsets.UTF_8))))
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
      return "LearnedResponseClient";
    }
  }

  private static final class TrackingClient implements SdkHttpClient {
    final AtomicBoolean closed = new AtomicBoolean();

    @Override
    public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
      throw new AssertionError("addressed polling must not use the delegate");
    }

    @Override
    public void close() {
      closed.set(true);
    }

    @Override
    public String clientName() {
      return "TrackingClient";
    }
  }
}
