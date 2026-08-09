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

import com.scylladb.alternator.TlsConfig;
import com.sun.net.httpserver.HttpServer;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLSocketFactory;
import org.junit.Assume;
import org.junit.Test;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpFullResponse;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;

/** Tests for the bounded one-shot addressed transport used by CRT polling. */
public class IpDnsFallbackSdkHttpClientTest {

  @Test
  public void testHttpAddressedGetPreservesLogicalRequestAndReadsResponse() throws Exception {
    InetAddress loopback = InetAddress.getByName("127.0.0.1");
    AtomicReference<String> target = new AtomicReference<>();
    AtomicReference<String> host = new AtomicReference<>();
    AtomicReference<String> authorization = new AtomicReference<>();
    HttpServer server = HttpServer.create(new InetSocketAddress(loopback, 0), 0);
    server.createContext(
        "/localnodes",
        exchange -> {
          target.set(exchange.getRequestURI().toASCIIString());
          host.set(exchange.getRequestHeaders().getFirst("Host"));
          authorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
          byte[] body = "[\"learned.test\"]".getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().add("X-Addressed", "true");
          exchange.sendResponseHeaders(200, body.length);
          try (OutputStream output = exchange.getResponseBody()) {
            output.write(body);
          }
        });
    server.start();
    TrackingClient delegate = new TrackingClient();
    IpDnsFallbackSdkHttpClient client = new IpDnsFallbackSdkHttpClient(delegate);
    try {
      int port = server.getAddress().getPort();
      SdkHttpRequest request =
          logicalRequest("http", "signed.test", port, "/localnodes?dc=a%26b").toBuilder()
              .putHeader("Authorization", "signature-over-logical-host")
              .build();

      HttpExecuteResponse response = callAddressed(client, request, loopback);

      assertEquals(200, response.httpResponse().statusCode());
      assertEquals("true", response.httpResponse().firstMatchingHeader("X-Addressed").get());
      assertEquals("[\"learned.test\"]", readBody(response));
      assertEquals("/localnodes?dc=a%26b", target.get());
      assertEquals("signed.test:" + port, host.get());
      assertEquals("signature-over-logical-host", authorization.get());
      assertEquals(0, delegate.prepared.get());
      assertEquals(0, client.activeRequestCount());
    } finally {
      client.close();
      server.stop(0);
    }
    assertTrue(delegate.closed.get());
    assertTrue(client.isTimeoutExecutorShutdown());
    assertTrue(client.isResolverExecutorShutdown());
  }

  @Test(timeout = 5000)
  public void testDnsResolutionTimeoutAndExecutorAreBounded() throws Exception {
    CountDownLatch resolverEntered = new CountDownLatch(1);
    CountDownLatch releaseResolver = new CountDownLatch(1);
    AtomicInteger resolverCalls = new AtomicInteger();
    TrackingClient delegate = new TrackingClient();
    IpDnsFallbackSdkHttpClient client =
        new IpDnsFallbackSdkHttpClient(
            delegate,
            null,
            hostname -> {
              resolverCalls.incrementAndGet();
              resolverEntered.countDown();
              boolean released = false;
              while (!released) {
                try {
                  released = releaseResolver.await(20, TimeUnit.MILLISECONDS);
                } catch (InterruptedException ignored) {
                  // Model a platform resolver that does not honor interruption.
                }
              }
              return Collections.singletonList(InetAddress.getLoopbackAddress());
            },
            systemSslSocketFactory(),
            new IpDnsFallbackSdkHttpClient.Limits(100, 1_000, 1_000, 2_000, 1024, 1024));
    try {
      assertDnsTimesOut(client);
      assertTrue(resolverEntered.await(1, TimeUnit.SECONDS));
      assertDnsTimesOut(client);
      assertDnsTimesOut(client);
      assertEquals("a stuck resolver must occupy at most one worker", 1, resolverCalls.get());
    } finally {
      releaseResolver.countDown();
      client.close();
    }
    assertTrue(delegate.closed.get());
    assertTrue(client.isResolverExecutorShutdown());
  }

  @Test
  public void testDefaultBoundsAreFinite() {
    assertEquals(3_000, IpDnsFallbackSdkHttpClient.DEFAULT_LIMITS.dnsTimeoutMillis);
    assertEquals(3_000, IpDnsFallbackSdkHttpClient.DEFAULT_LIMITS.connectTimeoutMillis);
    assertEquals(5_000, IpDnsFallbackSdkHttpClient.DEFAULT_LIMITS.readTimeoutMillis);
    assertEquals(10_000, IpDnsFallbackSdkHttpClient.DEFAULT_LIMITS.responseTimeoutMillis);
    assertEquals(64 * 1024, IpDnsFallbackSdkHttpClient.DEFAULT_LIMITS.maxHeaderBytes);
    assertEquals(1024 * 1024, IpDnsFallbackSdkHttpClient.DEFAULT_LIMITS.maxBodyBytes);
  }

  @Test(timeout = 5000)
  public void testClientCloseCancelsWaitingDnsResolution() throws Exception {
    CountDownLatch resolverEntered = new CountDownLatch(1);
    CountDownLatch releaseResolver = new CountDownLatch(1);
    TrackingClient delegate = new TrackingClient();
    IpDnsFallbackSdkHttpClient client =
        new IpDnsFallbackSdkHttpClient(
            delegate,
            null,
            hostname -> {
              resolverEntered.countDown();
              boolean released = false;
              while (!released) {
                try {
                  released = releaseResolver.await(20, TimeUnit.MILLISECONDS);
                } catch (InterruptedException ignored) {
                  // Model a platform resolver that ignores shutdown interruption.
                }
              }
              return Collections.singletonList(InetAddress.getLoopbackAddress());
            },
            systemSslSocketFactory(),
            new IpDnsFallbackSdkHttpClient.Limits(4_000, 1_000, 1_000, 4_000, 1024, 1024));
    ExecutorService caller = Executors.newSingleThreadExecutor();
    try {
      Future<?> resolution = caller.submit(() -> client.resolve("blocked.test"));
      assertTrue(resolverEntered.await(1, TimeUnit.SECONDS));

      client.close();

      assertFutureFailedWithIOException(resolution);
      assertTrue(delegate.closed.get());
      assertTrue(client.isResolverExecutorShutdown());
    } finally {
      releaseResolver.countDown();
      client.close();
      caller.shutdownNow();
    }
  }

  @Test
  public void testChunkedErrorResponseIsReturnedAndSocketIsReleased() throws Exception {
    InetAddress loopback = InetAddress.getByName("127.0.0.1");
    HttpServer server = HttpServer.create(new InetSocketAddress(loopback, 0), 0);
    server.createContext(
        "/localnodes",
        exchange -> {
          byte[] body = "temporarily unavailable".getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(503, 0);
          try (OutputStream output = exchange.getResponseBody()) {
            output.write(body);
          }
        });
    server.start();
    IpDnsFallbackSdkHttpClient client = new IpDnsFallbackSdkHttpClient(new TrackingClient());
    try {
      int port = server.getAddress().getPort();
      HttpExecuteResponse response =
          callAddressed(
              client, logicalRequest("http", "logical.test", port, "/localnodes"), loopback);

      assertEquals(503, response.httpResponse().statusCode());
      assertEquals("temporarily unavailable", readBody(response));
      assertEquals(0, client.activeRequestCount());
    } finally {
      client.close();
      server.stop(0);
    }
  }

  @Test
  public void testIpv6AddressedGetWhenLoopbackIsAvailable() throws Exception {
    InetAddress loopback = InetAddress.getByName("::1");
    HttpServer server;
    try {
      server = HttpServer.create(new InetSocketAddress(loopback, 0), 0);
    } catch (IOException e) {
      Assume.assumeNoException("IPv6 loopback is unavailable", e);
      return;
    }
    server.createContext(
        "/localnodes",
        exchange -> {
          byte[] body = "[]".getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(200, body.length);
          try (OutputStream output = exchange.getResponseBody()) {
            output.write(body);
          }
        });
    server.start();
    IpDnsFallbackSdkHttpClient client = new IpDnsFallbackSdkHttpClient(new TrackingClient());
    try {
      HttpExecuteResponse response =
          callAddressed(
              client,
              logicalRequest("http", "logical.test", server.getAddress().getPort(), "/localnodes"),
              loopback);
      assertEquals(200, response.httpResponse().statusCode());
      assertEquals("[]", readBody(response));
    } finally {
      client.close();
      server.stop(0);
    }
  }

  @Test(timeout = 5000)
  public void testTotalResponseTimeoutClosesStalledSocket() throws Exception {
    CountDownLatch requestReceived = new CountDownLatch(1);
    CountDownLatch clientClosedSocket = new CountDownLatch(1);
    try (RawServer server =
        new RawServer(
            socket -> {
              readRequestHeaders(socket.getInputStream());
              requestReceived.countDown();
              while (socket.getInputStream().read() >= 0) {
                // Wait for the timeout path to close the client socket.
              }
              clientClosedSocket.countDown();
            })) {
      TrackingClient delegate = new TrackingClient();
      IpDnsFallbackSdkHttpClient client = clientWithLimits(delegate, 1_000, 5_000, 200, 1024, 1024);
      try {
        ExecutableHttpRequest request =
            addressedRequest(client, logicalRequest(server.port()), server.address());

        try {
          request.call();
          fail("Expected total response timeout");
        } catch (SocketTimeoutException e) {
          assertTrue(e.getMessage().contains("exceeded 200 ms"));
        }

        assertTrue(requestReceived.await(1, TimeUnit.SECONDS));
        assertTrue(clientClosedSocket.await(1, TimeUnit.SECONDS));
        server.await();
        assertEquals(0, client.activeRequestCount());
      } finally {
        client.close();
      }
      assertTrue(delegate.closed.get());
    }
  }

  @Test(timeout = 5000)
  public void testTotalResponseTimeoutBeforeSocketCreationPreventsConnection() throws Exception {
    AtomicBoolean serverAcceptedConnection = new AtomicBoolean(false);
    try (RawServer server =
        new RawServer(
            socket -> {
              serverAcceptedConnection.set(true);
              socket
                  .getOutputStream()
                  .write(
                      "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n[]"
                          .getBytes(StandardCharsets.US_ASCII));
              socket.getOutputStream().flush();
            })) {
      ScheduledExecutorService timeoutScheduler = Executors.newSingleThreadScheduledExecutor();
      try {
        IpDnsFallbackSdkHttpClient.Limits limits =
            new IpDnsFallbackSdkHttpClient.Limits(1_000, 1_000, 200, 1024, 1024);
        OneShotAddressedHttpRequest request =
            new OneShotAddressedHttpRequest(
                HttpExecuteRequest.builder().request(logicalRequest(server.port())).build(),
                server.address(),
                systemSslSocketFactory(),
                true,
                limits,
                new OneShotAddressedHttpRequest.Lifecycle() {
                  @Override
                  public java.util.concurrent.ScheduledFuture<?> activate(
                      OneShotAddressedHttpRequest ignored, Runnable timeoutTask) {
                    timeoutTask.run();
                    return timeoutScheduler.schedule(() -> {}, 1, TimeUnit.HOURS);
                  }

                  @Override
                  public void deactivate(OneShotAddressedHttpRequest ignored) {}
                });

        try {
          request.call();
          fail("Expected total response timeout before connection");
        } catch (SocketTimeoutException e) {
          assertTrue(e.getMessage().contains("exceeded 200 ms"));
        }
        assertFalse(serverAcceptedConnection.get());
      } finally {
        timeoutScheduler.shutdownNow();
      }
    }
  }

  @Test(timeout = 5000)
  public void testAbortAndClientCloseReleaseActiveSockets() throws Exception {
    assertCancellationClosesSocket(false);
    assertCancellationClosesSocket(true);
  }

  @Test
  public void testRejectsOversizedHeadersAndBodies() throws Exception {
    assertRawResponseRejected(
        "HTTP/1.1 200 OK\r\nX-Large: " + "x".repeat(256) + "\r\n\r\n",
        new IpDnsFallbackSdkHttpClient.Limits(1_000, 1_000, 2_000, 128, 1024),
        "headers exceed");
    assertRawResponseRejected(
        "HTTP/1.1 200 OK\r\nContent-Length: 32\r\n\r\n" + "x".repeat(32),
        new IpDnsFallbackSdkHttpClient.Limits(1_000, 1_000, 2_000, 1024, 16),
        "body exceeds");
  }

  @Test
  public void testOrdinaryRequestsStillUseDelegate() throws Exception {
    TrackingClient delegate = new TrackingClient();
    IpDnsFallbackSdkHttpClient client = new IpDnsFallbackSdkHttpClient(delegate);
    try {
      HttpExecuteResponse response =
          client
              .prepareRequest(HttpExecuteRequest.builder().request(logicalRequest(80)).build())
              .call();
      assertEquals(204, response.httpResponse().statusCode());
      assertEquals(1, delegate.prepared.get());
    } finally {
      client.close();
    }
  }

  private static void assertCancellationClosesSocket(boolean closeClient) throws Exception {
    CountDownLatch requestReceived = new CountDownLatch(1);
    CountDownLatch clientClosedSocket = new CountDownLatch(1);
    try (RawServer server =
        new RawServer(
            socket -> {
              readRequestHeaders(socket.getInputStream());
              requestReceived.countDown();
              while (socket.getInputStream().read() >= 0) {
                // Wait for abort or client shutdown to close the socket.
              }
              clientClosedSocket.countDown();
            })) {
      TrackingClient delegate = new TrackingClient();
      IpDnsFallbackSdkHttpClient client =
          clientWithLimits(delegate, 1_000, 5_000, 4_000, 1024, 1024);
      ExecutorService caller = Executors.newSingleThreadExecutor();
      try {
        ExecutableHttpRequest request =
            addressedRequest(client, logicalRequest(server.port()), server.address());
        Future<HttpExecuteResponse> call = caller.submit(request::call);
        assertTrue(requestReceived.await(1, TimeUnit.SECONDS));
        assertEquals(1, client.activeRequestCount());

        if (closeClient) {
          client.close();
        } else {
          request.abort();
        }

        assertFutureFailedWithIOException(call);
        assertTrue(clientClosedSocket.await(1, TimeUnit.SECONDS));
        server.await();
        assertEquals(0, client.activeRequestCount());
        if (closeClient) {
          assertTrue(delegate.closed.get());
          assertTrue(client.isTimeoutExecutorShutdown());
        } else {
          assertFalse(delegate.closed.get());
          client.close();
        }
      } finally {
        caller.shutdownNow();
        client.close();
      }
    }
  }

  private static void assertDnsTimesOut(IpDnsFallbackSdkHttpClient client) throws Exception {
    long started = System.nanoTime();
    try {
      client.resolve("blocked.test");
      fail("Expected bounded DNS resolution timeout");
    } catch (SocketTimeoutException e) {
      assertTrue(e.getMessage().contains("exceeded 100 ms"));
    }
    long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
    assertTrue("DNS timeout took " + elapsedMillis + " ms", elapsedMillis < 1_000);
  }

  private static void assertRawResponseRejected(
      String rawResponse, IpDnsFallbackSdkHttpClient.Limits limits, String expectedMessage)
      throws Exception {
    try (RawServer server =
        new RawServer(
            socket -> {
              readRequestHeaders(socket.getInputStream());
              socket.getOutputStream().write(rawResponse.getBytes(StandardCharsets.ISO_8859_1));
              socket.getOutputStream().flush();
            })) {
      IpDnsFallbackSdkHttpClient client =
          new IpDnsFallbackSdkHttpClient(
              new TrackingClient(),
              null,
              hostname -> Collections.singletonList(server.address()),
              systemSslSocketFactory(),
              limits);
      try {
        try {
          addressedRequest(client, logicalRequest(server.port()), server.address()).call();
          fail("Expected bounded response rejection");
        } catch (IOException e) {
          assertTrue(e.getMessage(), e.getMessage().contains(expectedMessage));
        }
        server.await();
        assertEquals(0, client.activeRequestCount());
      } finally {
        client.close();
      }
    }
  }

  private static IpDnsFallbackSdkHttpClient clientWithLimits(
      TrackingClient delegate,
      int connectTimeout,
      int readTimeout,
      int responseTimeout,
      int maxHeaderBytes,
      int maxBodyBytes) {
    return new IpDnsFallbackSdkHttpClient(
        delegate,
        null,
        hostname -> Collections.singletonList(InetAddress.getLoopbackAddress()),
        systemSslSocketFactory(),
        new IpDnsFallbackSdkHttpClient.Limits(
            connectTimeout, readTimeout, responseTimeout, maxHeaderBytes, maxBodyBytes));
  }

  private static SSLSocketFactory systemSslSocketFactory() {
    return TlsContextFactory.createSslContext(TlsConfig.systemDefault()).getSocketFactory();
  }

  private static HttpExecuteResponse callAddressed(
      IpDnsFallbackSdkHttpClient client, SdkHttpRequest request, InetAddress address)
      throws IOException {
    return addressedRequest(client, request, address).call();
  }

  private static ExecutableHttpRequest addressedRequest(
      IpDnsFallbackSdkHttpClient client, SdkHttpRequest request, InetAddress address) {
    return client.prepareRequestForAddress(
        HttpExecuteRequest.builder().request(request).build(), address);
  }

  private static SdkHttpRequest logicalRequest(int port) {
    return logicalRequest("http", "logical.test", port, "/localnodes");
  }

  private static SdkHttpRequest logicalRequest(
      String scheme, String host, int port, String pathAndQuery) {
    return SdkHttpRequest.builder()
        .uri(URI.create(scheme + "://" + host + ":" + port + pathAndQuery))
        .method(SdkHttpMethod.GET)
        .putHeader("Host", host + ":" + port)
        .build();
  }

  private static String readBody(HttpExecuteResponse response) throws IOException {
    assertTrue(response.responseBody().isPresent());
    try (AbortableInputStream body = response.responseBody().get()) {
      return new String(body.readAllBytes(), StandardCharsets.UTF_8);
    }
  }

  private static void assertFutureFailedWithIOException(Future<?> future) throws Exception {
    try {
      future.get(2, TimeUnit.SECONDS);
      fail("Expected addressed call to fail after cancellation");
    } catch (ExecutionException e) {
      assertTrue(e.getCause().toString(), e.getCause() instanceof IOException);
    }
  }

  private static void readRequestHeaders(InputStream input) throws IOException {
    int matched = 0;
    int total = 0;
    byte[] end = {'\r', '\n', '\r', '\n'};
    while (matched < end.length) {
      int value = input.read();
      if (value < 0) {
        throw new IOException("Client closed before completing request headers");
      }
      if (++total > 64 * 1024) {
        throw new IOException("Request headers exceeded test bound");
      }
      matched = value == end[matched] ? matched + 1 : value == end[0] ? 1 : 0;
    }
  }

  private interface SocketHandler {
    void handle(Socket socket) throws Exception;
  }

  private static final class RawServer implements AutoCloseable {
    private final ServerSocket serverSocket;
    private final ExecutorService executor;
    private final AtomicReference<Socket> acceptedSocket = new AtomicReference<>();
    private final Future<?> handler;

    RawServer(SocketHandler socketHandler) throws IOException {
      serverSocket = new ServerSocket();
      serverSocket.bind(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0));
      executor =
          Executors.newSingleThreadExecutor(
              runnable -> {
                Thread thread = new Thread(runnable, "addressed-http-test-server");
                thread.setDaemon(true);
                return thread;
              });
      handler =
          executor.submit(
              () -> {
                try (Socket socket = serverSocket.accept()) {
                  acceptedSocket.set(socket);
                  socketHandler.handle(socket);
                }
                return null;
              });
    }

    int port() {
      return serverSocket.getLocalPort();
    }

    InetAddress address() {
      return serverSocket.getInetAddress();
    }

    void await() throws Exception {
      handler.get(2, TimeUnit.SECONDS);
    }

    @Override
    public void close() throws Exception {
      serverSocket.close();
      Socket socket = acceptedSocket.get();
      if (socket != null) {
        socket.close();
      }
      executor.shutdownNow();
      executor.awaitTermination(2, TimeUnit.SECONDS);
    }
  }

  private static final class TrackingClient implements SdkHttpClient {
    final AtomicInteger prepared = new AtomicInteger();
    final AtomicBoolean closed = new AtomicBoolean();

    @Override
    public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
      prepared.incrementAndGet();
      return new ExecutableHttpRequest() {
        @Override
        public HttpExecuteResponse call() {
          return HttpExecuteResponse.builder()
              .response(SdkHttpFullResponse.builder().statusCode(204).build())
              .responseBody(AbortableInputStream.create(new ByteArrayInputStream(new byte[0])))
              .build();
        }

        @Override
        public void abort() {}
      };
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
