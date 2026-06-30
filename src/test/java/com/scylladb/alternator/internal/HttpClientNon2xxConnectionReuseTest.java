package com.scylladb.alternator.internal;

import static org.junit.Assert.assertEquals;

import com.scylladb.alternator.AlternatorConfig;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.http.async.AsyncExecuteRequest;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.async.SdkAsyncHttpResponseHandler;
import software.amazon.awssdk.http.async.SdkHttpContentPublisher;

/**
 * Verifies configured HTTP clients keep reusable TCP connections after drained non-2xx responses.
 */
public class HttpClientNon2xxConnectionReuseTest {

  private static final int REQUESTS = 5;
  private static final int[] NON_2XX_STATUSES = {400, 500};

  @Test(timeout = 30000)
  public void apacheSyncReusesConnectionsAfterNon2xxResponses() throws Exception {
    assertSyncClientReusesConnections(
        "apache-sync", ApacheSyncClientFactory.create(null, config(), null));
  }

  @Test(timeout = 30000)
  public void crtSyncReusesConnectionsAfterNon2xxResponses() throws Exception {
    assertSyncClientReusesConnections(
        "crt-sync", CrtSyncClientFactory.create(null, config(), null));
  }

  @Test(timeout = 30000)
  public void nettyAsyncReusesConnectionsAfterNon2xxResponses() throws Exception {
    assertAsyncClientReusesConnections(
        "netty-async", NettyAsyncClientFactory.create(null, config(), null));
  }

  @Test(timeout = 30000)
  public void crtAsyncReusesConnectionsAfterNon2xxResponses() throws Exception {
    assertAsyncClientReusesConnections(
        "crt-async", CrtAsyncClientFactory.create(null, config(), null));
  }

  private AlternatorConfig config() {
    return AlternatorConfig.builder()
        .withMaxConnections(1)
        .withConnectionAcquisitionTimeoutMs(5_000)
        .withConnectionMaxIdleTimeMs(60_000)
        .withConnectionTimeoutMs(5_000)
        .build();
  }

  private void assertSyncClientReusesConnections(String clientName, SdkHttpClient client)
      throws Exception {
    try {
      for (int status : NON_2XX_STATUSES) {
        assertSyncClientReusesConnections(clientName, status, client);
      }
    } finally {
      client.close();
    }
  }

  private void assertSyncClientReusesConnections(
      String clientName, int status, SdkHttpClient client) throws Exception {
    ReuseProbeServer server = new ReuseProbeServer(status);
    server.start();
    try {
      for (int i = 0; i < REQUESTS; i++) {
        HttpExecuteResponse response =
            client
                .prepareRequest(HttpExecuteRequest.builder().request(server.request()).build())
                .call();
        assertEquals(status, response.httpResponse().statusCode());
        if (response.responseBody().isPresent()) {
          drainAndClose(response.responseBody().get());
        }
      }
      assertConnectionReuse(clientName, status, server);
    } finally {
      server.stop();
    }
  }

  private void assertAsyncClientReusesConnections(String clientName, SdkAsyncHttpClient client)
      throws Exception {
    try {
      for (int status : NON_2XX_STATUSES) {
        assertAsyncClientReusesConnections(clientName, status, client);
      }
    } finally {
      client.close();
    }
  }

  private void assertAsyncClientReusesConnections(
      String clientName, int status, SdkAsyncHttpClient client) throws Exception {
    ReuseProbeServer server = new ReuseProbeServer(status);
    server.start();
    try {
      for (int i = 0; i < REQUESTS; i++) {
        assertEquals(
            status, executeAsync(client, server.request()).get(10, TimeUnit.SECONDS).intValue());
      }
      assertConnectionReuse(clientName, status, server);
    } finally {
      server.stop();
    }
  }

  private void assertConnectionReuse(String clientName, int status, ReuseProbeServer server)
      throws Exception {
    server.awaitRequests();
    assertEquals(
        clientName + " status " + status + " should reach the server",
        REQUESTS,
        server.requestCount());
    assertEquals(
        clientName + " status " + status + " should reuse one TCP connection",
        1,
        server.acceptedConnections());
    assertEquals(
        clientName + " status " + status + " should use one client TCP port",
        1,
        server.uniqueRemotePorts());
  }

  private void drainAndClose(AbortableInputStream body) throws IOException {
    try (AbortableInputStream stream = body) {
      byte[] buffer = new byte[1024];
      while (stream.read(buffer) != -1) {
        // Drain response so the connection can return to the pool.
      }
    }
  }

  private CompletableFuture<Integer> executeAsync(
      SdkAsyncHttpClient client, SdkHttpRequest request) {
    CompletableFuture<Integer> result = new CompletableFuture<>();
    AtomicInteger statusCode = new AtomicInteger(-1);
    AsyncExecuteRequest executeRequest =
        AsyncExecuteRequest.builder()
            .request(request)
            .requestContentPublisher(new EmptyPublisher())
            .responseHandler(
                new SdkAsyncHttpResponseHandler() {
                  @Override
                  public void onHeaders(SdkHttpResponse headers) {
                    statusCode.set(headers.statusCode());
                  }

                  @Override
                  public void onStream(Publisher<ByteBuffer> stream) {
                    stream.subscribe(
                        new Subscriber<ByteBuffer>() {
                          @Override
                          public void onSubscribe(Subscription subscription) {
                            subscription.request(Long.MAX_VALUE);
                          }

                          @Override
                          public void onNext(ByteBuffer byteBuffer) {}

                          @Override
                          public void onError(Throwable error) {
                            result.completeExceptionally(error);
                          }

                          @Override
                          public void onComplete() {
                            result.complete(statusCode.get());
                          }
                        });
                  }

                  @Override
                  public void onError(Throwable error) {
                    result.completeExceptionally(error);
                  }
                })
            .build();

    client
        .execute(executeRequest)
        .whenComplete(
            (ignored, error) -> {
              if (error != null) {
                result.completeExceptionally(error);
              }
            });
    return result;
  }

  private static class ReuseProbeServer {
    private final int status;
    private final List<Integer> remotePorts = new ArrayList<>();
    private final List<Socket> activeSockets = new ArrayList<>();
    private final AtomicInteger requestCount = new AtomicInteger(0);
    private final AtomicInteger acceptedConnections = new AtomicInteger(0);
    private volatile boolean running = true;
    private volatile IOException serverError;
    private ServerSocket server;
    private Thread serverThread;
    private int port;

    ReuseProbeServer(int status) {
      this.status = status;
    }

    void start() throws IOException {
      server = new ServerSocket(0);
      port = server.getLocalPort();
      serverThread = new Thread(this::serve, "non-2xx-reuse-probe");
      serverThread.setDaemon(true);
      serverThread.start();
    }

    private void serve() {
      try {
        while (running && requestCount.get() < REQUESTS) {
          Socket socket = server.accept();
          acceptedConnections.incrementAndGet();
          synchronized (activeSockets) {
            activeSockets.add(socket);
          }
          handleSocket(socket);
        }
      } catch (IOException e) {
        if (running) {
          serverError = e;
        }
      }
    }

    private void handleSocket(Socket socket) throws IOException {
      socket.setSoTimeout(10_000);
      try (Socket current = socket) {
        InputStream input = current.getInputStream();
        OutputStream output = current.getOutputStream();
        while (running && requestCount.get() < REQUESTS) {
          if (!readRequest(input)) {
            return;
          }
          synchronized (remotePorts) {
            remotePorts.add(current.getPort());
          }
          requestCount.incrementAndGet();
          writeResponse(output);
        }
      } finally {
        synchronized (activeSockets) {
          activeSockets.remove(socket);
        }
      }
    }

    private boolean readRequest(InputStream input) throws IOException {
      String requestLine = readLine(input);
      if (requestLine == null) {
        return false;
      }

      int contentLength = 0;
      while (true) {
        String header = readLine(input);
        if (header == null || header.isEmpty()) {
          break;
        }
        if (header.regionMatches(true, 0, "Content-Length:", 0, "Content-Length:".length())) {
          contentLength = Integer.parseInt(header.substring("Content-Length:".length()).trim());
        }
      }
      drainBytes(input, contentLength);
      return true;
    }

    private String readLine(InputStream input) throws IOException {
      StringBuilder line = new StringBuilder();
      while (true) {
        int b = input.read();
        if (b == -1) {
          return line.length() == 0 ? null : line.toString();
        }
        if (b == '\r') {
          continue;
        }
        if (b == '\n') {
          return line.toString();
        }
        line.append((char) b);
      }
    }

    private void drainBytes(InputStream input, int contentLength) throws IOException {
      for (int i = 0; i < contentLength; i++) {
        if (input.read() == -1) {
          return;
        }
      }
    }

    private void writeResponse(OutputStream output) throws IOException {
      byte[] body = ("response-" + status).getBytes(StandardCharsets.US_ASCII);
      String response =
          "HTTP/1.1 "
              + status
              + " Test\r\n"
              + "Content-Length: "
              + body.length
              + "\r\n"
              + "Connection: keep-alive\r\n"
              + "\r\n";
      output.write(response.getBytes(StandardCharsets.US_ASCII));
      output.write(body);
      output.flush();
    }

    SdkHttpRequest request() {
      return SdkHttpRequest.builder()
          .uri(URI.create("http://127.0.0.1:" + port + "/test"))
          .method(SdkHttpMethod.GET)
          .putHeader("Connection", "keep-alive")
          .build();
    }

    void awaitRequests() throws Exception {
      serverThread.join(2_000);
      if (serverError != null) {
        throw serverError;
      }
    }

    int requestCount() {
      return requestCount.get();
    }

    int acceptedConnections() {
      return acceptedConnections.get();
    }

    int uniqueRemotePorts() {
      synchronized (remotePorts) {
        Set<Integer> uniquePorts = new LinkedHashSet<>(remotePorts);
        return uniquePorts.size();
      }
    }

    void stop() throws IOException, InterruptedException {
      running = false;
      synchronized (activeSockets) {
        for (Socket socket : activeSockets) {
          socket.close();
        }
      }
      server.close();
      serverThread.join(2_000);
    }
  }

  private static class EmptyPublisher implements SdkHttpContentPublisher {
    @Override
    public Optional<Long> contentLength() {
      return Optional.of(0L);
    }

    @Override
    public void subscribe(Subscriber<? super ByteBuffer> subscriber) {
      subscriber.onSubscribe(
          new Subscription() {
            @Override
            public void request(long n) {
              subscriber.onComplete();
            }

            @Override
            public void cancel() {}
          });
    }
  }
}
