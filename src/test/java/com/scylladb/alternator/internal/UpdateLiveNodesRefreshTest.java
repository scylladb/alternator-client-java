package com.scylladb.alternator.internal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import com.scylladb.alternator.AlternatorConfig;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Test;
import software.amazon.awssdk.http.Abortable;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpFullResponse;
import software.amazon.awssdk.http.SdkHttpRequest;

/**
 * Tests for {@link AlternatorLiveNodes#updateLiveNodes()} covering the refresh semantics: a
 * successful scope always merges in every configured seed via {@code mergeWithInitialNodes} (a seed
 * that fails while another scope/seed still succeeds this cycle stays in rotation — see {@code
 * AlternatorLiveNodesClusterDiscoveryTest#testClusterScopeKeepsSuccessfulDiscoveryWhenAnotherSeedFails}),
 * and a cycle where every scope fails outright prunes the nodes confirmed dead this cycle,
 * restoring the seed list only when that would otherwise empty the live list.
 */
public class UpdateLiveNodesRefreshTest {

  private static final class ControllableHttpClient implements SdkHttpClient {
    private final Set<String> downHosts = ConcurrentHashMap.newKeySet();
    private final java.util.Map<String, String> responseByHost =
        new java.util.concurrent.ConcurrentHashMap<>();
    private final String defaultResponse;
    private int statusCode = 200;
    final List<String> contactedHosts = new CopyOnWriteArrayList<>();

    ControllableHttpClient(String defaultResponse) {
      this.defaultResponse = defaultResponse;
    }

    void markDown(String host) {
      downHosts.add(host);
    }

    void setResponseForHost(String host, String json) {
      responseByHost.put(host, json);
    }

    void setStatusCode(int code) {
      this.statusCode = code;
    }

    @Override
    public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
      SdkHttpRequest req = request.httpRequest();
      String host = req.host();
      contactedHosts.add(host);
      int code = this.statusCode;
      return new ExecutableHttpRequest() {
        @Override
        public HttpExecuteResponse call() throws IOException {
          if (downHosts.contains(host)) {
            throw new IOException("simulated connection refused: " + host);
          }
          String body = responseByHost.getOrDefault(host, defaultResponse);
          byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
          return HttpExecuteResponse.builder()
              .response(SdkHttpFullResponse.builder().statusCode(code).build())
              .responseBody(AbortableInputStream.create(new ByteArrayInputStream(bytes)))
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
      return "ControllableHttpClient";
    }
  }

  private static Set<String> hostsOf(List<URI> uris) {
    Set<String> out = new HashSet<>();
    for (URI u : uris) out.add(u.getHost());
    return out;
  }

  @Test
  public void sourceNodeIsAddedEvenWhenAbsentFromDiscoveredList() throws Exception {
    // 10.0.0.1 responds but omits itself from its /localnodes list.
    ControllableHttpClient http = new ControllableHttpClient("[\"10.0.0.2\",\"10.0.0.3\"]");
    AlternatorConfig config =
        AlternatorConfig.builder().withSeedNode(URI.create("http://10.0.0.1:8000")).build();
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, http);

    liveNodes.updateLiveNodes();

    Set<String> live = hostsOf(liveNodes.getLiveNodes());
    assertTrue(
        "10.0.0.1 (responder) must be added even though it omitted itself",
        live.contains("10.0.0.1"));
    assertTrue(live.contains("10.0.0.2"));
    assertTrue(live.contains("10.0.0.3"));
  }

  @Test
  public void updateLiveNodesUpdatesLastActivityTime() throws Exception {
    ControllableHttpClient http = new ControllableHttpClient("[\"10.0.0.1\"]");
    AlternatorConfig config =
        AlternatorConfig.builder().withSeedNode(URI.create("http://10.0.0.1:8000")).build();
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, http);

    Field field = AlternatorLiveNodes.class.getDeclaredField("lastActivityTime");
    field.setAccessible(true);
    AtomicLong lastActivityTime = (AtomicLong) field.get(liveNodes);

    long before = System.currentTimeMillis();
    liveNodes.updateLiveNodes();
    long after = System.currentTimeMillis();

    long recorded = lastActivityTime.get();
    assertTrue("lastActivityTime must be >= time before the call", recorded >= before);
    assertTrue("lastActivityTime must be <= time after the call", recorded <= after);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void drainBufferIs1KBAndReusedOnSameThread() throws Exception {
    Field field = AlternatorLiveNodes.class.getDeclaredField("DRAIN_BUFFER");
    field.setAccessible(true);
    ThreadLocal<byte[]> drainBuffer = (ThreadLocal<byte[]>) field.get(null);

    byte[] first = drainBuffer.get();
    assertEquals("drain buffer must be 1024 bytes", 1024, first.length);
    assertSame(
        "drain buffer must be the same instance on repeated calls (no re-alloc)",
        first,
        drainBuffer.get());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void drainBufferIsUsedWhenBodyIsConsumedOnNon200Response() throws Exception {
    // A non-200 response triggers consumeAndClose(), which reads via DRAIN_BUFFER.
    // Verify that the buffer is still intact after the call (not zeroed or replaced).
    ControllableHttpClient http = new ControllableHttpClient("error body");
    http.setStatusCode(503);
    AlternatorConfig config =
        AlternatorConfig.builder().withSeedNode(URI.create("http://10.0.0.1:8000")).build();
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, http);

    Field field = AlternatorLiveNodes.class.getDeclaredField("DRAIN_BUFFER");
    field.setAccessible(true);
    ThreadLocal<byte[]> drainBuffer = (ThreadLocal<byte[]>) field.get(null);

    byte[] bufBefore = drainBuffer.get();
    // updateLiveNodes() gets a 503; consumeAndClose() drains the body via DRAIN_BUFFER.
    liveNodes.updateLiveNodes();
    byte[] bufAfter = drainBuffer.get();

    assertSame(
        "DRAIN_BUFFER must be the same instance after consumeAndClose()", bufBefore, bufAfter);
    assertEquals("buffer length must remain 1024", 1024, bufAfter.length);
  }

  @Test
  public void emptyNodeListResponseCausesScopeFallthrough() throws Exception {
    // A 200 response with body "[]" yields an empty node list. The refresh must not publish
    // it; getNodesForScope() returns an empty list and updateLiveNodes() falls through to the
    // next scope (or leaves the live list untouched if no scope remains).
    ControllableHttpClient http = new ControllableHttpClient("[]");
    AlternatorConfig config =
        AlternatorConfig.builder().withSeedNode(URI.create("http://10.0.0.1:8000")).build();
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, http);

    liveNodes.updateLiveNodes();

    // No scope returned nodes → live list falls back to initial seeds.
    assertFalse(
        "live list must not be empty after all-empty refresh", liveNodes.getLiveNodes().isEmpty());
  }

  @Test
  public void getNodesReturnsEmptyListWhen200ResponseHasNoBody() throws Exception {
    SdkHttpClient noBodyClient =
        new SdkHttpClient() {
          @Override
          public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
            return new ExecutableHttpRequest() {
              @Override
              public HttpExecuteResponse call() {
                return HttpExecuteResponse.builder()
                    .response(SdkHttpFullResponse.builder().statusCode(200).build())
                    // no responseBody() → Optional.empty()
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
            return "NoBodyClient";
          }
        };

    AlternatorConfig config =
        AlternatorConfig.builder().withSeedNode(URI.create("http://10.0.0.1:8000")).build();
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, noBodyClient);

    // Must not throw; falls through to seed-restore since no nodes found.
    liveNodes.updateLiveNodes();
    assertFalse(liveNodes.getLiveNodes().isEmpty());
  }

  @Test
  public void consumeAndCloseAbsorbsReadIOException() throws Exception {
    // Stream that throws on read() but abort() is a no-op — covers the IOException catch path.
    InputStream throwingRead =
        new InputStream() {
          @Override
          public int read() throws IOException {
            throw new IOException("simulated read failure");
          }

          @Override
          public int read(byte[] b, int off, int len) throws IOException {
            throw new IOException("simulated read failure");
          }
        };
    SdkHttpClient client = singleResponseClient(503, AbortableInputStream.create(throwingRead));
    AlternatorLiveNodes liveNodes =
        new AlternatorLiveNodes(
            AlternatorConfig.builder().withSeedNode(URI.create("http://10.0.0.1:8000")).build(),
            client);
    liveNodes.updateLiveNodes(); // must not throw
  }

  @Test
  public void consumeAndCloseAbsorbsAbortExceptionAfterReadFailure() throws Exception {
    // Both read() and abort() throw — covers the inner catch(Exception abortEx) path.
    InputStream throwingRead =
        new InputStream() {
          @Override
          public int read() throws IOException {
            throw new IOException("simulated read failure");
          }

          @Override
          public int read(byte[] b, int off, int len) throws IOException {
            throw new IOException("simulated read failure");
          }
        };
    Abortable throwingAbort =
        () -> {
          throw new RuntimeException("simulated abort failure");
        };
    SdkHttpClient client =
        singleResponseClient(503, AbortableInputStream.create(throwingRead, throwingAbort));
    AlternatorLiveNodes liveNodes =
        new AlternatorLiveNodes(
            AlternatorConfig.builder().withSeedNode(URI.create("http://10.0.0.1:8000")).build(),
            client);
    liveNodes.updateLiveNodes(); // must not throw
  }

  private static SdkHttpClient singleResponseClient(int statusCode, AbortableInputStream body) {
    return new SdkHttpClient() {
      @Override
      public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
        return new ExecutableHttpRequest() {
          @Override
          public HttpExecuteResponse call() {
            return HttpExecuteResponse.builder()
                .response(SdkHttpFullResponse.builder().statusCode(statusCode).build())
                .responseBody(body)
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
        return "SingleResponseClient";
      }
    };
  }

  // Direct parseLocalNodes branch tests — placed here because forkCount=2 in Surefire
  // isolates ParseLocalNodesTest to a separate JVM whose JaCoCo data does not merge into
  // the main exec file. Tests here share the fork that covers AlternatorLiveNodes.

  @Test
  public void parseLocalNodes_whitespaceOnly() throws Exception {
    AlternatorLiveNodes ln =
        new AlternatorLiveNodes(
            AlternatorConfig.builder().withSeedNode(URI.create("http://10.0.0.1:8000")).build());
    assertTrue(ln.parseLocalNodes("   \t\n").isEmpty());
  }

  @Test
  public void parseLocalNodes_unclosedString() throws Exception {
    // Truncated mid-string; inner while exits when i >= n.
    AlternatorLiveNodes ln =
        new AlternatorLiveNodes(
            AlternatorConfig.builder().withSeedNode(URI.create("http://10.0.0.1:8000")).build());
    List<URI> r = ln.parseLocalNodes("[\"host");
    assertEquals(1, r.size()); // "host" is a valid URI hostname
  }

  @Test
  public void parseLocalNodes_backslashAtEndOfInput() throws Exception {
    // ch=='\\' AND i+1 >= n: condition is false, backslash appended as literal char.
    AlternatorLiveNodes ln =
        new AlternatorLiveNodes(
            AlternatorConfig.builder().withSeedNode(URI.create("http://10.0.0.1:8000")).build());
    // "[\"10.0.0.\\" Java literal → ["10.0.0.\ raw: backslash is the last char.
    assertTrue(ln.parseLocalNodes("[\"10.0.0.\\").isEmpty());
  }

  @Test
  public void parseLocalNodes_leadingSlashHostTriggersPortMismatch() throws Exception {
    // host="/10.0.0.1" causes URI() to produce port -1; port check throws URISyntaxException.
    AlternatorLiveNodes ln =
        new AlternatorLiveNodes(
            AlternatorConfig.builder().withSeedNode(URI.create("http://10.0.0.1:8000")).build());
    List<URI> r = ln.parseLocalNodes("[\"/10.0.0.1\",\"10.0.0.2\"]");
    assertEquals(1, r.size());
    assertEquals("10.0.0.2", r.get(0).getHost());
  }

  @Test
  public void parseLocalNodes_null() throws Exception {
    AlternatorLiveNodes ln =
        new AlternatorLiveNodes(
            AlternatorConfig.builder().withSeedNode(URI.create("http://10.0.0.1:8000")).build());
    assertTrue(ln.parseLocalNodes(null).isEmpty());
  }

  @Test
  public void parseLocalNodes_leadingWhitespace() throws Exception {
    AlternatorLiveNodes ln =
        new AlternatorLiveNodes(
            AlternatorConfig.builder().withSeedNode(URI.create("http://10.0.0.1:8000")).build());
    List<URI> r = ln.parseLocalNodes("  [ \"10.0.0.1\" ]");
    assertEquals(1, r.size());
  }

  @Test
  public void parseLocalNodes_nonArray() throws Exception {
    AlternatorLiveNodes ln =
        new AlternatorLiveNodes(
            AlternatorConfig.builder().withSeedNode(URI.create("http://10.0.0.1:8000")).build());
    assertTrue(ln.parseLocalNodes("{\"host\":\"10.0.0.1\"}").isEmpty());
  }

  @Test
  public void parseLocalNodes_unexpectedChar() throws Exception {
    AlternatorLiveNodes ln =
        new AlternatorLiveNodes(
            AlternatorConfig.builder().withSeedNode(URI.create("http://10.0.0.1:8000")).build());
    assertTrue(ln.parseLocalNodes("[invalid]").isEmpty());
  }

  @Test
  public void parseLocalNodes_backslashEscape() throws Exception {
    AlternatorLiveNodes ln =
        new AlternatorLiveNodes(
            AlternatorConfig.builder().withSeedNode(URI.create("http://10.0.0.1:8000")).build());
    // \"10.0.0\.1\" — the \. escape resolves to '.' giving host 10.0.0.1
    List<URI> r = ln.parseLocalNodes("[\"10.0.0\\.1\"]");
    assertEquals(1, r.size());
    assertEquals("10.0.0.1", r.get(0).getHost());
  }

  @Test
  public void parseLocalNodes_emptyStringEntry() throws Exception {
    AlternatorLiveNodes ln =
        new AlternatorLiveNodes(
            AlternatorConfig.builder().withSeedNode(URI.create("http://10.0.0.1:8000")).build());
    List<URI> r = ln.parseLocalNodes("[\"\",\"10.0.0.1\"]");
    assertEquals(1, r.size());
  }

  @Test
  public void parseLocalNodes_invalidHostCaughtAndSkipped() throws Exception {
    AlternatorLiveNodes ln =
        new AlternatorLiveNodes(
            AlternatorConfig.builder().withSeedNode(URI.create("http://10.0.0.1:8000")).build());
    // "host with spaces" throws URISyntaxException in hostToURI; valid host follows
    List<URI> r = ln.parseLocalNodes("[\"host with spaces\",\"10.0.0.1\"]");
    assertEquals(1, r.size());
  }

  @Test
  public void updateLiveNodes_allNodesDownRestoresSeedList() throws Exception {
    ControllableHttpClient http = new ControllableHttpClient("[\"10.0.0.1\",\"10.0.0.2\"]");
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedHosts(Arrays.asList("10.0.0.1", "10.0.0.2"))
            .withScheme("http")
            .withPort(8000)
            .build();
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, http);

    http.markDown("10.0.0.1");
    http.markDown("10.0.0.2");

    try {
      liveNodes.updateLiveNodes();
    } catch (IOException ignored) {
    }

    // All dead → remaining.isEmpty() == true → seed list restored.
    assertEquals(
        "seed list must be restored when all nodes are down", 2, liveNodes.getLiveNodes().size());
  }

  @Test
  public void hostToURICallsToURLWhenSchemeNotYetValidated() throws Exception {
    AlternatorConfig config =
        AlternatorConfig.builder().withSeedNode(URI.create("http://10.0.0.1:8000")).build();
    AlternatorLiveNodes liveNodes =
        new AlternatorLiveNodes(config, new ControllableHttpClient("[\"10.0.0.2\"]"));

    // Reset the flag to exercise the !schemeAndPortValidated branch in hostToURI().
    Field f = AlternatorLiveNodes.class.getDeclaredField("schemeAndPortValidated");
    f.setAccessible(true);
    f.setBoolean(liveNodes, false);

    List<URI> result = liveNodes.parseLocalNodes("[\"10.0.0.2\"]");
    assertEquals("uri must be returned even when toURL() validation is re-run", 1, result.size());
    assertEquals("http", result.get(0).getScheme());
    assertEquals(8000, result.get(0).getPort());
  }
}
