package com.scylladb.alternator.internal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.scylladb.alternator.AlternatorConfig;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.Test;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpFullResponse;
import software.amazon.awssdk.http.SdkHttpRequest;

/**
 * Regression tests for the dead-seed-stays-in-rotation bug reproduced from a Scylla rolling-
 * upgrade failure: the YCSB DynamoDB binding ran with native load balancing, the seed node went
 * down during the upgrade, and 62,624 UPDATE operations failed with connection-refused because
 * {@link AlternatorLiveNodes#updateLiveNodes()} kept re-injecting the dead seed into the live list
 * on every refresh, so {@code nextAsURI()} kept round-robining onto it.
 *
 * <p>Expected behavior: once every configured seed has failed to answer /localnodes in a refresh
 * cycle (the whole cycle fails, not just one poll), a dead seed is pruned from the live list rather
 * than being blindly merged back in; if that empties the list, the original seed list is restored
 * as a last-resort recovery candidate so a future refresh has something to retry. A seed that fails
 * while at least one other seed/scope still succeeds this cycle is deliberately left in rotation —
 * see {@code
 * AlternatorLiveNodesClusterDiscoveryTest#testClusterScopeKeepsSuccessfulDiscoveryWhenAnotherSeedFails}.
 */
public class AlternatorLiveNodesDeadSeedTest {

  /**
   * SdkHttpClient that lets the test mark hosts as "down". A request to a down host throws
   * IOException (mirrors connection refused/reset); requests to live hosts return the configured
   * /localnodes response body.
   */
  private static final class SelectiveHttpClient implements SdkHttpClient {
    private final Set<String> downHosts = ConcurrentHashMap.newKeySet();
    private final String responseJson;
    final List<String> contactedHosts = new CopyOnWriteArrayList<>();

    SelectiveHttpClient(String responseJson) {
      this.responseJson = responseJson;
    }

    void markDown(String host) {
      downHosts.add(host);
    }

    @Override
    public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
      SdkHttpRequest req = request.httpRequest();
      String host = req.host();
      contactedHosts.add(host);
      return new ExecutableHttpRequest() {
        @Override
        public HttpExecuteResponse call() throws IOException {
          if (downHosts.contains(host)) {
            throw new IOException("connection refused (simulated dead host " + host + ")");
          }
          byte[] body = responseJson.getBytes(StandardCharsets.UTF_8);
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
      return "SelectiveHttpClient";
    }
  }

  /**
   * The scenario from the production failure: three Alternator nodes, refresh discovers all three,
   * then one of them — the original seed — dies. The next refresh must drop the dead seed from the
   * live list so that subsequent {@code nextAsURI()} calls stop returning it.
   */
  @Test
  public void deadSeedIsEvictedAfterRefreshDiscoversHealthyPeers() throws Exception {
    // /localnodes from any healthy node reports the full cluster initially.
    SelectiveHttpClient http = new SelectiveHttpClient("[\"10.0.0.1\",\"10.0.0.2\",\"10.0.0.3\"]");

    AlternatorConfig config =
        AlternatorConfig.builder().withSeedNode(URI.create("http://10.0.0.1:8000")).build();
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, http);

    // First refresh: seed answers, full cluster known.
    liveNodes.updateLiveNodes();
    Set<String> known = hosts(liveNodes.getLiveNodes());
    assertTrue("seed should be live", known.contains("10.0.0.1"));
    assertTrue("peer .2 should be live", known.contains("10.0.0.2"));
    assertTrue("peer .3 should be live", known.contains("10.0.0.3"));

    // The seed goes down. Real Scylla peers stop advertising it via /localnodes, so the
    // healthy peers' response shrinks to the surviving nodes.
    http.markDown("10.0.0.1");
    http = new SelectiveHttpClient("[\"10.0.0.2\",\"10.0.0.3\"]");
    http.markDown("10.0.0.1");
    liveNodes = rebuildWithSameLiveNodes(liveNodes, config, http);

    // Drive several refresh cycles to give the round-robin polling a chance to land on a
    // healthy peer no matter which index it starts from.
    for (int i = 0; i < 5; i++) {
      try {
        liveNodes.updateLiveNodes();
      } catch (IOException ignored) {
        // tolerate: a single refresh that picks only the dead seed is allowed to fail,
        // the loop will keep trying.
      }
    }

    Set<String> after = hosts(liveNodes.getLiveNodes());
    assertFalse(
        "dead seed must not be in the live list after refresh discovers healthy peers — "
            + "this is the bug that caused the YCSB rolling-upgrade failure. liveNodes="
            + after,
        after.contains("10.0.0.1"));
    assertTrue("healthy peer .2 must remain", after.contains("10.0.0.2"));
    assertTrue("healthy peer .3 must remain", after.contains("10.0.0.3"));
  }

  /**
   * Mirrors the existing {@code testRepeatedUpdatesWithUnreachableNodesEventuallyTrySeedNodes}
   * invariant: when every known node is unreachable, the seed list survives as a last-resort
   * recovery candidate (otherwise the next refresh has nothing to try).
   */
  @Test
  public void seedListRestoredWhenEverythingIsDown() throws Exception {
    SelectiveHttpClient http = new SelectiveHttpClient("[\"10.0.0.1\",\"10.0.0.2\",\"10.0.0.3\"]");
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedHosts(Arrays.asList("10.0.0.1", "10.0.0.2", "10.0.0.3"))
            .withScheme("http")
            .withPort(8000)
            .build();
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, http);

    http.markDown("10.0.0.1");
    http.markDown("10.0.0.2");
    http.markDown("10.0.0.3");

    try {
      liveNodes.updateLiveNodes();
    } catch (IOException ignored) {
      // expected — every host fails
    }

    assertEquals(
        "all-down case must keep the seed list intact for the next refresh to retry",
        3,
        liveNodes.getLiveNodes().size());
  }

  private static Set<String> hosts(List<URI> uris) {
    Set<String> out = new HashSet<>();
    for (URI u : uris) {
      out.add(u.getHost());
    }
    return out;
  }

  // Helper: AlternatorLiveNodes does not expose a way to swap the polling client. To
  // simulate the cluster state after the seed died (peers stop advertising it), we rebuild
  // the instance with a new mock client whose /localnodes response excludes the dead seed,
  // then seed the new instance's live list from the old one so we keep the discovered state.
  private static AlternatorLiveNodes rebuildWithSameLiveNodes(
      AlternatorLiveNodes previous, AlternatorConfig config, SdkHttpClient newClient)
      throws Exception {
    AlternatorLiveNodes next = new AlternatorLiveNodes(config, newClient);
    // Use the previous discovered list (which still contains the seed) so the new instance
    // starts in the same state the running client would be in at the moment the seed dies.
    java.lang.reflect.Field f = AlternatorLiveNodes.class.getDeclaredField("liveNodes");
    f.setAccessible(true);
    @SuppressWarnings("unchecked")
    java.util.concurrent.atomic.AtomicReference<List<URI>> ref =
        (java.util.concurrent.atomic.AtomicReference<List<URI>>) f.get(next);
    ref.set(new ArrayList<>(previous.getLiveNodes()));
    return next;
  }
}
