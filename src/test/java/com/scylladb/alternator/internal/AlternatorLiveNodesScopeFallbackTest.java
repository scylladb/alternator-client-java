package com.scylladb.alternator.internal;

import static org.junit.Assert.*;

import com.scylladb.alternator.AlternatorConfig;
import com.scylladb.alternator.routing.ClusterScope;
import com.scylladb.alternator.routing.DatacenterScope;
import com.scylladb.alternator.routing.RackScope;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpFullResponse;
import software.amazon.awssdk.http.SdkHttpRequest;

/**
 * Tests for GitHub issue #83: Fallback behavior when all nodes in a scope become unreachable.
 *
 * <p>When all nodes in the client's current scope become unreachable (IOException), the scope
 * fallback chain should still be traversed. Currently, the IOException propagates out of
 * updateLiveNodes() and the fallback never happens. These tests assert the correct/desired behavior
 * and are expected to FAIL until the fix is implemented.
 *
 * @see <a href="https://github.com/scylladb/alternator-client-java/issues/83">Issue #83</a>
 */
public class AlternatorLiveNodesScopeFallbackTest {

  /**
   * Mock SdkHttpClient that throws IOException on call(), simulating an unreachable node. Tracks
   * the number of attempts made.
   */
  private static class UnreachableHttpClient implements SdkHttpClient {
    final AtomicInteger callCount = new AtomicInteger(0);
    final List<SdkHttpRequest> capturedRequests = new CopyOnWriteArrayList<>();

    @Override
    public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
      capturedRequests.add(request.httpRequest());
      return new ExecutableHttpRequest() {
        @Override
        public HttpExecuteResponse call() throws IOException {
          callCount.incrementAndGet();
          throw new IOException("Connection refused (simulated unreachable node)");
        }

        @Override
        public void abort() {}
      };
    }

    @Override
    public void close() {}

    @Override
    public String clientName() {
      return "UnreachableHttpClient";
    }
  }

  /**
   * Mock SdkHttpClient that returns a valid response with the given node list. Tracks captured
   * requests.
   */
  private static class ReachableHttpClient implements SdkHttpClient {
    final List<SdkHttpRequest> capturedRequests = new CopyOnWriteArrayList<>();
    private final String responseJson;

    ReachableHttpClient(String responseJson) {
      this.responseJson = responseJson;
    }

    @Override
    public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
      capturedRequests.add(request.httpRequest());
      return new ExecutableHttpRequest() {
        @Override
        public HttpExecuteResponse call() {
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
      return "ReachableHttpClient";
    }
  }

  /**
   * Verifies that when all nodes are unreachable, updateLiveNodes() should traverse the entire
   * scope fallback chain (Rack -&gt; DC -&gt; Cluster) before giving up.
   *
   * <p>With a 3-level fallback chain, all 3 scope levels should be attempted even when nodes throw
   * IOException. The method should not throw IOException until all scopes have been exhausted.
   *
   * @throws Exception if an unexpected error occurs
   */
  @Test
  public void testScopeFallbackTraversedOnIOException() throws Exception {
    UnreachableHttpClient unreachableClient = new UnreachableHttpClient();

    // Configure with RackScope -> DatacenterScope -> ClusterScope fallback chain
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedNode(URI.create("http://10.0.0.1:8000"))
            .withRoutingScope(
                RackScope.of("dc1", "rack1", DatacenterScope.of("dc1", ClusterScope.create())))
            .build();

    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, unreachableClient);

    // updateLiveNodes() should attempt all 3 scope levels before giving up.
    // It may throw IOException after exhausting all scopes, but the key assertion is
    // that all 3 scopes were tried.
    try {
      liveNodes.updateLiveNodes();
    } catch (IOException e) {
      // Acceptable to throw after exhausting all scopes
    }

    // All 3 scope levels should have been attempted
    assertTrue(
        "All 3 scope levels (Rack, DC, Cluster) should be attempted on IOException, "
            + "but only "
            + unreachableClient.callCount.get()
            + " attempt(s) were made",
        unreachableClient.callCount.get() >= 3);
  }

  /**
   * Verifies that when all discovered nodes become unreachable, the client should fall back to the
   * original seed nodes for re-discovery.
   *
   * <p>After a successful discovery replaces liveNodes with new nodes, if all those new nodes
   * become unreachable, the client should re-inject the seed URLs into its candidate list so it can
   * recover through the original entry points.
   *
   * @throws Exception if an unexpected error occurs
   */
  @Test
  public void testSeedNodesReusedWhenDiscoveredNodesUnreachable() throws Exception {
    // Phase 1: Successful discovery returns nodes B,C (not the seed A)
    ReachableHttpClient reachableClient = new ReachableHttpClient("[\"10.0.0.2\",\"10.0.0.3\"]");

    AlternatorConfig config =
        AlternatorConfig.builder().withSeedNode(URI.create("http://10.0.0.1:8000")).build();

    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, reachableClient);

    // Initial liveNodes should contain seed (10.0.0.1)
    List<URI> initialList = liveNodes.getLiveNodes();
    assertEquals(1, initialList.size());
    assertEquals("10.0.0.1", initialList.get(0).getHost());

    // After a successful update, liveNodes should contain discovered nodes AND the seed
    liveNodes.updateLiveNodes();
    List<URI> updatedList = liveNodes.getLiveNodes();
    List<String> hosts = new ArrayList<>();
    for (URI uri : updatedList) {
      hosts.add(uri.getHost());
    }
    assertTrue("Should contain discovered node 10.0.0.2", hosts.contains("10.0.0.2"));
    assertTrue("Should contain discovered node 10.0.0.3", hosts.contains("10.0.0.3"));

    // The seed node should still be present in liveNodes as a fallback,
    // so that if 10.0.0.2 and 10.0.0.3 both die, the client can re-discover via 10.0.0.1.
    assertTrue(
        "Seed node 10.0.0.1 should be retained as a fallback after discovery, "
            + "but liveNodes only contains: "
            + hosts,
        hosts.contains("10.0.0.1"));
  }

  /**
   * Verifies that when getNodes() returns an empty list (node is reachable but scope has no nodes),
   * the scope fallback works correctly. This is the "happy path" for fallback and should continue
   * to work after the fix.
   *
   * @throws Exception if an unexpected error occurs
   */
  @Test
  public void testFallbackWorksWhenScopeReturnsEmptyList() throws Exception {
    // This client will return an empty list for rack/dc-scoped queries and a non-empty list
    // for the unscoped (cluster) query.
    AtomicInteger requestCount = new AtomicInteger(0);
    SdkHttpClient scopeAwareClient =
        new SdkHttpClient() {
          @Override
          public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
            SdkHttpRequest httpReq = request.httpRequest();
            return new ExecutableHttpRequest() {
              @Override
              public HttpExecuteResponse call() {
                requestCount.incrementAndGet();
                String responseJson;
                // If the query includes rack or dc parameters, return empty
                // Only the cluster-wide query (no dc/rack) returns nodes
                boolean hasScopeFilter =
                    httpReq.rawQueryParameters().containsKey("rack")
                        || httpReq.rawQueryParameters().containsKey("dc");
                if (hasScopeFilter) {
                  responseJson = "[]";
                } else {
                  responseJson = "[\"10.0.0.5\"]";
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
            return "ScopeAwareHttpClient";
          }
        };

    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedNode(URI.create("http://10.0.0.1:8000"))
            .withRoutingScope(
                RackScope.of("dc1", "rack1", DatacenterScope.of("dc1", ClusterScope.create())))
            .build();

    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, scopeAwareClient);

    // This should NOT throw - the fallback chain should traverse Rack -> DC -> Cluster
    liveNodes.updateLiveNodes();

    // All 3 scopes should have been queried (rack returned empty, dc returned empty,
    // cluster returned a node)
    assertEquals(
        "Fallback should traverse all 3 scope levels when empty lists are returned",
        3,
        requestCount.get());

    // liveNodes should now contain the cluster-scope node and the seed node
    List<URI> nodes = liveNodes.getLiveNodes();
    List<String> nodeHosts = new ArrayList<>();
    for (URI uri : nodes) {
      nodeHosts.add(uri.getHost());
    }
    assertTrue("Should contain cluster-scope node 10.0.0.5", nodeHosts.contains("10.0.0.5"));
    assertTrue("Should contain seed node 10.0.0.1 as fallback", nodeHosts.contains("10.0.0.1"));
  }

  /**
   * Verifies that repeated updateLiveNodes() calls with all-unreachable discovered nodes should
   * eventually try the seed nodes, allowing recovery if the seeds are still alive.
   *
   * @throws Exception if an unexpected error occurs
   */
  @Test
  public void testRepeatedUpdatesWithUnreachableNodesEventuallyTrySeedNodes() throws Exception {
    UnreachableHttpClient unreachableClient = new UnreachableHttpClient();

    // Seeds are 10.0.0.2, 10.0.0.3, 10.0.0.4 (simulating post-discovery state where
    // seeds == discovered nodes, and all are now unreachable)
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedHosts(Arrays.asList("10.0.0.2", "10.0.0.3", "10.0.0.4"))
            .withScheme("http")
            .withPort(8000)
            .build();

    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, unreachableClient);

    // Simulate multiple update cycles
    for (int i = 0; i < 9; i++) {
      try {
        liveNodes.updateLiveNodes();
      } catch (IOException e) {
        // May throw, that's fine for this test
      }
    }

    // The liveNodes list should remain unchanged (the unreachable nodes should not be removed
    // without replacement)
    List<URI> currentNodes = liveNodes.getLiveNodes();
    assertEquals(
        "liveNodes list should remain unchanged after failed updates", 3, currentNodes.size());

    // Verify the requests cycled through all 3 nodes via round-robin
    List<String> contactedHosts = new ArrayList<>();
    for (SdkHttpRequest req : unreachableClient.capturedRequests) {
      contactedHosts.add(req.host());
    }
    assertTrue("Should have contacted node 10.0.0.2", contactedHosts.contains("10.0.0.2"));
    assertTrue("Should have contacted node 10.0.0.3", contactedHosts.contains("10.0.0.3"));
    assertTrue("Should have contacted node 10.0.0.4", contactedHosts.contains("10.0.0.4"));
  }

  /**
   * Verifies that IOException fallback behavior should be consistent with empty-list fallback
   * behavior. Both scenarios should traverse the full scope chain.
   *
   * <p>With the same scope configuration (Rack -&gt; DC -&gt; Cluster), the number of scope levels
   * attempted should be the same regardless of whether the failure is an empty list or an
   * IOException.
   *
   * @throws Exception if an unexpected error occurs
   */
  @Test
  public void testIOExceptionFallbackConsistentWithEmptyListFallback() throws Exception {
    // Scope chain: Rack -> DC -> Cluster (3 levels)
    AlternatorConfig.Builder configBuilder =
        AlternatorConfig.builder()
            .withSeedNode(URI.create("http://10.0.0.1:8000"))
            .withRoutingScope(
                RackScope.of("dc1", "rack1", DatacenterScope.of("dc1", ClusterScope.create())));

    // --- Case 1: Empty-list responses ---
    AtomicInteger emptyListRequestCount = new AtomicInteger(0);
    SdkHttpClient emptyListClient =
        new SdkHttpClient() {
          @Override
          public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
            return new ExecutableHttpRequest() {
              @Override
              public HttpExecuteResponse call() {
                emptyListRequestCount.incrementAndGet();
                byte[] body = "[]".getBytes(StandardCharsets.UTF_8);
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
            return "EmptyListClient";
          }
        };

    AlternatorLiveNodes liveNodesEmptyCase =
        new AlternatorLiveNodes(configBuilder.build(), emptyListClient);

    liveNodesEmptyCase.updateLiveNodes();
    int emptyListScopeAttempts = emptyListRequestCount.get();

    // --- Case 2: IOException responses ---
    UnreachableHttpClient ioExceptionClient = new UnreachableHttpClient();

    AlternatorLiveNodes liveNodesIOExceptionCase =
        new AlternatorLiveNodes(configBuilder.build(), ioExceptionClient);

    try {
      liveNodesIOExceptionCase.updateLiveNodes();
    } catch (IOException e) {
      // May throw after exhausting all scopes
    }
    int ioExceptionScopeAttempts = ioExceptionClient.callCount.get();

    // Both cases should traverse the same number of scope levels
    assertEquals(
        "IOException fallback should traverse the same number of scope levels as empty-list "
            + "fallback. Empty-list traversed "
            + emptyListScopeAttempts
            + " levels, but "
            + "IOException only traversed "
            + ioExceptionScopeAttempts
            + " levels",
        emptyListScopeAttempts,
        ioExceptionScopeAttempts);
  }
}
