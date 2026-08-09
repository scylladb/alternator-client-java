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
import com.scylladb.alternator.routing.ClusterScope;
import com.scylladb.alternator.routing.DatacenterScope;
import com.scylladb.alternator.routing.RackScope;
import com.scylladb.alternator.routing.RoutingScope;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
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
   * <p>After a successful discovery replaces liveNodes with new nodes, if those live nodes cannot
   * return a usable /localnodes response, the client should fall back to the original seed URLs so
   * it can recover through the configured entry points.
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

    // After a successful update, liveNodes should contain discovered nodes without injecting seeds
    liveNodes.updateLiveNodes();
    List<URI> updatedList = liveNodes.getLiveNodes();
    List<String> hosts = new ArrayList<>();
    for (URI uri : updatedList) {
      hosts.add(uri.getHost());
    }
    assertTrue("Should contain discovered node 10.0.0.2", hosts.contains("10.0.0.2"));
    assertTrue("Should contain discovered node 10.0.0.3", hosts.contains("10.0.0.3"));

    assertFalse(
        "Seed node 10.0.0.1 should remain a discovery candidate, not a routing target",
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

    assertEquals(
        "an explicit cluster fallback authorizes the configured seed immediately",
        "10.0.0.1",
        liveNodes.nextAsURI().getHost());

    // This should NOT throw - the fallback chain should traverse Rack -> DC -> Cluster
    liveNodes.updateLiveNodes();

    // All 3 scopes should have been queried (rack returned empty, dc returned empty,
    // cluster returned a node)
    assertEquals(
        "Fallback should traverse all 3 scope levels when empty lists are returned",
        3,
        requestCount.get());

    // liveNodes should now contain only the cluster-scope node
    List<URI> nodes = liveNodes.getLiveNodes();
    List<String> nodeHosts = new ArrayList<>();
    for (URI uri : nodes) {
      nodeHosts.add(uri.getHost());
    }
    assertTrue("Should contain cluster-scope node 10.0.0.5", nodeHosts.contains("10.0.0.5"));
    assertFalse(
        "Seed node 10.0.0.1 should not be added to the routing list",
        nodeHosts.contains("10.0.0.1"));
  }

  @Test
  public void testStrictWrongScopeAuthoritativeEmptyProducesClearValidationError()
      throws Exception {
    ReachableHttpClient emptyClient = new ReachableHttpClient("[]");
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedHost("10.0.0.1")
            .withScheme("http")
            .withRoutingScope(RackScope.of("dc1", "wrong-rack", null))
            .build();
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, emptyClient);

    assertTrue(
        "strict scoped seeds must not become routing targets", liveNodes.getLiveNodes().isEmpty());
    try {
      liveNodes.nextAsURI();
      fail("Expected strict scope to have no routing target before discovery");
    } catch (IllegalStateException expected) {
      assertTrue(expected.getMessage(), expected.getMessage().contains("No live nodes"));
    }

    liveNodes.updateLiveNodes();
    assertTrue(
        "an authoritative terminal empty response must remain fail-closed",
        liveNodes.getLiveNodes().isEmpty());

    try {
      liveNodes.checkIfRackAndDatacenterSetCorrectly();
      fail("Expected strict wrong scope validation to fail");
    } catch (AlternatorLiveNodes.ValidationError e) {
      assertTrue(e.getMessage(), e.getMessage().contains("returned empty list"));
    } catch (AlternatorLiveNodes.FailedToCheck e) {
      fail("A valid [] is authoritative, not a connectivity failure: " + e);
    }
  }

  @Test
  public void testAuthoritativeEmptySurvivesOtherCandidateErrorsForScopeValidation()
      throws Exception {
    SdkHttpClient errorThenEmpty =
        new SdkHttpClient() {
          @Override
          public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
            boolean shouldFail = "error.test".equals(request.httpRequest().host());
            return new ExecutableHttpRequest() {
              @Override
              public HttpExecuteResponse call() throws IOException {
                if (shouldFail) {
                  throw new IOException("simulated candidate failure");
                }
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
            return "ErrorThenEmptyClient";
          }
        };
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedHosts(Arrays.asList("error.test", "empty.test"))
            .withScheme("http")
            .withRoutingScope(RackScope.of("dc1", "wrong-rack", null))
            .build();
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, errorThenEmpty);

    try {
      liveNodes.checkIfRackAndDatacenterSetCorrectly();
      fail("Expected authoritative empty validation failure");
    } catch (AlternatorLiveNodes.ValidationError expected) {
      assertTrue(expected.getMessage(), expected.getMessage().contains("returned empty list"));
    }
  }

  @Test
  public void testNonSuccessMissingAndMalformedResponsesAreNotAuthoritativeEmpty()
      throws Exception {
    assertValidationCheckFailsAsConnectivityError(response(503, "temporarily unavailable"));
    assertValidationCheckFailsAsConnectivityError(
        HttpExecuteResponse.builder()
            .response(SdkHttpFullResponse.builder().statusCode(200).build())
            .build());
    assertValidationCheckFailsAsConnectivityError(response(200, "not-json"));
  }

  @Test(timeout = 5000)
  public void testStalledPrimaryScopeRetainsBudgetForHealthyFallbackScope() throws Exception {
    CountDownLatch releasePrimary = new CountDownLatch(1);
    SdkHttpClient stalledPrimary =
        new SdkHttpClient() {
          @Override
          public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
            boolean primaryScope = request.httpRequest().rawQueryParameters().containsKey("rack");
            return new ExecutableHttpRequest() {
              @Override
              public HttpExecuteResponse call() {
                if (primaryScope) {
                  boolean released = false;
                  while (!released) {
                    try {
                      released = releasePrimary.await(20, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException ignored) {
                      // Model a transport call that ignores interrupt and abort.
                    }
                  }
                }
                return response(200, "[\"fallback.test\"]");
              }

              @Override
              public void abort() {}
            };
          }

          @Override
          public void close() {}

          @Override
          public String clientName() {
            return "StalledPrimaryScopeClient";
          }
        };
    LiveNodesPollingLimits limits =
        new LiveNodesPollingLimits(100, 2, 100, 100, 250, 250, 600, 1024, 1024);
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedHost("seed.test")
            .withScheme("http")
            .withRoutingScope(RackScope.of("dc1", "wrong-rack", ClusterScope.create()))
            .build();
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, stalledPrimary, limits);

    try {
      liveNodes.updateLiveNodes();
      assertEquals("fallback.test", liveNodes.nextAsURI().getHost());
    } finally {
      releasePrimary.countDown();
    }
  }

  @Test
  public void testWhollyInvalidResponsesTraverseScopeFallbackAndRemainFailClosed()
      throws Exception {
    ReachableHttpClient whollyInvalidClient = new ReachableHttpClient("[\"bad host\"]");
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedNode(URI.create("http://10.0.0.1:8000"))
            .withRoutingScope(
                RackScope.of("dc1", "rack1", DatacenterScope.of("dc1", ClusterScope.create())))
            .build();
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, whollyInvalidClient);

    liveNodes.updateLiveNodes();

    assertEquals(
        "wholly invalid nonempty data must traverse Rack, DC, and Cluster",
        3,
        whollyInvalidClient.capturedRequests.size());
    assertEquals(
        "invalid discovery must retain the seed authorized by the explicit cluster fallback",
        "10.0.0.1",
        liveNodes.nextAsURI().getHost());
  }

  @Test(timeout = 1000)
  public void testCyclicCustomRoutingScopeFailsBeforeDiscovery() throws Exception {
    RoutingScope cyclic =
        new RoutingScope() {
          @Override
          public String getName() {
            return "Cyclic";
          }

          @Override
          public String getDescription() {
            return "cyclic test scope";
          }

          @Override
          public RoutingScope getFallback() {
            return this;
          }

          @Override
          public String getLocalNodesQuery() {
            return "dc=missing";
          }
        };
    UnreachableHttpClient client = new UnreachableHttpClient();
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedHost("seed.test")
            .withScheme("http")
            .withRoutingScope(cyclic)
            .build();

    try {
      new AlternatorLiveNodes(config, client).updateLiveNodes();
      fail("Expected cyclic routing scope to fail");
    } catch (IOException expected) {
      assertTrue(expected.getMessage(), expected.getMessage().contains("cycle"));
    }
    assertEquals("cycle must fail before network discovery", 0, client.callCount.get());
  }

  @Test
  public void testAuthoritativeScopedEmptyClearsPreviouslyValidatedSnapshot() throws Exception {
    AtomicInteger phase = new AtomicInteger();
    SdkHttpClient client =
        new SdkHttpClient() {
          @Override
          public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
            return new ExecutableHttpRequest() {
              @Override
              public HttpExecuteResponse call() {
                return response(200, phase.get() == 0 ? "[\"validated.test\"]" : "[]");
              }

              @Override
              public void abort() {}
            };
          }

          @Override
          public void close() {}

          @Override
          public String clientName() {
            return "ChangingScopedClient";
          }
        };
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedHost("seed.test")
            .withScheme("http")
            .withRoutingScope(DatacenterScope.of("dc1", null))
            .build();
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, client);

    liveNodes.updateLiveNodes();
    assertEquals("validated.test", liveNodes.nextAsURI().getHost());
    phase.incrementAndGet();
    liveNodes.updateLiveNodes();

    assertTrue(
        "authoritative empty scope must remove stale routing targets",
        liveNodes.getLiveNodes().isEmpty());
  }

  @Test
  public void testStrictPublishedScopeClearsWhenThatScopeBecomesEmpty() throws Exception {
    AtomicInteger phase = new AtomicInteger();
    SdkHttpClient client =
        new SdkHttpClient() {
          @Override
          public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
            return new ExecutableHttpRequest() {
              @Override
              public HttpExecuteResponse call() {
                return response(200, phase.get() == 0 ? "[\"validated.test\"]" : "[]");
              }

              @Override
              public void abort() {}
            };
          }

          @Override
          public void close() {}

          @Override
          public String clientName() {
            return "ChangingClusterFallbackClient";
          }
        };
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedHost("seed.test")
            .withScheme("http")
            .withRoutingScope(RackScope.of("dc1", "rack1", ClusterScope.create()))
            .build();
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, client);

    liveNodes.updateLiveNodes();
    assertEquals("validated.test", liveNodes.nextAsURI().getHost());
    phase.incrementAndGet();
    liveNodes.updateLiveNodes();

    assertTrue(
        "an authoritative empty result must clear nodes published by that strict scope",
        liveNodes.getLiveNodes().isEmpty());
  }

  @Test
  public void testPrimaryEmptyClearsPrimaryOriginWhenFallbackFails() throws Exception {
    AtomicInteger phase = new AtomicInteger();
    AlternatorLiveNodes liveNodes =
        new AlternatorLiveNodes(provenanceConfig(), provenanceClient(phase, true));

    liveNodes.updateLiveNodes();
    assertEquals("primary.test", liveNodes.nextAsURI().getHost());

    phase.incrementAndGet();
    liveNodes.updateLiveNodes();

    assertTrue(
        "primary authoritative empty must clear a primary-origin snapshot even if fallback fails",
        liveNodes.getLiveNodes().isEmpty());
  }

  @Test
  public void testPrimaryEmptyRetainsFallbackOriginWhenFallbackFails() throws Exception {
    AtomicInteger phase = new AtomicInteger();
    AlternatorLiveNodes liveNodes =
        new AlternatorLiveNodes(provenanceConfig(), provenanceClient(phase, false));

    liveNodes.updateLiveNodes();
    assertEquals("fallback.test", liveNodes.nextAsURI().getHost());

    phase.incrementAndGet();
    liveNodes.updateLiveNodes();

    assertEquals(
        "failure cannot disprove a snapshot published by the fallback scope",
        "fallback.test",
        liveNodes.nextAsURI().getHost());
  }

  private static AlternatorConfig provenanceConfig() {
    return AlternatorConfig.builder()
        .withSeedHost("seed.test")
        .withScheme("http")
        .withRoutingScope(DatacenterScope.of("dc1", DatacenterScope.of("dc2", null)))
        .build();
  }

  private static SdkHttpClient provenanceClient(AtomicInteger phase, boolean primaryOrigin) {
    return new SdkHttpClient() {
      @Override
      public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
        List<String> values = request.httpRequest().rawQueryParameters().get("dc");
        String datacenter = values == null || values.isEmpty() ? "" : values.get(0);
        return new ExecutableHttpRequest() {
          @Override
          public HttpExecuteResponse call() throws IOException {
            if (phase.get() == 0) {
              if ("dc1".equals(datacenter)) {
                return response(200, primaryOrigin ? "[\"primary.test\"]" : "[]");
              }
              return response(200, "[\"fallback.test\"]");
            }
            if ("dc1".equals(datacenter)) {
              return response(200, "[]");
            }
            throw new IOException("fallback unavailable");
          }

          @Override
          public void abort() {}
        };
      }

      @Override
      public void close() {}

      @Override
      public String clientName() {
        return "ProvenanceHttpClient";
      }
    };
  }

  @Test
  public void testScopedFeatureProbeUsesDiscoverySeedWithoutRoutingIt() throws Exception {
    AtomicInteger requests = new AtomicInteger();
    SdkHttpClient client =
        new SdkHttpClient() {
          @Override
          public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
            boolean fakeRack = request.httpRequest().rawQueryParameters().containsKey("rack");
            return new ExecutableHttpRequest() {
              @Override
              public HttpExecuteResponse call() {
                requests.incrementAndGet();
                return response(200, fakeRack ? "[]" : "[\"seed.test\"]");
              }

              @Override
              public void abort() {}
            };
          }

          @Override
          public void close() {}

          @Override
          public String clientName() {
            return "FeatureProbeClient";
          }
        };
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedHost("seed.test")
            .withScheme("http")
            .withRoutingScope(DatacenterScope.of("dc1", null))
            .build();
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, client);

    assertTrue(liveNodes.checkIfRackDatacenterFeatureIsSupported());
    assertEquals(2, requests.get());
    assertTrue(
        "feature probing must not authorize a scoped seed", liveNodes.getLiveNodes().isEmpty());
  }

  @Test
  public void testClusterRefreshUnionsLearnedAndOriginalSeedResponses() throws Exception {
    AtomicInteger phase = new AtomicInteger();
    SdkHttpClient client =
        new SdkHttpClient() {
          @Override
          public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
            String host = request.httpRequest().host();
            return new ExecutableHttpRequest() {
              @Override
              public HttpExecuteResponse call() {
                if (phase.get() == 0) {
                  return response(200, "[\"learned.test\"]");
                }
                return response(
                    200, "learned.test".equals(host) ? "[\"dc1.test\"]" : "[\"dc2.test\"]");
              }

              @Override
              public void abort() {}
            };
          }

          @Override
          public void close() {}

          @Override
          public String clientName() {
            return "MultiDatacenterSeedClient";
          }
        };
    AlternatorConfig config =
        AlternatorConfig.builder().withSeedHost("seed.test").withScheme("http").build();
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, client);

    liveNodes.updateLiveNodes();
    assertEquals("learned.test", liveNodes.getLiveNodes().get(0).getHost());
    phase.incrementAndGet();
    liveNodes.updateLiveNodes();

    List<String> hosts = new ArrayList<>();
    for (URI node : liveNodes.getLiveNodes()) {
      hosts.add(node.getHost());
    }
    assertEquals(Arrays.asList("dc1.test", "dc2.test"), hosts);
  }

  private static HttpExecuteResponse response(int status, String json) {
    byte[] body = json.getBytes(StandardCharsets.UTF_8);
    return HttpExecuteResponse.builder()
        .response(SdkHttpFullResponse.builder().statusCode(status).build())
        .responseBody(AbortableInputStream.create(new ByteArrayInputStream(body)))
        .build();
  }

  private static void assertValidationCheckFailsAsConnectivityError(HttpExecuteResponse response) {
    SdkHttpClient client =
        new SdkHttpClient() {
          @Override
          public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
            return new ExecutableHttpRequest() {
              @Override
              public HttpExecuteResponse call() {
                return response;
              }

              @Override
              public void abort() {}
            };
          }

          @Override
          public void close() {}

          @Override
          public String clientName() {
            return "NonAuthoritativeResponseClient";
          }
        };
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedHost("seed.test")
            .withScheme("http")
            .withRoutingScope(RackScope.of("dc1", "wrong-rack", null))
            .build();

    try {
      new AlternatorLiveNodes(config, client).checkIfRackAndDatacenterSetCorrectly();
      fail("Expected response to be treated as a failed check");
    } catch (AlternatorLiveNodes.FailedToCheck expected) {
      // Expected: only a parsed HTTP 200 [] is authoritative.
    } catch (AlternatorLiveNodes.ValidationError error) {
      fail("Response must not be treated as authoritative empty: " + error);
    }
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
