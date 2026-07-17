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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.Test;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpFullResponse;
import software.amazon.awssdk.http.SdkHttpRequest;

/** Unit tests for cluster-wide discovery across user-provided seed nodes. */
public class AlternatorLiveNodesClusterDiscoveryTest {

  private static class DiscoveryHttpClient implements SdkHttpClient {
    final List<SdkHttpRequest> capturedRequests = new CopyOnWriteArrayList<>();
    private final Map<String, String> responsesByHost;
    private final Set<String> failingHosts;

    DiscoveryHttpClient(Map<String, String> responsesByHost) {
      this(responsesByHost, Collections.<String>emptySet());
    }

    DiscoveryHttpClient(Map<String, String> responsesByHost, Set<String> failingHosts) {
      this.responsesByHost = responsesByHost;
      this.failingHosts = failingHosts;
    }

    @Override
    public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
      SdkHttpRequest httpRequest = request.httpRequest();
      capturedRequests.add(httpRequest);
      return new ExecutableHttpRequest() {
        @Override
        public HttpExecuteResponse call() throws IOException {
          String host = httpRequest.host();
          if (failingHosts.contains(host)) {
            throw new IOException("simulated discovery failure for " + host);
          }
          if (!responsesByHost.containsKey(host)) {
            throw new AssertionError("Unexpected discovery host: " + host);
          }
          byte[] body = responsesByHost.get(host).getBytes(StandardCharsets.UTF_8);
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
      return "DiscoveryHttpClient";
    }
  }

  @Test
  public void testClusterScopeMergesConfiguredSeedNodes() throws Exception {
    Map<String, String> responses = new HashMap<>();
    responses.put("dc1-node1.example.com", "[\"dc1-node1.example.com\",\"dc1-node2.example.com\"]");
    responses.put("dc2-node1.example.com", "[\"dc2-node1.example.com\",\"dc2-node2.example.com\"]");
    DiscoveryHttpClient httpClient = new DiscoveryHttpClient(responses);

    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedHosts(Arrays.asList("dc1-node1.example.com", "dc2-node1.example.com"))
            .withScheme("http")
            .withPort(8000)
            .withRoutingScope(ClusterScope.create())
            .build();

    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, httpClient);

    liveNodes.updateLiveNodes();

    assertEquals(
        new LinkedHashSet<>(
            Arrays.asList(
                "dc1-node1.example.com",
                "dc1-node2.example.com",
                "dc2-node1.example.com",
                "dc2-node2.example.com")),
        hostSet(liveNodes.getLiveNodes()));
    assertEquals(
        new HashSet<>(Arrays.asList("dc1-node1.example.com", "dc2-node1.example.com")),
        capturedHostSet(httpClient.capturedRequests));
    for (SdkHttpRequest request : httpClient.capturedRequests) {
      assertEquals("/localnodes", request.encodedPath());
      assertTrue(request.rawQueryParameters().isEmpty());
    }
  }

  @Test
  public void testClusterScopeKeepsSuccessfulDiscoveryWhenAnotherSeedFails() throws Exception {
    Map<String, String> responses = new HashMap<>();
    responses.put("dc1-node1.example.com", "[\"dc1-node1.example.com\"]");
    DiscoveryHttpClient httpClient =
        new DiscoveryHttpClient(responses, new HashSet<>(Arrays.asList("dc2-node1.example.com")));

    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedHosts(Arrays.asList("dc1-node1.example.com", "dc2-node1.example.com"))
            .withScheme("http")
            .withPort(8000)
            .withRoutingScope(ClusterScope.create())
            .build();

    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, httpClient);

    liveNodes.updateLiveNodes();

    assertEquals(
        new LinkedHashSet<>(Arrays.asList("dc1-node1.example.com")),
        hostSet(liveNodes.getLiveNodes()));
    assertEquals(
        new HashSet<>(Arrays.asList("dc1-node1.example.com", "dc2-node1.example.com")),
        capturedHostSet(httpClient.capturedRequests));
  }

  @Test
  public void testDiscoveryUsesKnownLiveNodesBeforeInitialSeed() throws Exception {
    Map<String, String> responses = new HashMap<>();
    Set<String> failingHosts = new HashSet<>();
    responses.put("seed.example.com", "[\"node2.example.com\",\"node3.example.com\"]");
    responses.put("node2.example.com", "[\"node2.example.com\",\"node3.example.com\"]");
    responses.put("node3.example.com", "[\"node2.example.com\",\"node3.example.com\"]");
    DiscoveryHttpClient httpClient = new DiscoveryHttpClient(responses, failingHosts);

    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedHost("seed.example.com")
            .withScheme("http")
            .withPort(8000)
            .withRoutingScope(ClusterScope.create())
            .build();

    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, httpClient);
    liveNodes.updateLiveNodes();
    assertEquals(
        new LinkedHashSet<>(Arrays.asList("node2.example.com", "node3.example.com")),
        hostSet(liveNodes.getLiveNodes()));

    failingHosts.add("seed.example.com");
    httpClient.capturedRequests.clear();
    liveNodes.updateLiveNodes();

    assertEquals(
        new LinkedHashSet<>(Arrays.asList("node2.example.com", "node3.example.com")),
        hostSet(liveNodes.getLiveNodes()));
    assertEquals(
        new HashSet<>(Arrays.asList("node2.example.com", "node3.example.com")),
        capturedHostSet(httpClient.capturedRequests));
  }

  @Test
  public void testDiscoveryFallsBackToInitialSeedWhenLiveNodesReturnNoNodes() throws Exception {
    Map<String, String> responses = new HashMap<>();
    Set<String> failingHosts = new HashSet<>();
    responses.put("seed.example.com", "[\"node2.example.com\",\"node3.example.com\"]");
    responses.put("node2.example.com", "[]");
    responses.put("node3.example.com", "[]");
    DiscoveryHttpClient httpClient = new DiscoveryHttpClient(responses, failingHosts);

    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedHost("seed.example.com")
            .withScheme("http")
            .withPort(8000)
            .withRoutingScope(ClusterScope.create())
            .build();

    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, httpClient);
    liveNodes.updateLiveNodes();
    assertEquals(
        new LinkedHashSet<>(Arrays.asList("node2.example.com", "node3.example.com")),
        hostSet(liveNodes.getLiveNodes()));

    failingHosts.add("node2.example.com");
    responses.put("seed.example.com", "[\"recovered.example.com\"]");
    httpClient.capturedRequests.clear();
    liveNodes.updateLiveNodes();

    assertEquals(
        new LinkedHashSet<>(Arrays.asList("recovered.example.com")),
        hostSet(liveNodes.getLiveNodes()));
    assertEquals(
        Arrays.asList("node2.example.com", "node3.example.com", "seed.example.com"),
        capturedHosts(httpClient.capturedRequests));
  }

  @Test
  public void testRackScopeTriesNextSeedWhenFirstSeedIsInWrongDatacenter() throws Exception {
    Map<String, String> responses = new HashMap<>();
    responses.put("dc2-node1.example.com", "[]");
    responses.put("dc1-node1.example.com", "[\"dc1-rack1-node.example.com\"]");
    DiscoveryHttpClient httpClient = new DiscoveryHttpClient(responses);

    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedHosts(Arrays.asList("dc2-node1.example.com", "dc1-node1.example.com"))
            .withScheme("http")
            .withPort(8000)
            .withRoutingScope(
                RackScope.of("dc1", "rack1", DatacenterScope.of("dc1", ClusterScope.create())))
            .build();

    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, httpClient);

    liveNodes.updateLiveNodes();

    assertEquals(
        new LinkedHashSet<>(Arrays.asList("dc1-rack1-node.example.com")),
        hostSet(liveNodes.getLiveNodes()));
    assertEquals(
        Arrays.asList("dc2-node1.example.com", "dc1-node1.example.com"),
        capturedHosts(httpClient.capturedRequests));
    for (SdkHttpRequest request : httpClient.capturedRequests) {
      assertTrue(request.rawQueryParameters().containsKey("dc"));
      assertTrue(request.rawQueryParameters().containsKey("rack"));
    }
  }

  @Test
  public void testScopedLocalNodesQueryValuesAreEncodedOnce() throws Exception {
    Map<String, String> responses = new HashMap<>();
    responses.put("seed.example.com", "[\"scoped-node.example.com\"]");
    DiscoveryHttpClient httpClient = new DiscoveryHttpClient(responses);

    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedHost("seed.example.com")
            .withScheme("http")
            .withPort(8000)
            .withRoutingScope(RackScope.of("dc&prod", "rack=1/blue", null))
            .build();

    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, httpClient);
    liveNodes.updateLiveNodes();

    SdkHttpRequest request = httpClient.capturedRequests.get(0);
    String encodedQuery = request.encodedQueryParameters().get();
    assertTrue(encodedQuery.contains("dc=dc%26prod"));
    assertTrue(encodedQuery.contains("rack=rack%3D1%2Fblue"));
    assertFalse(encodedQuery.contains("%25"));
  }

  @Test
  public void testCustomScopeLocalNodesQueryValuesAreEncodedWhenNeeded() throws Exception {
    Map<String, String> responses = new HashMap<>();
    responses.put("seed.example.com", "[\"scoped-node.example.com\"]");
    DiscoveryHttpClient httpClient = new DiscoveryHttpClient(responses);

    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedHost("seed.example.com")
            .withScheme("http")
            .withPort(8000)
            .withRoutingScope(customScope("dc=us east"))
            .build();

    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, httpClient);
    liveNodes.updateLiveNodes();

    SdkHttpRequest request = httpClient.capturedRequests.get(0);
    assertEquals("dc=us%20east", request.encodedQueryParameters().get());
  }

  @Test
  public void testRackScopeValidationTriesNextSeedWhenFirstSeedIsInWrongDatacenter()
      throws Exception {
    Map<String, String> responses = new HashMap<>();
    responses.put("dc2-node1.example.com", "[]");
    responses.put("dc1-node1.example.com", "[\"dc1-rack1-node.example.com\"]");
    DiscoveryHttpClient httpClient = new DiscoveryHttpClient(responses);

    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedHosts(Arrays.asList("dc2-node1.example.com", "dc1-node1.example.com"))
            .withScheme("http")
            .withPort(8000)
            .withRoutingScope(
                RackScope.of("dc1", "rack1", DatacenterScope.of("dc1", ClusterScope.create())))
            .build();

    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, httpClient);

    liveNodes.checkIfRackAndDatacenterSetCorrectly();

    assertEquals(
        Arrays.asList("dc2-node1.example.com", "dc1-node1.example.com"),
        capturedHosts(httpClient.capturedRequests));
    for (SdkHttpRequest request : httpClient.capturedRequests) {
      assertTrue(request.rawQueryParameters().containsKey("dc"));
      assertTrue(request.rawQueryParameters().containsKey("rack"));
    }
  }

  private static Set<String> hostSet(List<URI> uris) {
    Set<String> hosts = new LinkedHashSet<>();
    for (URI uri : uris) {
      hosts.add(uri.getHost());
    }
    return hosts;
  }

  private static Set<String> capturedHostSet(List<SdkHttpRequest> requests) {
    Set<String> hosts = new HashSet<>();
    for (SdkHttpRequest request : requests) {
      hosts.add(request.host());
    }
    return hosts;
  }

  private static List<String> capturedHosts(List<SdkHttpRequest> requests) {
    List<String> hosts = new ArrayList<>();
    for (SdkHttpRequest request : requests) {
      hosts.add(request.host());
    }
    return hosts;
  }

  private static RoutingScope customScope(String query) {
    return new RoutingScope() {
      @Override
      public String getName() {
        return "Custom";
      }

      @Override
      public String getDescription() {
        return "Custom";
      }

      @Override
      public RoutingScope getFallback() {
        return null;
      }

      @Override
      public String getLocalNodesQuery() {
        return query;
      }
    };
  }
}
