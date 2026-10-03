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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.scylladb.alternator.AlternatorConfig;
import com.scylladb.alternator.CoversRequirements;
import com.scylladb.alternator.NodeHealthConfig;
import com.scylladb.alternator.NodeHealthState;
import com.scylladb.alternator.keyrouting.KeyRouteAffinity;
import com.scylladb.alternator.keyrouting.KeyRouteAffinityConfig;
import com.scylladb.alternator.queryplan.AffinityQueryPlanInterceptor;
import com.scylladb.alternator.routing.RackScope;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpFullResponse;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

/** Regression coverage for key-route affinity combined with rack-aware routing. */
public class AlternatorLiveNodesAffinityScopeTest {
  private static final URI SEED = URI.create("http://10.0.0.100:8000");
  private static final URI RACK_ONE_NODE = URI.create("http://10.0.0.1:8000");
  private static final URI RACK_TWO_NODE = URI.create("http://10.0.0.2:8000");
  private static final URI RACK_THREE_NODE = URI.create("http://10.0.0.3:8000");
  private static final List<URI> CLUSTER_NODES =
      Arrays.asList(RACK_ONE_NODE, RACK_TWO_NODE, RACK_THREE_NODE);
  private static final String TABLE_NAME = "test_table";
  private static final String PK_NAME = "pk";
  private static final String PK_VALUE = "shared-key";

  @Test
  @CoversRequirements("AFF-REQ-008")
  public void testRmwAffinityOverridesRackScopeWhileReadsRemainLocal() throws Exception {
    ScopeAwareDiscoveryHttpClient discoveryClient = new ScopeAwareDiscoveryHttpClient();
    KeyRouteAffinityConfig affinityConfig = affinityConfig(KeyRouteAffinity.RMW);
    AlternatorLiveNodes rackOne = createLiveNodesWithoutAffinityConfig("rack1", discoveryClient);
    AlternatorLiveNodes rackTwo = createLiveNodesWithoutAffinityConfig("rack2", discoveryClient);
    CapturingDynamoHttpClient rackOneTraffic = new CapturingDynamoHttpClient();
    CapturingDynamoHttpClient rackTwoTraffic = new CapturingDynamoHttpClient();
    DynamoDbClient rackOneClient = createClient(rackOne, affinityConfig, rackOneTraffic);
    DynamoDbClient rackTwoClient = createClient(rackTwo, affinityConfig, rackTwoTraffic);

    try {
      rackOne.refreshDiscoveredNodes();
      rackTwo.refreshDiscoveredNodes();

      assertEquals(Collections.singletonList(RACK_ONE_NODE), rackOne.getQueryPlanNodes());
      assertEquals(Collections.singletonList(RACK_TWO_NODE), rackTwo.getQueryPlanNodes());
      assertTrue(discoveryClient.getClusterRequestCount() >= 6);
      assertTrue(discoveryClient.getClusterRequestHosts().contains("10.0.0.1"));
      assertTrue(discoveryClient.getClusterRequestHosts().contains("10.0.0.2"));
      assertTrue(discoveryClient.getClusterRequestHosts().contains("10.0.0.3"));
      for (URI node : CLUSTER_NODES) {
        assertEquals(NodeHealthState.ACTIVE, rackOne.getNodeHealthStatus(node).getState());
        assertEquals(NodeHealthState.ACTIVE, rackTwo.getNodeHealthStatus(node).getState());
      }

      PutItemRequest conditionalPut =
          PutItemRequest.builder()
              .tableName(TABLE_NAME)
              .item(item())
              .conditionExpression("attribute_not_exists(pk)")
              .build();
      rackOneClient.putItem(conditionalPut);
      rackTwoClient.putItem(conditionalPut);

      URI rackOneWrite = rackOneTraffic.getLastRequestUri();
      URI rackTwoWrite = rackTwoTraffic.getLastRequestUri();
      assertSameEndpoint(rackOneWrite, rackTwoWrite);
      assertTrue(CLUSTER_NODES.contains(endpoint(rackOneWrite)));

      rackOneTraffic.clear();
      rackTwoTraffic.clear();
      PutItemRequest unconditionalPut =
          PutItemRequest.builder().tableName(TABLE_NAME).item(item()).build();
      rackOneClient.putItem(unconditionalPut);
      rackTwoClient.putItem(unconditionalPut);

      assertSameEndpoint(RACK_ONE_NODE, rackOneTraffic.getLastRequestUri());
      assertSameEndpoint(RACK_TWO_NODE, rackTwoTraffic.getLastRequestUri());

      rackOneTraffic.clear();
      rackTwoTraffic.clear();
      GetItemRequest read =
          GetItemRequest.builder().tableName(TABLE_NAME).key(key(PK_VALUE)).build();
      rackOneClient.getItem(read);
      rackTwoClient.getItem(read);

      assertSameEndpoint(RACK_ONE_NODE, rackOneTraffic.getLastRequestUri());
      assertSameEndpoint(RACK_TWO_NODE, rackTwoTraffic.getLastRequestUri());
    } finally {
      rackOneClient.close();
      rackTwoClient.close();
      rackOne.shutdownAndWait(1_000);
      rackTwo.shutdownAndWait(1_000);
    }
  }

  @Test
  public void testAnyWriteSingleAndBatchAffinityUseSameClusterRingAcrossRacks() throws Exception {
    ScopeAwareDiscoveryHttpClient discoveryClient = new ScopeAwareDiscoveryHttpClient();
    KeyRouteAffinityConfig affinityConfig = affinityConfig(KeyRouteAffinity.ANY_WRITE);
    AlternatorLiveNodes rackOne = createLiveNodes("rack1", affinityConfig, discoveryClient);
    AlternatorLiveNodes rackTwo = createLiveNodes("rack2", affinityConfig, discoveryClient);
    CapturingDynamoHttpClient rackOneTraffic = new CapturingDynamoHttpClient();
    CapturingDynamoHttpClient rackTwoTraffic = new CapturingDynamoHttpClient();
    DynamoDbClient rackOneClient = createClient(rackOne, affinityConfig, rackOneTraffic);
    DynamoDbClient rackTwoClient = createClient(rackTwo, affinityConfig, rackTwoTraffic);

    try {
      rackOne.refreshDiscoveredNodes();
      rackTwo.refreshDiscoveredNodes();

      PutItemRequest put = PutItemRequest.builder().tableName(TABLE_NAME).item(item()).build();
      rackOneClient.putItem(put);
      rackTwoClient.putItem(put);

      assertSameEndpoint(rackOneTraffic.getLastRequestUri(), rackTwoTraffic.getLastRequestUri());
      rackOneTraffic.clear();
      rackTwoTraffic.clear();

      Map<String, List<WriteRequest>> requestItems = new HashMap<>();
      requestItems.put(
          TABLE_NAME,
          Collections.singletonList(
              WriteRequest.builder()
                  .putRequest(PutRequest.builder().item(item()).build())
                  .build()));
      BatchWriteItemRequest request =
          BatchWriteItemRequest.builder().requestItems(requestItems).build();

      rackOneClient.batchWriteItem(request);
      rackTwoClient.batchWriteItem(request);

      assertSameEndpoint(rackOneTraffic.getLastRequestUri(), rackTwoTraffic.getLastRequestUri());
    } finally {
      rackOneClient.close();
      rackTwoClient.close();
      rackOne.shutdownAndWait(1_000);
      rackTwo.shutdownAndWait(1_000);
    }
  }

  @Test
  public void testDisabledAffinityDoesNotPerformClusterWideDiscovery() throws Exception {
    ScopeAwareDiscoveryHttpClient discoveryClient = new ScopeAwareDiscoveryHttpClient();
    KeyRouteAffinityConfig affinityConfig = affinityConfig(KeyRouteAffinity.NONE);
    AlternatorLiveNodes liveNodes = createLiveNodes("rack1", affinityConfig, discoveryClient);
    try {
      liveNodes.refreshDiscoveredNodes();

      assertEquals(Collections.singletonList(RACK_ONE_NODE), liveNodes.getQueryPlanNodes());
      assertEquals(0, discoveryClient.getClusterRequestCount());
    } finally {
      liveNodes.shutdownAndWait(1_000);
    }
  }

  @Test
  public void testClusterAffinityNodesParticipateInHealthProbes() throws Exception {
    ScopeAwareDiscoveryHttpClient discoveryClient = new ScopeAwareDiscoveryHttpClient(false);
    KeyRouteAffinityConfig affinityConfig = affinityConfig(KeyRouteAffinity.RMW);
    AlternatorLiveNodes liveNodes =
        createLiveNodes("rack1", affinityConfig, discoveryClient, false);
    try {
      liveNodes.refreshDiscoveredNodes();

      assertEquals(
          NodeHealthState.QUARANTINED, liveNodes.getNodeHealthStatus(RACK_TWO_NODE).getState());
      assertEquals(
          NodeHealthState.QUARANTINED, liveNodes.getNodeHealthStatus(RACK_THREE_NODE).getState());

      discoveryClient.setRemoteNodesReachable(true);
      List<URI> probed = liveNodes.probeQuarantinedNodes();

      assertEquals(Arrays.asList(RACK_TWO_NODE, RACK_THREE_NODE), probed);
      assertEquals(NodeHealthState.ACTIVE, liveNodes.getNodeHealthStatus(RACK_TWO_NODE).getState());
      assertEquals(
          NodeHealthState.ACTIVE, liveNodes.getNodeHealthStatus(RACK_THREE_NODE).getState());
    } finally {
      liveNodes.shutdownAndWait(1_000);
    }
  }

  @Test
  public void testNewAffinityRingIsPublishedOnlyAfterDirectVerification() throws Exception {
    BlockingVerificationDiscoveryHttpClient discoveryClient =
        new BlockingVerificationDiscoveryHttpClient();
    KeyRouteAffinityConfig affinityConfig = affinityConfig(KeyRouteAffinity.RMW);
    AlternatorLiveNodes liveNodes =
        createLiveNodes("rack1", affinityConfig, discoveryClient, false);
    AtomicReference<Throwable> refreshFailure = new AtomicReference<>();
    Thread refresh =
        new Thread(
            () -> {
              try {
                liveNodes.refreshDiscoveredNodes();
              } catch (Throwable failure) {
                refreshFailure.set(failure);
              }
            });
    try {
      refresh.start();
      assertTrue(discoveryClient.awaitVerification());

      assertEquals(
          Collections.singletonList(SEED), liveNodes.getDiscoveredNodesForAffinityQueryPlan());

      discoveryClient.releaseVerification();
      refresh.join(5_000);
      assertTrue("refresh should complete", !refresh.isAlive());
      assertNull(refreshFailure.get());
      assertEquals(CLUSTER_NODES, liveNodes.getDiscoveredNodesForAffinityQueryPlan());
    } finally {
      discoveryClient.releaseVerification();
      refresh.join(5_000);
      liveNodes.shutdownAndWait(1_000);
    }
  }

  @Test
  public void testAffinityVerificationExceedingProbeCapacityActivatesEntireRing() throws Exception {
    List<URI> clusterNodes = new ArrayList<>();
    for (int i = 1; i <= 20; i++) {
      clusterNodes.add(URI.create("http://10.0.1." + i + ":8000"));
    }
    LargeClusterDiscoveryHttpClient discoveryClient =
        new LargeClusterDiscoveryHttpClient(clusterNodes);
    KeyRouteAffinityConfig affinityConfig = affinityConfig(KeyRouteAffinity.RMW);
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedNode(SEED)
            .withRoutingScope(RackScope.of("dc1", "rack1", null))
            .withKeyRouteAffinity(affinityConfig)
            .withNodeHealthConfig(NodeHealthConfig.builder().withHealthProbeConcurrency(1).build())
            .build();
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, discoveryClient);
    try {
      liveNodes.refreshDiscoveredNodes();

      assertEquals(
          new HashSet<>(clusterNodes),
          new HashSet<>(liveNodes.getDiscoveredNodesForAffinityQueryPlan()));
      for (URI node : clusterNodes) {
        assertEquals(NodeHealthState.ACTIVE, liveNodes.getNodeHealthStatus(node).getState());
      }
    } finally {
      liveNodes.shutdownAndWait(1_000);
    }
  }

  private static AlternatorLiveNodes createLiveNodes(
      String rack, KeyRouteAffinityConfig affinityConfig, SdkHttpClient discoveryClient) {
    return createLiveNodes(rack, affinityConfig, discoveryClient, true);
  }

  private static AlternatorLiveNodes createLiveNodes(
      String rack,
      KeyRouteAffinityConfig affinityConfig,
      SdkHttpClient discoveryClient,
      boolean disableNodeHealth) {
    AlternatorConfig.Builder configBuilder =
        AlternatorConfig.builder()
            .withSeedNode(SEED)
            .withRoutingScope(RackScope.of("dc1", rack, null))
            .withKeyRouteAffinity(affinityConfig);
    if (disableNodeHealth) {
      configBuilder.withNodeHealthDisabled();
    }
    AlternatorConfig config = configBuilder.build();
    return new AlternatorLiveNodes(config, discoveryClient);
  }

  private static AlternatorLiveNodes createLiveNodesWithoutAffinityConfig(
      String rack, SdkHttpClient discoveryClient) {
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedNode(SEED)
            .withRoutingScope(RackScope.of("dc1", rack, null))
            .build();
    return new AlternatorLiveNodes(config, discoveryClient);
  }

  private static DynamoDbClient createClient(
      AlternatorLiveNodes liveNodes,
      KeyRouteAffinityConfig affinityConfig,
      SdkHttpClient httpClient) {
    ClientOverrideConfiguration overrides =
        ClientOverrideConfiguration.builder()
            .addExecutionInterceptor(new AffinityQueryPlanInterceptor(affinityConfig, liveNodes))
            .build();
    return DynamoDbClient.builder()
        .region(Region.of("test-region"))
        .credentialsProvider(AnonymousCredentialsProvider.create())
        .endpointOverride(SEED)
        .httpClient(httpClient)
        .overrideConfiguration(overrides)
        .build();
  }

  private static KeyRouteAffinityConfig affinityConfig(KeyRouteAffinity type) {
    return KeyRouteAffinityConfig.builder().withType(type).withPkInfo(TABLE_NAME, PK_NAME).build();
  }

  private static Map<String, AttributeValue> item() {
    Map<String, AttributeValue> item = key(PK_VALUE);
    item.put("value", AttributeValue.builder().s("payload").build());
    return item;
  }

  private static Map<String, AttributeValue> key(String value) {
    Map<String, AttributeValue> key = new HashMap<>();
    key.put(PK_NAME, AttributeValue.builder().s(value).build());
    return key;
  }

  private static void assertSameEndpoint(URI expected, URI actual) {
    assertEquals(expected.getScheme(), actual.getScheme());
    assertEquals(expected.getHost(), actual.getHost());
    assertEquals(expected.getPort(), actual.getPort());
  }

  private static URI endpoint(URI uri) {
    return URI.create(uri.getScheme() + "://" + uri.getHost() + ":" + uri.getPort());
  }

  private static final class ScopeAwareDiscoveryHttpClient implements SdkHttpClient {
    private final AtomicInteger clusterRequestCount = new AtomicInteger();
    private final List<String> clusterRequestHosts = new CopyOnWriteArrayList<>();
    private volatile boolean remoteNodesReachable;

    ScopeAwareDiscoveryHttpClient() {
      this(true);
    }

    ScopeAwareDiscoveryHttpClient(boolean remoteNodesReachable) {
      this.remoteNodesReachable = remoteNodesReachable;
    }

    void setRemoteNodesReachable(boolean remoteNodesReachable) {
      this.remoteNodesReachable = remoteNodesReachable;
    }

    int getClusterRequestCount() {
      return clusterRequestCount.get();
    }

    List<String> getClusterRequestHosts() {
      return clusterRequestHosts;
    }

    @Override
    public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
      SdkHttpRequest httpRequest = request.httpRequest();
      return new ExecutableHttpRequest() {
        @Override
        public HttpExecuteResponse call() throws IOException {
          List<String> racks = httpRequest.rawQueryParameters().get("rack");
          String rack = racks == null || racks.isEmpty() ? null : racks.get(0);
          String body;
          if (rack == null) {
            clusterRequestCount.incrementAndGet();
            clusterRequestHosts.add(httpRequest.host());
            if (!remoteNodesReachable
                && ("10.0.0.2".equals(httpRequest.host())
                    || "10.0.0.3".equals(httpRequest.host()))) {
              throw new IOException("simulated unreachable remote rack");
            }
            body = "[\"10.0.0.1\",\"10.0.0.2\",\"10.0.0.3\"]";
          } else if ("rack1".equals(rack)) {
            body = "[\"10.0.0.1\"]";
          } else if ("rack2".equals(rack)) {
            body = "[\"10.0.0.2\"]";
          } else {
            body = "[]";
          }
          return response(body);
        }

        @Override
        public void abort() {}
      };
    }

    @Override
    public void close() {}

    @Override
    public String clientName() {
      return "ScopeAwareDiscoveryHttpClient";
    }
  }

  private static final class CapturingDynamoHttpClient implements SdkHttpClient {
    private final List<URI> requests = new CopyOnWriteArrayList<>();

    void clear() {
      requests.clear();
    }

    URI getLastRequestUri() {
      return requests.get(requests.size() - 1);
    }

    @Override
    public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
      requests.add(request.httpRequest().getUri());
      return new ExecutableHttpRequest() {
        @Override
        public HttpExecuteResponse call() {
          return response("{}");
        }

        @Override
        public void abort() {}
      };
    }

    @Override
    public void close() {}

    @Override
    public String clientName() {
      return "CapturingDynamoHttpClient";
    }
  }

  private static final class BlockingVerificationDiscoveryHttpClient implements SdkHttpClient {
    private final CountDownLatch verificationStarted = new CountDownLatch(1);
    private final CountDownLatch verificationReleased = new CountDownLatch(1);

    boolean awaitVerification() throws InterruptedException {
      return verificationStarted.await(5, TimeUnit.SECONDS);
    }

    void releaseVerification() {
      verificationReleased.countDown();
    }

    @Override
    public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
      SdkHttpRequest httpRequest = request.httpRequest();
      return new ExecutableHttpRequest() {
        @Override
        public HttpExecuteResponse call() throws IOException {
          List<String> racks = httpRequest.rawQueryParameters().get("rack");
          if (racks != null && !racks.isEmpty()) {
            return response("[\"10.0.0.1\"]");
          }
          if ("10.0.0.2".equals(httpRequest.host())) {
            verificationStarted.countDown();
            try {
              if (!verificationReleased.await(5, TimeUnit.SECONDS)) {
                throw new IOException("timed out waiting to release verification");
              }
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new IOException("verification interrupted", e);
            }
          }
          return response("[\"10.0.0.1\",\"10.0.0.2\",\"10.0.0.3\"]");
        }

        @Override
        public void abort() {
          verificationReleased.countDown();
        }
      };
    }

    @Override
    public void close() {}

    @Override
    public String clientName() {
      return "BlockingVerificationDiscoveryHttpClient";
    }
  }

  private static final class LargeClusterDiscoveryHttpClient implements SdkHttpClient {
    private final String clusterResponse;
    private final String rackResponse;

    LargeClusterDiscoveryHttpClient(List<URI> clusterNodes) {
      List<String> quotedHosts = new ArrayList<>();
      for (URI node : clusterNodes) {
        quotedHosts.add("\"" + node.getHost() + "\"");
      }
      this.clusterResponse = "[" + String.join(",", quotedHosts) + "]";
      this.rackResponse = "[" + quotedHosts.get(0) + "]";
    }

    @Override
    public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
      boolean rackScoped = request.httpRequest().rawQueryParameters().containsKey("rack");
      return new ExecutableHttpRequest() {
        @Override
        public HttpExecuteResponse call() {
          return response(rackScoped ? rackResponse : clusterResponse);
        }

        @Override
        public void abort() {}
      };
    }

    @Override
    public void close() {}

    @Override
    public String clientName() {
      return "LargeClusterDiscoveryHttpClient";
    }
  }

  private static HttpExecuteResponse response(String body) {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    SdkHttpFullResponse response =
        SdkHttpFullResponse.builder()
            .statusCode(200)
            .putHeader("Content-Type", "application/x-amz-json-1.0")
            .putHeader("Content-Length", String.valueOf(bytes.length))
            .build();
    return HttpExecuteResponse.builder()
        .response(response)
        .responseBody(AbortableInputStream.create(new ByteArrayInputStream(bytes)))
        .build();
  }
}
