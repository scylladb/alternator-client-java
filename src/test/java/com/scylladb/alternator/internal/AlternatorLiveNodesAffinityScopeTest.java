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
import com.scylladb.alternator.CoversRequirements;
import com.scylladb.alternator.NodeHealthState;
import com.scylladb.alternator.keyrouting.KeyRouteAffinity;
import com.scylladb.alternator.keyrouting.KeyRouteAffinityConfig;
import com.scylladb.alternator.queryplan.AffinityQueryPlanInterceptor;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
      assertEquals(2, discoveryClient.getDatacenterRequestCount());
      for (URI node : CLUSTER_NODES) {
        assertEquals(NodeHealthState.QUARANTINED, rackOne.getNodeHealthStatus(node).getState());
        assertEquals(NodeHealthState.QUARANTINED, rackTwo.getNodeHealthStatus(node).getState());
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
  public void testAnyWriteSingleAndBatchAffinityUseSameDatacenterRingAcrossRacks()
      throws Exception {
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

      discoveryClient.setDatacenterResponse("[\"10.0.0.3\"]");
      rackOne.refreshDiscoveredNodes();
      rackTwo.refreshDiscoveredNodes();
      assertEquals(
          Collections.singletonList(RACK_THREE_NODE), rackOne.getAffinityQueryPlanNodesSnapshot());
      assertEquals(
          Collections.singletonList(RACK_THREE_NODE), rackTwo.getAffinityQueryPlanNodesSnapshot());

      Map<String, AttributeValue> missingKey =
          Collections.singletonMap("value", AttributeValue.builder().s("no-key").build());
      PutItemRequest fallbackPut =
          PutItemRequest.builder().tableName(TABLE_NAME).item(missingKey).build();
      rackOneTraffic.clear();
      rackTwoTraffic.clear();
      rackOneClient.putItem(fallbackPut);
      rackTwoClient.putItem(fallbackPut);
      assertSameEndpoint(RACK_ONE_NODE, rackOneTraffic.getLastRequestUri());
      assertSameEndpoint(RACK_TWO_NODE, rackTwoTraffic.getLastRequestUri());

      Map<String, List<WriteRequest>> missingKeyBatch =
          Collections.singletonMap(
              TABLE_NAME,
              Collections.singletonList(
                  WriteRequest.builder()
                      .putRequest(PutRequest.builder().item(missingKey).build())
                      .build()));
      BatchWriteItemRequest fallbackBatch =
          BatchWriteItemRequest.builder().requestItems(missingKeyBatch).build();
      rackOneTraffic.clear();
      rackTwoTraffic.clear();
      rackOneClient.batchWriteItem(fallbackBatch);
      rackTwoClient.batchWriteItem(fallbackBatch);
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
  public void testDisabledAffinityDoesNotPerformDatacenterDiscovery() throws Exception {
    ScopeAwareDiscoveryHttpClient discoveryClient = new ScopeAwareDiscoveryHttpClient();
    KeyRouteAffinityConfig affinityConfig = affinityConfig(KeyRouteAffinity.NONE);
    AlternatorLiveNodes liveNodes = createLiveNodes("rack1", affinityConfig, discoveryClient);
    try {
      liveNodes.refreshDiscoveredNodes();

      assertEquals(Collections.singletonList(RACK_ONE_NODE), liveNodes.getQueryPlanNodes());
      assertEquals(0, discoveryClient.getDatacenterRequestCount());
    } finally {
      liveNodes.shutdownAndWait(1_000);
    }
  }

  @Test
  public void testDatacenterAffinityNodesParticipateInHealthProbes() throws Exception {
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

      assertEquals(CLUSTER_NODES, probed);
      assertEquals(NodeHealthState.ACTIVE, liveNodes.getNodeHealthStatus(RACK_TWO_NODE).getState());
      assertEquals(
          NodeHealthState.ACTIVE, liveNodes.getNodeHealthStatus(RACK_THREE_NODE).getState());
    } finally {
      liveNodes.shutdownAndWait(1_000);
    }
  }

  @Test
  public void testAffinityRefreshPublishesQuarantineWithoutSynchronousVerification()
      throws Exception {
    ScopeAwareDiscoveryHttpClient http = new ScopeAwareDiscoveryHttpClient(false);
    AlternatorLiveNodes nodes =
        createLiveNodes("rack1", affinityConfig(KeyRouteAffinity.RMW), http, false);
    try {
      nodes.refreshDiscoveredNodes();
      assertEquals(CLUSTER_NODES, nodes.getAffinityQueryPlanNodesSnapshot());
      assertEquals(0, http.getDirectProbeCount());
      nodes.refreshDiscoveredNodes();
      assertEquals(CLUSTER_NODES, nodes.getAffinityQueryPlanNodesSnapshot());
      assertEquals(0, http.getDirectProbeCount());
      assertEquals(
          NodeHealthState.QUARANTINED, nodes.getNodeHealthStatus(RACK_TWO_NODE).getState());
      assertEquals(
          NodeHealthState.QUARANTINED, nodes.getNodeHealthStatus(RACK_THREE_NODE).getState());
      assertEquals(Collections.singletonList(RACK_ONE_NODE), nodes.getQueryPlanNodes());
      http.setRemoteNodesReachable(true);
      assertEquals(Arrays.asList(RACK_TWO_NODE, RACK_THREE_NODE), nodes.probeQuarantinedNodes());
      assertEquals(NodeHealthState.ACTIVE, nodes.getNodeHealthStatus(RACK_TWO_NODE).getState());
    } finally {
      nodes.shutdownAndWait(1_000);
    }
  }

  @Test
  public void testAffinityLeavesUnusedCustomTailForConfiguredAndLateEnablement() throws Exception {
    for (boolean late : new boolean[] {false, true}) {
      ChainDiscoveryHttpClient http = new ChainDiscoveryHttpClient();
      http.responses.put("custom=1", "[\"10.0.0.1\"]");
      TrackingScope custom =
          new TrackingScope(
              RackScope.of(
                  "A",
                  "r1",
                  RackScope.of("A", "r1", DatacenterScope.of("A", ClusterScope.create()))),
              () -> "custom=1");
      AlternatorLiveNodes nodes =
          createChainLiveNodes(custom, http, late ? KeyRouteAffinity.NONE : KeyRouteAffinity.RMW);
      try {
        assertEquals(0, custom.fallbackReads);
        assertEquals(0, custom.queryReads);
        nodes.enableAffinityRouting();
        nodes.enableAffinityRouting();
        assertEquals(0, custom.fallbackReads);
        nodes.refreshDiscoveredNodes();
        nodes.refreshDiscoveredNodes();
        assertEquals(0, custom.fallbackReads);
        assertEquals(Arrays.asList("custom=1", "custom=1"), http.queries);
        assertEquals(
            Collections.singletonList(RACK_ONE_NODE), nodes.getAffinityQueryPlanNodesSnapshot());
        assertTrue(nodes.getRoutingScope() == custom);
      } finally {
        nodes.shutdownAndWait(1_000);
      }
    }
  }

  @Test
  public void testDisabledAffinityDoesNotReadUnusedCustomFallback() throws Exception {
    ChainDiscoveryHttpClient http = new ChainDiscoveryHttpClient();
    http.responses.put("custom=1", "[\"10.0.0.1\"]");
    TrackingScope custom = new TrackingScope(RackScope.of("A", "r1", null), () -> "custom=1");
    AlternatorLiveNodes nodes = createChainLiveNodes(custom, http, KeyRouteAffinity.NONE);
    try {
      assertEquals(0, custom.fallbackReads);
      assertEquals(0, custom.queryReads);
      nodes.refreshDiscoveredNodes();
      assertEquals(0, custom.fallbackReads);
      assertEquals(1, custom.queryReads);
      assertEquals(Collections.singletonList("custom=1"), http.queries);
    } finally {
      nodes.shutdownAndWait(1_000);
    }
  }

  @Test
  public void testCustomQueryIsCapturedOnceForDiscoveryAndCacheIdentity() throws Exception {
    for (boolean enabled : new boolean[] {false, true}) {
      ChainDiscoveryHttpClient http = new ChainDiscoveryHttpClient();
      http.responses.put("dc=A", "[\"10.0.0.1\"]");
      http.responses.put("dc=B", "[\"10.0.0.2\"]");
      AtomicInteger reads = new AtomicInteger();
      TrackingScope custom =
          new TrackingScope(
              RackScope.of("A", "r1", null),
              () -> reads.incrementAndGet() % 2 == 1 ? "dc=A" : "dc=B");
      AlternatorLiveNodes nodes =
          createChainLiveNodes(
              custom, http, enabled ? KeyRouteAffinity.RMW : KeyRouteAffinity.NONE);
      try {
        assertEquals(0, reads.get());
        nodes.refreshDiscoveredNodes();
        assertEquals(Collections.singletonList(RACK_ONE_NODE), nodes.getQueryPlanNodes());
        nodes.refreshDiscoveredNodes();
        assertEquals(Collections.singletonList(RACK_TWO_NODE), nodes.getQueryPlanNodes());
        assertEquals(
            Collections.singletonList(RACK_TWO_NODE), nodes.getAffinityQueryPlanNodesSnapshot());
        assertEquals(2, reads.get());
        assertEquals(Arrays.asList("dc=A", "dc=B"), http.queries);
      } finally {
        nodes.shutdownAndWait(1_000);
      }
    }
  }

  @Test
  public void testInitiallyEquivalentCustomQueriesCanDivergeOnLaterRefresh() throws Exception {
    ChainDiscoveryHttpClient http = new ChainDiscoveryHttpClient();
    http.responses.put("dc=A&rack=r1", "[\"10.0.0.1\"]");
    http.responses.put("dc=B", "[\"10.0.0.2\",\"10.0.0.3\"]");
    AtomicInteger phase = new AtomicInteger();
    TrackingScope second =
        new TrackingScope(ClusterScope.create(), () -> phase.get() == 0 ? "dc=A" : "dc=B");
    TrackingScope first = new TrackingScope(second, () -> "dc=A");
    AlternatorLiveNodes nodes =
        createChainLiveNodes(RackScope.of("A", "r1", first), http, KeyRouteAffinity.RMW);
    try {
      assertEquals(0, first.queryReads + second.queryReads);
      assertEquals(0, first.fallbackReads);
      assertEquals(0, second.fallbackReads);
      nodes.refreshDiscoveredNodes();
      assertEquals(Collections.singletonList(SEED), nodes.getAffinityQueryPlanNodesSnapshot());
      phase.incrementAndGet();
      http.responses.remove("dc=A&rack=r1");
      nodes.refreshDiscoveredNodes();
      assertEquals(
          Arrays.asList(RACK_TWO_NODE, RACK_THREE_NODE), nodes.getAffinityQueryPlanNodesSnapshot());
      assertEquals(Arrays.asList(RACK_TWO_NODE, RACK_THREE_NODE), nodes.getQueryPlanNodes());
      assertEquals(2, first.fallbackReads);
      assertEquals(1, second.fallbackReads);
      assertEquals(2, first.queryReads);
      assertEquals(2, second.queryReads);
    } finally {
      nodes.shutdownAndWait(1_000);
    }
  }

  @Test
  public void testFailedEquivalentAffinityOperationIsReusedAndRetriedNextRefresh()
      throws Exception {
    ChainDiscoveryHttpClient http = new ChainDiscoveryHttpClient();
    http.responses.put("dc=A&rack=r1", "[\"10.0.0.1\"]");
    http.responses.put("dc=A", "failure");
    TrackingScope sameDatacenter = new TrackingScope(ClusterScope.create(), () -> "dc=A");
    RoutingScope scope =
        RackScope.of("A", "r1", RackScope.of("A", "r1", DatacenterScope.of("A", sameDatacenter)));
    AlternatorLiveNodes nodes = createChainLiveNodes(scope, http, KeyRouteAffinity.RMW);
    try {
      nodes.refreshDiscoveredNodes();
      assertEquals(2, Collections.frequency(http.queries, "dc=A"));
      http.queries.clear();
      http.responses.put("dc=A", "[\"10.0.0.2\",\"10.0.0.3\"]");
      nodes.refreshDiscoveredNodes();
      assertEquals(1, Collections.frequency(http.queries, "dc=A"));
      assertEquals(
          Arrays.asList(RACK_TWO_NODE, RACK_THREE_NODE), nodes.getAffinityQueryPlanNodesSnapshot());
    } finally {
      nodes.shutdownAndWait(1_000);
    }
  }

  @Test
  public void testAffinityPlansKeepSnapshotAcrossActualRefreshAndRetryCycles() throws Exception {
    for (boolean preferred : new boolean[] {false, true}) {
      ChainDiscoveryHttpClient http = new ChainDiscoveryHttpClient();
      http.responses.put("dc=A&rack=r1", "[\"10.0.0.1\"]");
      http.responses.put("dc=A", "[\"10.0.0.1\",\"10.0.0.2\",\"10.0.0.3\"]");
      AlternatorLiveNodes nodes =
          createChainLiveNodes(RackScope.of("A", "r1", null), http, KeyRouteAffinity.RMW);
      try {
        nodes.refreshDiscoveredNodes();
        List<URI> snapshot = nodes.getAffinityQueryPlanNodesSnapshot();
        LazyQueryPlan source =
            preferred
                ? LazyQueryPlan.fromAffinitySnapshot(
                    nodes, snapshot, Collections.singletonList(RACK_TWO_NODE))
                : new LazyQueryPlan(nodes, 42L);
        NodeHealthQueryPlan plan = nodes.newAffinityQueryPlan(source);
        List<URI> firstCycle = new ArrayList<>();
        firstCycle.add(plan.nextRouteCandidate());
        http.responses.put("dc=A", "[\"10.9.0.1\"]");
        nodes.refreshDiscoveredNodes();
        firstCycle.add(plan.nextRouteCandidate());
        firstCycle.add(plan.nextRouteCandidate());
        assertEquals(new HashSet<>(CLUSTER_NODES), new HashSet<>(firstCycle));
        List<URI> secondCycle =
            Arrays.asList(
                plan.nextRouteCandidate(), plan.nextRouteCandidate(), plan.nextRouteCandidate());
        assertEquals(firstCycle, secondCycle);
        assertEquals(
            Collections.singletonList(URI.create("http://10.9.0.1:8000")),
            nodes.getAffinityQueryPlanNodesSnapshot());
        assertEquals(
            URI.create("http://10.9.0.1:8000"),
            nodes.newAffinityQueryPlan(new LazyQueryPlan(nodes, 42L)).nextRouteCandidate());
      } finally {
        nodes.shutdownAndWait(1_000);
      }
    }
  }

  @Test
  public void testCustomBareQueryRemainsDistinctFromClusterSeedAggregation() throws Exception {
    AtomicInteger bareCalls = new AtomicInteger();
    SdkHttpClient http =
        new SdkHttpClient() {
          @Override
          public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
            SdkHttpRequest httpRequest = request.httpRequest();
            return new ExecutableHttpRequest() {
              @Override
              public HttpExecuteResponse call() {
                if (httpRequest.rawQueryParameters().containsKey("rack")) {
                  return response("[\"10.0.0.1\",\"10.0.0.4\"]");
                }
                if (!httpRequest.rawQueryParameters().isEmpty()
                    || bareCalls.incrementAndGet() == 1) {
                  return response("[]");
                }
                return response(
                    "10.0.0.1".equals(httpRequest.host()) ? "[\"10.0.0.2\"]" : "[\"10.0.0.3\"]");
              }

              @Override
              public void abort() {}
            };
          }

          @Override
          public void close() {}

          @Override
          public String clientName() {
            return "AggregationDiscoveryHttpClient";
          }
        };
    TrackingScope custom =
        new TrackingScope(RackScope.of("A", "r1", ClusterScope.create()), () -> "");
    AlternatorLiveNodes nodes = createChainLiveNodes(custom, http, KeyRouteAffinity.RMW);
    try {
      nodes.refreshDiscoveredNodes();
      assertEquals(
          Arrays.asList(RACK_TWO_NODE, RACK_THREE_NODE), nodes.getAffinityQueryPlanNodesSnapshot());
      assertEquals(4, bareCalls.get());
    } finally {
      nodes.shutdownAndWait(1_000);
    }
  }

  @Test(timeout = 5_000)
  public void testAffinityScopeResolutionContainsUnusedCustomCycle() throws Exception {
    TrackingScope custom = new TrackingScope(null, () -> "custom=1");
    custom.fallback = custom;
    ChainDiscoveryHttpClient http = new ChainDiscoveryHttpClient();
    http.responses.put("custom=1", "[\"10.0.0.1\"]");
    AlternatorLiveNodes nodes = createChainLiveNodes(custom, http, KeyRouteAffinity.RMW);
    try {
      assertEquals(0, custom.fallbackReads);
      nodes.refreshDiscoveredNodes();
      assertEquals(Collections.singletonList(RACK_ONE_NODE), nodes.getQueryPlanNodes());
      assertEquals(0, custom.fallbackReads);
    } finally {
      nodes.shutdownAndWait(1_000);
    }
  }

  @Test
  public void testAuxiliaryFallbackGetterDoesNotIntroduceConstructionFailure() throws Exception {
    TrackingScope custom = new TrackingScope(null, () -> "custom=1");
    custom.throwOnFallback = true;
    ChainDiscoveryHttpClient http = new ChainDiscoveryHttpClient();
    http.responses.put("dc=A&rack=r1", "[\"10.0.0.1\"]");
    http.responses.put("dc=A", "[\"10.0.0.2\"]");
    AlternatorLiveNodes nodes =
        createChainLiveNodes(RackScope.of("A", "r1", custom), http, KeyRouteAffinity.RMW);
    try {
      assertEquals(0, custom.fallbackReads);
      assertEquals(0, custom.queryReads);
      nodes.refreshDiscoveredNodes();
      assertEquals(Collections.singletonList(RACK_ONE_NODE), nodes.getQueryPlanNodes());
      assertEquals(
          Collections.singletonList(RACK_TWO_NODE), nodes.getAffinityQueryPlanNodesSnapshot());
      assertEquals(0, custom.fallbackReads);
    } finally {
      nodes.shutdownAndWait(1_000);
    }
  }

  @Test
  public void testAffinityPreservesExplicitCrossDatacenterFallbackOrder() throws Exception {
    RoutingScope scope = RackScope.of("A", "r1", RackScope.of("B", "r2", ClusterScope.create()));
    ChainDiscoveryHttpClient http = new ChainDiscoveryHttpClient();
    http.responses.put("dc=A&rack=r1", "[\"10.0.0.1\"]");
    http.responses.put("dc=B", "[\"10.0.0.2\",\"10.0.0.3\"]");
    http.responses.put("", "[\"10.9.0.1\"]");
    AlternatorLiveNodes nodes = createChainLiveNodes(scope, http, KeyRouteAffinity.RMW);
    try {
      nodes.refreshDiscoveredNodes();
      assertEquals(Collections.singletonList(RACK_ONE_NODE), nodes.getQueryPlanNodes());
      assertEquals(
          Arrays.asList(RACK_TWO_NODE, RACK_THREE_NODE), nodes.getAffinityQueryPlanNodesSnapshot());
      assertEquals(Arrays.asList("dc=A&rack=r1", "dc=A", "dc=B"), http.uniqueQueries());

      http.queries.clear();
      http.responses.remove("dc=A&rack=r1");
      http.responses.put("dc=B&rack=r2", "[\"10.0.0.2\"]");
      http.responses.put("dc=A", "[\"10.0.0.1\",\"10.0.0.3\"]");
      nodes.refreshDiscoveredNodes();
      assertEquals(Collections.singletonList(RACK_TWO_NODE), nodes.getQueryPlanNodes());
      assertEquals(
          Arrays.asList(RACK_ONE_NODE, RACK_THREE_NODE), nodes.getAffinityQueryPlanNodesSnapshot());
      assertEquals(Arrays.asList("dc=A&rack=r1", "dc=B&rack=r2", "dc=A"), http.uniqueQueries());

      http.queries.clear();
      http.responses.put("dc=A&rack=r1", "[\"10.0.0.1\"]");
      http.responses.remove("dc=A");
      http.responses.remove("dc=B");
      nodes.refreshDiscoveredNodes();
      assertEquals(
          Collections.singletonList(URI.create("http://10.9.0.1:8000")),
          nodes.getAffinityQueryPlanNodesSnapshot());
      assertEquals(Arrays.asList("dc=A&rack=r1", "dc=A", "dc=B", ""), http.uniqueQueries());
    } finally {
      nodes.shutdownAndWait(1_000);
    }
  }

  @Test
  public void testStrictAffinityRetainsPreviousViewOnEmptyFailureAndRecovers() throws Exception {
    ChainDiscoveryHttpClient http = new ChainDiscoveryHttpClient();
    String dc = "A & /";
    RoutingScope scope = RackScope.of(dc, "r1", null);
    String rackQuery = scope.getLocalNodesQuery();
    String dcQuery = DatacenterScope.of(dc, null).getLocalNodesQuery();
    http.responses.put(rackQuery, "[\"10.0.0.1\"]");
    http.responses.put("", "[\"10.9.0.1\"]");
    AlternatorLiveNodes nodes = createChainLiveNodes(scope, http, KeyRouteAffinity.RMW);
    try {
      assertEquals(Collections.singletonList(SEED), nodes.getAffinityQueryPlanNodesSnapshot());
      nodes.refreshDiscoveredNodes();
      assertEquals(Collections.singletonList(SEED), nodes.getAffinityQueryPlanNodesSnapshot());
      assertEquals(Arrays.asList(rackQuery, dcQuery), http.uniqueQueries());
      http.responses.put(dcQuery, "[\"10.0.0.1\",\"10.0.0.2\"]");
      nodes.refreshDiscoveredNodes();
      assertEquals(
          Arrays.asList(RACK_ONE_NODE, RACK_TWO_NODE), nodes.getAffinityQueryPlanNodesSnapshot());
      http.responses.put(dcQuery, "failure");
      nodes.refreshDiscoveredNodes();
      assertEquals(
          Arrays.asList(RACK_ONE_NODE, RACK_TWO_NODE), nodes.getAffinityQueryPlanNodesSnapshot());
      http.responses.remove(dcQuery);
      nodes.refreshDiscoveredNodes();
      assertEquals(
          Arrays.asList(RACK_ONE_NODE, RACK_TWO_NODE), nodes.getAffinityQueryPlanNodesSnapshot());
      http.responses.put(dcQuery, "[\"10.0.0.3\"]");
      nodes.refreshDiscoveredNodes();
      assertEquals(
          Collections.singletonList(RACK_THREE_NODE), nodes.getAffinityQueryPlanNodesSnapshot());
      assertEquals(Collections.singletonList(RACK_ONE_NODE), nodes.getQueryPlanNodes());
      assertTrue(!http.queries.contains(""));
    } finally {
      nodes.shutdownAndWait(1_000);
    }
  }

  @Test
  public void testAffinityReusesMatchingOrdinaryScopeAndDisabledAffinityAddsNoDiscovery()
      throws Exception {
    for (RoutingScope scope :
        Arrays.asList(
            DatacenterScope.of("A", null),
            DatacenterScope.of("A", RackScope.of("B", "r2", null)))) {
      ChainDiscoveryHttpClient http = new ChainDiscoveryHttpClient();
      http.responses.put("dc=A", "[\"10.0.0.1\",\"10.0.0.2\"]");
      AlternatorLiveNodes nodes = createChainLiveNodes(scope, http, KeyRouteAffinity.RMW);
      try {
        nodes.refreshDiscoveredNodes();
        assertEquals(
            Arrays.asList(RACK_ONE_NODE, RACK_TWO_NODE), nodes.getAffinityQueryPlanNodesSnapshot());
        assertEquals(Collections.singletonList("dc=A"), http.queries);
      } finally {
        nodes.shutdownAndWait(1_000);
      }
    }
    ChainDiscoveryHttpClient http = new ChainDiscoveryHttpClient();
    http.responses.put("dc=A&rack=r1", "[\"10.0.0.1\"]");
    AlternatorLiveNodes nodes =
        createChainLiveNodes(RackScope.of("A", "r1", null), http, KeyRouteAffinity.NONE);
    try {
      nodes.refreshDiscoveredNodes();
      assertEquals(Collections.singletonList("dc=A&rack=r1"), http.queries);
      assertEquals(
          Collections.singletonList(RACK_ONE_NODE), nodes.getAffinityQueryPlanNodesSnapshot());
    } finally {
      nodes.shutdownAndWait(1_000);
    }
  }

  @Test
  public void testAffinityReusesOrdinaryDatacenterFallback() throws Exception {
    ChainDiscoveryHttpClient http = new ChainDiscoveryHttpClient();
    http.responses.put("dc=A", "[\"10.0.0.1\",\"10.0.0.2\"]");
    AlternatorLiveNodes nodes =
        createChainLiveNodes(
            RackScope.of("A", "r1", DatacenterScope.of("A", null)), http, KeyRouteAffinity.RMW);
    try {
      nodes.refreshDiscoveredNodes();
      assertEquals(
          Arrays.asList(RACK_ONE_NODE, RACK_TWO_NODE), nodes.getAffinityQueryPlanNodesSnapshot());
      assertEquals(1, Collections.frequency(http.queries, "dc=A"));
    } finally {
      nodes.shutdownAndWait(1_000);
    }
  }

  @Test
  public void testCustomScopeKeepsItsQueryWhileRackFallbackWidens() throws Exception {
    RoutingScope scope =
        new RoutingScope() {
          private final RoutingScope fallback = RackScope.of("B", "r2", null);

          @Override
          public String getName() {
            return "custom";
          }

          @Override
          public String getDescription() {
            return "Custom targeting";
          }

          @Override
          public String getLocalNodesQuery() {
            return "custom=1";
          }

          @Override
          public synchronized RoutingScope getFallback() {
            return fallback;
          }
        };
    ChainDiscoveryHttpClient http = new ChainDiscoveryHttpClient();
    http.responses.put("dc=B&rack=r2", "[\"10.0.0.2\"]");
    http.responses.put("dc=B", "[\"10.0.0.2\",\"10.0.0.3\"]");
    AlternatorLiveNodes nodes = createChainLiveNodes(scope, http, KeyRouteAffinity.RMW);
    try {
      nodes.refreshDiscoveredNodes();
      assertEquals(Collections.singletonList(RACK_TWO_NODE), nodes.getQueryPlanNodes());
      assertEquals(
          Arrays.asList(RACK_TWO_NODE, RACK_THREE_NODE), nodes.getAffinityQueryPlanNodesSnapshot());
      assertEquals(Arrays.asList("custom=1", "dc=B&rack=r2", "dc=B"), http.uniqueQueries());
      assertEquals(1, Collections.frequency(http.queries, "custom=1"));
    } finally {
      nodes.shutdownAndWait(1_000);
    }
  }

  @Test
  public void testFiniteCustomOccurrencesReachHiddenRack() throws Exception {
    ChainDiscoveryHttpClient http = new ChainDiscoveryHttpClient();
    http.responses.put("dc=B&rack=r2", "[\"10.0.0.1\"]");
    http.responses.put("dc=B", "[\"10.0.0.2\",\"10.0.0.3\"]");
    RoutingScope rack = RackScope.of("B", "r2", null);
    TrackingScope custom = new TrackingScope(null, () -> "custom=1");
    custom.fallbackSupplier = () -> custom.fallbackReads == 1 ? custom : rack;
    AlternatorLiveNodes nodes = createChainLiveNodes(custom, http, KeyRouteAffinity.RMW);
    try {
      assertEquals(0, custom.fallbackReads);
      nodes.refreshDiscoveredNodes();
      assertEquals(2, custom.fallbackReads);
      assertEquals(2, custom.queryReads);
      assertEquals(2, Collections.frequency(http.queries, "custom=1"));
      assertEquals(Collections.singletonList(RACK_ONE_NODE), nodes.getQueryPlanNodes());
      assertEquals(
          Arrays.asList(RACK_TWO_NODE, RACK_THREE_NODE), nodes.getAffinityQueryPlanNodesSnapshot());
    } finally {
      nodes.shutdownAndWait(1_000);
    }
  }

  @Test
  public void testRepeatedCustomOccurrenceQueriesPreserveOrdinaryDiscovery() throws Exception {
    for (KeyRouteAffinity mode : Arrays.asList(KeyRouteAffinity.NONE, KeyRouteAffinity.RMW)) {
      ChainDiscoveryHttpClient http = new ChainDiscoveryHttpClient();
      http.responses.put("custom=B", "[\"10.0.0.2\"]");
      http.responses.put("dc=B&rack=r2", "[\"10.0.0.1\"]");
      AtomicInteger queries = new AtomicInteger();
      TrackingScope custom =
          new TrackingScope(null, () -> queries.incrementAndGet() == 1 ? "custom=A" : "custom=B");
      custom.fallbackSupplier =
          () -> custom.fallbackReads == 1 ? custom : RackScope.of("B", "r2", null);
      AlternatorLiveNodes nodes = createChainLiveNodes(custom, http, mode);
      try {
        assertEquals(0, custom.fallbackReads);
        nodes.refreshDiscoveredNodes();
        assertEquals(1, custom.fallbackReads);
        assertEquals(2, custom.queryReads);
        assertEquals(Arrays.asList("custom=A", "custom=B"), http.queries);
        assertEquals(Collections.singletonList(RACK_TWO_NODE), nodes.getQueryPlanNodes());
        assertEquals(
            Collections.singletonList(RACK_TWO_NODE), nodes.getAffinityQueryPlanNodesSnapshot());
      } finally {
        nodes.shutdownAndWait(1_000);
      }
    }
  }

  @Test
  public void testNestedCustomOnlyChainUsesOnlyActualOrdinaryObservations() throws Exception {
    ChainDiscoveryHttpClient http = new ChainDiscoveryHttpClient();
    http.responses.put("custom=B", "[\"10.0.0.1\"]");
    http.responses.put("custom=C", "[\"10.0.0.2\"]");
    AtomicInteger phase = new AtomicInteger();
    TrackingScope first = new TrackingScope(null, () -> "custom=B");
    TrackingScope second = new TrackingScope(null, () -> "custom=C");
    TrackingScope head = new TrackingScope(null, () -> "custom=A");
    head.fallbackSupplier = () -> phase.get() == 0 ? first : second;
    AlternatorLiveNodes nodes = createChainLiveNodes(head, http, KeyRouteAffinity.RMW);
    try {
      nodes.refreshDiscoveredNodes();
      phase.incrementAndGet();
      nodes.refreshDiscoveredNodes();
      assertEquals(2, head.fallbackReads);
      assertEquals(1, first.queryReads);
      assertEquals(1, second.queryReads);
      assertEquals(0, first.fallbackReads + second.fallbackReads);
      assertEquals(
          Arrays.asList("custom=A", "custom=B", "custom=A", "custom=A", "custom=C"), http.queries);
      assertEquals(Collections.singletonList(RACK_TWO_NODE), nodes.getQueryPlanNodes());
      assertEquals(
          Collections.singletonList(RACK_TWO_NODE), nodes.getAffinityQueryPlanNodesSnapshot());
    } finally {
      nodes.shutdownAndWait(1_000);
    }
  }

  @Test
  public void testHiddenRackRetainsLastCustomViewWhenDatacenterFails() throws Exception {
    ChainDiscoveryHttpClient http = new ChainDiscoveryHttpClient();
    http.responses.put("custom=1", "[\"10.0.0.3\"]");
    http.responses.put("dc=B&rack=r2", "[\"10.0.0.1\"]");
    http.responses.put("dc=B", "failure");
    TrackingScope custom = new TrackingScope(RackScope.of("B", "r2", null), () -> "custom=1");
    AlternatorLiveNodes nodes = createChainLiveNodes(custom, http, KeyRouteAffinity.RMW);
    try {
      nodes.refreshDiscoveredNodes();
      assertEquals(
          Collections.singletonList(RACK_THREE_NODE), nodes.getAffinityQueryPlanNodesSnapshot());
      http.responses.remove("custom=1");
      nodes.refreshDiscoveredNodes();
      assertEquals(1, custom.fallbackReads);
      assertEquals(Collections.singletonList(RACK_ONE_NODE), nodes.getQueryPlanNodes());
      assertEquals(
          Collections.singletonList(RACK_THREE_NODE), nodes.getAffinityQueryPlanNodesSnapshot());
    } finally {
      nodes.shutdownAndWait(1_000);
    }
  }

  @Test
  public void testDeferredFallbackFailureIsRetriedOnLaterRefresh() throws Exception {
    ChainDiscoveryHttpClient http = new ChainDiscoveryHttpClient();
    http.responses.put("dc=A&rack=r1", "[\"10.0.0.1\"]");
    http.responses.put("dc=B", "[\"10.0.0.2\",\"10.0.0.3\"]");
    TrackingScope custom = new TrackingScope(null, () -> "custom=1");
    custom.fallbackSupplier =
        () -> {
          if (custom.fallbackReads == 1) {
            throw new IllegalStateException("transient fallback failure");
          }
          return RackScope.of("B", "r2", null);
        };
    AlternatorLiveNodes nodes =
        createChainLiveNodes(RackScope.of("A", "r1", custom), http, KeyRouteAffinity.RMW);
    try {
      assertEquals(0, custom.fallbackReads);
      nodes.refreshDiscoveredNodes();
      assertEquals(1, custom.fallbackReads);
      assertEquals(Collections.singletonList(SEED), nodes.getAffinityQueryPlanNodesSnapshot());
      nodes.refreshDiscoveredNodes();
      assertEquals(2, custom.fallbackReads);
      assertEquals(
          Arrays.asList(RACK_TWO_NODE, RACK_THREE_NODE), nodes.getAffinityQueryPlanNodesSnapshot());
    } finally {
      nodes.shutdownAndWait(1_000);
    }
  }

  @Test
  public void testConcurrentRefreshesRetainOneSuccessfulFallbackResolution() throws Exception {
    ChainDiscoveryHttpClient http = new ChainDiscoveryHttpClient();
    http.responses.put("dc=A&rack=r1", "[\"10.0.0.1\"]");
    http.responses.put("dc=B", "[\"10.0.0.2\",\"10.0.0.3\"]");
    CountDownLatch ordinaryRequests = new CountDownLatch(2);
    CountDownLatch resolving = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    http.onQuery =
        query -> {
          if ("dc=A&rack=r1".equals(query)) {
            ordinaryRequests.countDown();
          }
        };
    TrackingScope custom = new TrackingScope(null, () -> "custom=1");
    custom.fallbackSupplier =
        () -> {
          resolving.countDown();
          try {
            if (!release.await(5, TimeUnit.SECONDS)) {
              throw new IllegalStateException("fallback release timed out");
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
          }
          return RackScope.of("B", "r2", null);
        };
    AlternatorLiveNodes nodes =
        createChainLiveNodes(RackScope.of("A", "r1", custom), http, KeyRouteAffinity.RMW);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      assertEquals(0, custom.fallbackReads);
      Future<?> first = executor.submit(() -> refresh(nodes));
      assertTrue("fallback should be needed", resolving.await(5, TimeUnit.SECONDS));
      Future<?> second = executor.submit(() -> refresh(nodes));
      assertTrue(
          "ordinary discovery must run during resolution",
          ordinaryRequests.await(5, TimeUnit.SECONDS));
      assertEquals(Collections.singletonList(SEED), nodes.getAffinityQueryPlanNodesSnapshot());
      release.countDown();
      first.get(5, TimeUnit.SECONDS);
      second.get(5, TimeUnit.SECONDS);
      assertEquals(1, custom.fallbackReads);
      assertEquals(
          Arrays.asList(RACK_TWO_NODE, RACK_THREE_NODE), nodes.getAffinityQueryPlanNodesSnapshot());
    } finally {
      release.countDown();
      executor.shutdownNow();
      assertTrue("refresh tasks must stop", executor.awaitTermination(5, TimeUnit.SECONDS));
      nodes.shutdownAndWait(1_000);
    }
  }

  @Test
  @CoversRequirements("AFF-REQ-008")
  public void testAuxiliaryFailureDoesNotEvaluateUnusedCustomDescription() throws Exception {
    AtomicInteger queryReads = new AtomicInteger();
    AtomicInteger descriptionReads = new AtomicInteger();
    TrackingScope custom =
        new TrackingScope(
            null,
            () -> {
              if (queryReads.incrementAndGet() > 1) {
                throw new IllegalStateException("Custom query temporarily unavailable");
              }
              return "custom=retained";
            });
    custom.description =
        () -> {
          descriptionReads.incrementAndGet();
          throw new IllegalStateException("Unused custom description");
        };
    ChainDiscoveryHttpClient http = new ChainDiscoveryHttpClient();
    http.responses.put("dc=A&rack=a", "[\"10.0.0.1\"]");
    http.responses.put("custom=retained", "[\"10.0.0.2\"]");
    AlternatorLiveNodes nodes =
        createChainLiveNodes(RackScope.of("A", "a", custom), http, KeyRouteAffinity.RMW);
    try {
      nodes.refreshDiscoveredNodes();
      List<URI> affinity = nodes.getAffinityQueryPlanNodesSnapshot();
      assertEquals(Collections.singletonList(RACK_TWO_NODE), affinity);

      nodes.refreshDiscoveredNodes();

      assertEquals(Collections.singletonList(RACK_ONE_NODE), nodes.getQueryPlanNodes());
      assertEquals(affinity, nodes.getAffinityQueryPlanNodesSnapshot());
      assertEquals(2, queryReads.get());
      assertEquals(0, descriptionReads.get());
    } finally {
      nodes.shutdownAndWait(1_000);
    }
  }

  private static void refresh(AlternatorLiveNodes nodes) {
    try {
      nodes.refreshDiscoveredNodes();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  private static AlternatorLiveNodes createChainLiveNodes(
      RoutingScope scope, SdkHttpClient http, KeyRouteAffinity mode) {
    return new AlternatorLiveNodes(
        AlternatorConfig.builder()
            .withSeedNode(SEED)
            .withRoutingScope(scope)
            .withKeyRouteAffinity(affinityConfig(mode))
            .withNodeHealthDisabled()
            .build(),
        http);
  }

  private static final class TrackingScope implements RoutingScope {
    private RoutingScope fallback;
    private boolean throwOnFallback;
    private java.util.function.Supplier<RoutingScope> fallbackSupplier;
    private java.util.function.Supplier<String> description = () -> "Custom";
    private final java.util.function.Supplier<String> query;
    private int fallbackReads;
    private int queryReads;

    private TrackingScope(RoutingScope fallback, java.util.function.Supplier<String> query) {
      this.fallback = fallback;
      this.query = query;
    }

    @Override
    public String getName() {
      return "Custom";
    }

    @Override
    public String getDescription() {
      return description.get();
    }

    @Override
    public synchronized RoutingScope getFallback() {
      fallbackReads++;
      if (throwOnFallback) {
        throw new IllegalStateException("Unused custom fallback unavailable");
      }
      return fallbackSupplier == null ? fallback : fallbackSupplier.get();
    }

    @Override
    public synchronized String getLocalNodesQuery() {
      queryReads++;
      return query.get();
    }
  }

  private static final class ChainDiscoveryHttpClient implements SdkHttpClient {
    final Map<String, String> responses = new ConcurrentHashMap<>();
    final List<String> queries = new CopyOnWriteArrayList<>();
    private java.util.function.Consumer<String> onQuery;

    List<String> uniqueQueries() {
      return new ArrayList<>(new java.util.LinkedHashSet<>(queries));
    }

    @Override
    public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
      String query = request.httpRequest().getUri().getRawQuery();
      String[] parameters = query == null ? new String[0] : query.split("&");
      Arrays.sort(parameters);
      String key = String.join("&", parameters);
      return new ExecutableHttpRequest() {
        @Override
        public HttpExecuteResponse call() throws IOException {
          queries.add(key);
          if (onQuery != null) {
            onQuery.accept(key);
          }
          String body = responses.getOrDefault(key, "[]");
          if ("failure".equals(body)) {
            throw new IOException("simulated discovery failure");
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
      return "ChainDiscoveryHttpClient";
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
    private final AtomicInteger datacenterRequestCount = new AtomicInteger();
    private final AtomicInteger directProbeCount = new AtomicInteger();
    private volatile boolean remoteNodesReachable;
    private volatile String datacenterResponse = "[\"10.0.0.1\",\"10.0.0.2\",\"10.0.0.3\"]";

    ScopeAwareDiscoveryHttpClient() {
      this(true);
    }

    ScopeAwareDiscoveryHttpClient(boolean remoteNodesReachable) {
      this.remoteNodesReachable = remoteNodesReachable;
    }

    void setRemoteNodesReachable(boolean remoteNodesReachable) {
      this.remoteNodesReachable = remoteNodesReachable;
    }

    void setDatacenterResponse(String response) {
      datacenterResponse = response;
    }

    int getDatacenterRequestCount() {
      return datacenterRequestCount.get();
    }

    int getDirectProbeCount() {
      return directProbeCount.get();
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
            if (httpRequest.rawQueryParameters().isEmpty()) {
              directProbeCount.incrementAndGet();
            }
            if (httpRequest.rawQueryParameters().containsKey("dc")) {
              datacenterRequestCount.incrementAndGet();
            }
            if (!remoteNodesReachable
                && ("10.0.0.2".equals(httpRequest.host())
                    || "10.0.0.3".equals(httpRequest.host()))) {
              throw new IOException("simulated unreachable remote rack");
            }
            body =
                httpRequest.rawQueryParameters().containsKey("dc")
                    ? datacenterResponse
                    : "[\"10.0.0.1\",\"10.0.0.2\",\"10.0.0.3\"]";
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
