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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scylladb.alternator.routing.ClusterScope;
import com.scylladb.alternator.routing.DatacenterScope;
import com.scylladb.alternator.routing.RackScope;
import com.scylladb.alternator.routing.RoutingScope;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.endpoints.DynamoDbEndpointParams;
import software.amazon.awssdk.services.dynamodb.endpoints.DynamoDbEndpointProvider;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.Select;

/** Unit tests for topology discovery through Alternator's DynamoDB system-table access. */
public class SystemTableTopologyDiscoveryTest {

  @Test
  public void readTopologyScansLocalAndPeersWithPaginationAtRequestedEndpoint() throws Exception {
    DynamoDbClient client = mock(DynamoDbClient.class);
    URI endpoint = URI.create("https://seed.example.com:8043");
    Map<String, AttributeValue> cursor =
        Collections.singletonMap("peer", stringAttribute("page-two"));
    when(client.scan(any(ScanRequest.class)))
        .thenReturn(
            ScanResponse.builder()
                .items(Collections.singletonList(row("10.0.0.1", "dc1", "rack1")))
                .build(),
            ScanResponse.builder()
                .items(Collections.singletonList(row("10.0.0.2", "dc1", "rack2")))
                .lastEvaluatedKey(cursor)
                .build(),
            ScanResponse.builder()
                .items(Collections.singletonList(row("10.0.1.1", "dc2", "rack1")))
                .build());
    SystemTableTopologyDiscovery discovery =
        new SystemTableTopologyDiscovery(client, "https", 8043);

    List<TopologyNode> topology = discovery.readTopology(endpoint);

    assertEquals(
        Arrays.asList(
            URI.create("https://10.0.0.1:8043"),
            URI.create("https://10.0.0.2:8043"),
            URI.create("https://10.0.1.1:8043")),
        nodeUris(topology));

    ArgumentCaptor<ScanRequest> requests = ArgumentCaptor.forClass(ScanRequest.class);
    verify(client, times(3)).scan(requests.capture());
    List<ScanRequest> captured = requests.getAllValues();
    assertEquals(SystemTableTopologyDiscovery.LOCAL_TABLE, captured.get(0).tableName());
    assertEquals(SystemTableTopologyDiscovery.PEERS_TABLE, captured.get(1).tableName());
    assertEquals(SystemTableTopologyDiscovery.PEERS_TABLE, captured.get(2).tableName());
    assertTrue(captured.get(0).exclusiveStartKey().isEmpty());
    assertTrue(captured.get(1).exclusiveStartKey().isEmpty());
    assertEquals(cursor, captured.get(2).exclusiveStartKey());
    for (ScanRequest request : captured) {
      assertEquals("rpc_address,data_center,rack", request.projectionExpression());
      assertEquals(Boolean.TRUE, request.consistentRead());
      assertEndpoint(request, endpoint);
    }
  }

  @Test
  public void readTopologyParsesDeduplicatesAndSkipsInvalidRows() {
    DynamoDbClient client = mock(DynamoDbClient.class);
    Map<String, AttributeValue> missingAddress = new LinkedHashMap<>();
    missingAddress.put("data_center", stringAttribute("dc1"));
    Map<String, AttributeValue> nonStringAddress = new LinkedHashMap<>();
    nonStringAddress.put("rpc_address", AttributeValue.builder().n("42").build());
    when(client.scan(any(ScanRequest.class)))
        .thenReturn(
            ScanResponse.builder()
                .items(
                    Arrays.asList(
                        row("127.0.0.1", "dc1", "rack1"),
                        missingAddress,
                        row("bad address", "dc1", "rack1"),
                        nonStringAddress))
                .build(),
            ScanResponse.builder()
                .items(
                    Arrays.asList(
                        row("127.0.0.1", "ignored-dc", "ignored-rack"),
                        row("::1", null, null),
                        row(" 192.0.2.10 ", "dc2", "rack9"),
                        row("198.51.100.1", "dc2", "rack1"),
                        row("198.51.100.1", "dc3", "rack2"),
                        row(" ", "dc2", "rack9")))
                .build());
    SystemTableTopologyDiscovery discovery =
        new SystemTableTopologyDiscovery(client, "https", 8043);

    List<TopologyNode> topology =
        discovery.readTopology(URI.create("https://seed.example.com:8043"));

    assertEquals(4, topology.size());
    assertNode(topology.get(0), "https://127.0.0.1:8043", "dc1", "rack1");
    assertNode(topology.get(1), "https://[::1]:8043", null, null);
    assertNode(topology.get(2), "https://192.0.2.10:8043", "dc2", "rack9");
    assertNode(topology.get(3), "https://198.51.100.1:8043", null, null);
  }

  @Test
  public void nodesForScopeFiltersBuiltInAndCustomScopes() {
    DynamoDbClient client = mock(DynamoDbClient.class);
    SystemTableTopologyDiscovery discovery = new SystemTableTopologyDiscovery(client, "http", 8000);
    TopologyNode dc1Rack1 = node("node1.example.com", "dc1", "rack1");
    TopologyNode dc1Rack2 = node("node2.example.com", "dc1", "rack2");
    TopologyNode dc2Rack1 = node("node3.example.com", "dc2", "rack1");
    TopologyNode encoded = node("node4.example.com", "dc&prod", "rack=blue/1");
    TopologyNode missingMetadata = node("node5.example.com", null, null);
    List<TopologyNode> topology =
        Arrays.asList(dc1Rack1, null, dc1Rack2, dc2Rack1, encoded, missingMetadata);

    assertEquals(
        nodeUris(Arrays.asList(dc1Rack1, dc1Rack2, dc2Rack1, encoded, missingMetadata)),
        discovery.nodesForScope(topology, ClusterScope.create()));
    assertEquals(
        nodeUris(Arrays.asList(dc1Rack1, dc1Rack2)),
        discovery.nodesForScope(topology, DatacenterScope.of("dc1", null)));
    assertEquals(
        nodeUris(Collections.singletonList(dc1Rack2)),
        discovery.nodesForScope(topology, RackScope.of("dc1", "rack2", null)));
    assertEquals(
        nodeUris(Collections.singletonList(encoded)),
        discovery.nodesForScope(topology, customScope("?dc=dc%26prod&rack=rack%3Dblue%2F1")));
    assertTrue(discovery.nodesForScope(topology, customScope("unknown=value")).isEmpty());
    assertTrue(discovery.nodesForScope(topology, customScope("dc=dc1&unknown=value")).isEmpty());
    assertTrue(discovery.nodesForScope(topology, customScope("rack=rack1")).isEmpty());
    assertTrue(discovery.nodesForScope(topology, customScope("dc=%ZZ")).isEmpty());
    assertTrue(discovery.nodesForScope(topology, customScope(null)).isEmpty());
  }

  @Test
  public void readTopologyPropagatesScanFailureInsteadOfPublishingPartialTopology() {
    DynamoDbClient client = mock(DynamoDbClient.class);
    RuntimeException failure = new RuntimeException("peers scan failed");
    when(client.scan(any(ScanRequest.class)))
        .thenReturn(
            ScanResponse.builder()
                .items(Collections.singletonList(row("127.0.0.1", "dc1", "rack1")))
                .build())
        .thenThrow(failure);
    SystemTableTopologyDiscovery discovery = new SystemTableTopologyDiscovery(client, "http", 8000);

    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () -> discovery.readTopology(URI.create("http://seed.example.com:8000")));

    assertSame(failure, thrown);
    verify(client, times(2)).scan(any(ScanRequest.class));
  }

  @Test
  public void readTopologyRejectsPeersOnlySnapshotWhenLocalRowIsInvalid() {
    DynamoDbClient client = mock(DynamoDbClient.class);
    when(client.scan(any(ScanRequest.class)))
        .thenReturn(
            ScanResponse.builder()
                .items(Collections.singletonList(Collections.emptyMap()))
                .build());
    SystemTableTopologyDiscovery discovery = new SystemTableTopologyDiscovery(client, "http", 8000);

    IllegalStateException failure =
        assertThrows(
            IllegalStateException.class,
            () -> discovery.readTopology(URI.create("http://seed.example.com:8000")));

    assertTrue(failure.getMessage().contains("no valid system.local row"));
    verify(client).scan(any(ScanRequest.class));
  }

  @Test
  public void readTopologyRejectsRepeatedPaginationCursor() {
    DynamoDbClient client = mock(DynamoDbClient.class);
    Map<String, AttributeValue> cursor =
        Collections.singletonMap("peer", stringAttribute("same-page"));
    when(client.scan(any(ScanRequest.class)))
        .thenReturn(
            ScanResponse.builder()
                .items(Collections.singletonList(row("127.0.0.1", "dc1", "rack1")))
                .build(),
            ScanResponse.builder().items(Collections.emptyList()).lastEvaluatedKey(cursor).build(),
            ScanResponse.builder().items(Collections.emptyList()).lastEvaluatedKey(cursor).build());
    SystemTableTopologyDiscovery discovery = new SystemTableTopologyDiscovery(client, "http", 8000);

    IllegalStateException failure =
        assertThrows(
            IllegalStateException.class,
            () -> discovery.readTopology(URI.create("http://seed.example.com:8000")));

    assertTrue(failure.getMessage().contains("repeated pagination cursor"));
    verify(client, times(3)).scan(any(ScanRequest.class));
  }

  @Test
  public void probeUsesMinimalConsistentSystemLocalScanAtRequestedEndpoint() throws Exception {
    DynamoDbClient client = mock(DynamoDbClient.class);
    Map<String, AttributeValue> cursor =
        Collections.singletonMap("key", stringAttribute("ignored-page"));
    when(client.scan(any(ScanRequest.class)))
        .thenReturn(ScanResponse.builder().count(1).lastEvaluatedKey(cursor).build());
    SystemTableTopologyDiscovery discovery = new SystemTableTopologyDiscovery(client, "http", 8000);
    URI endpoint = URI.create("http://probe.example.com:8000");

    discovery.probe(endpoint);

    ArgumentCaptor<ScanRequest> request = ArgumentCaptor.forClass(ScanRequest.class);
    verify(client).scan(request.capture());
    ScanRequest captured = request.getValue();
    assertEquals(SystemTableTopologyDiscovery.LOCAL_TABLE, captured.tableName());
    assertEquals(Select.COUNT, captured.select());
    assertEquals(Integer.valueOf(1), captured.limit());
    assertEquals(Boolean.TRUE, captured.consistentRead());
    assertNull(captured.projectionExpression());
    assertFalse(captured.hasExclusiveStartKey());
    assertEndpoint(captured, endpoint);
  }

  private static AttributeValue stringAttribute(String value) {
    return AttributeValue.builder().s(value).build();
  }

  private static Map<String, AttributeValue> row(String address, String dataCenter, String rack) {
    Map<String, AttributeValue> row = new LinkedHashMap<>();
    row.put("rpc_address", stringAttribute(address));
    if (dataCenter != null) {
      row.put("data_center", stringAttribute(dataCenter));
    }
    if (rack != null) {
      row.put("rack", stringAttribute(rack));
    }
    return row;
  }

  private static TopologyNode node(String host, String dataCenter, String rack) {
    return new TopologyNode(URI.create("http://" + host + ":8000"), dataCenter, rack);
  }

  private static List<URI> nodeUris(List<TopologyNode> nodes) {
    List<URI> uris = new ArrayList<>();
    for (TopologyNode node : nodes) {
      uris.add(node.getUri());
    }
    return uris;
  }

  private static void assertNode(
      TopologyNode node, String expectedUri, String expectedDataCenter, String expectedRack) {
    assertEquals(URI.create(expectedUri), node.getUri());
    assertEquals(expectedDataCenter, node.getDataCenter());
    assertEquals(expectedRack, node.getRack());
  }

  private static void assertEndpoint(ScanRequest request, URI expected) throws Exception {
    DynamoDbEndpointProvider endpointProvider =
        (DynamoDbEndpointProvider) request.overrideConfiguration().get().endpointProvider().get();
    assertEquals(
        expected,
        endpointProvider
            .resolveEndpoint(DynamoDbEndpointParams.builder().build())
            .get(1, TimeUnit.SECONDS)
            .url());
  }

  private static RoutingScope customScope(String query) {
    return new RoutingScope() {
      @Override
      public String getName() {
        return "Custom";
      }

      @Override
      public String getDescription() {
        return "Custom topology scope";
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
