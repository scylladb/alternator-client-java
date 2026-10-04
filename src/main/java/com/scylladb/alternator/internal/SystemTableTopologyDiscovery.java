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

import com.scylladb.alternator.routing.ClusterScope;
import com.scylladb.alternator.routing.DatacenterScope;
import com.scylladb.alternator.routing.RackScope;
import com.scylladb.alternator.routing.RoutingScope;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import software.amazon.awssdk.endpoints.Endpoint;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.endpoints.DynamoDbEndpointProvider;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.Select;

/** Reads Alternator cluster topology from the Scylla system tables. */
final class SystemTableTopologyDiscovery {
  static final String LOCAL_TABLE = ".scylla.alternator.system.local";
  static final String PEERS_TABLE = ".scylla.alternator.system.peers";

  private static final String RPC_ADDRESS = "rpc_address";
  private static final String DATA_CENTER = "data_center";
  private static final String RACK = "rack";
  private static final String PROJECTION = RPC_ADDRESS + "," + DATA_CENTER + "," + RACK;

  private final DynamoDbClient client;
  private final String scheme;
  private final int port;
  private final long operationTimeoutNanos;

  SystemTableTopologyDiscovery(DynamoDbClient client, String scheme, int port) {
    this(client, scheme, port, 5_000);
  }

  SystemTableTopologyDiscovery(
      DynamoDbClient client, String scheme, int port, long operationTimeoutMs) {
    this.client = Objects.requireNonNull(client, "client");
    this.scheme = Objects.requireNonNull(scheme, "scheme");
    this.port = port;
    if (operationTimeoutMs <= 0) {
      throw new IllegalArgumentException("operationTimeoutMs must be positive");
    }
    this.operationTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(operationTimeoutMs);
  }

  /**
   * Reads one complete topology snapshot from the supplied endpoint.
   *
   * <p>A snapshot is valid only when {@code system.local} contains at least one usable local-node
   * row. Both tables are read completely before the snapshot is returned, so scan failures never
   * publish partial topology.
   *
   * @param endpoint node that serves both system-table scans
   * @return valid local and peer nodes, deduplicated by URI
   * @throws IllegalStateException when no valid local-node row is returned
   */
  List<TopologyNode> readTopology(URI endpoint) {
    Objects.requireNonNull(endpoint, "endpoint");
    DynamoDbEndpointProvider endpointProvider = endpointProvider(endpoint);
    long deadlineNanos = System.nanoTime() + operationTimeoutNanos;
    Map<URI, TopologyNode> localNodes = new LinkedHashMap<>();
    scanTable(LOCAL_TABLE, endpointProvider, localNodes, new HashSet<>(), deadlineNanos);
    if (localNodes.isEmpty()) {
      throw new IllegalStateException(
          "Topology read from " + endpoint + " returned no valid system.local row");
    }

    Map<URI, TopologyNode> peerNodes = new LinkedHashMap<>();
    scanTable(PEERS_TABLE, endpointProvider, peerNodes, new HashSet<>(), deadlineNanos);
    Map<URI, TopologyNode> nodes = new LinkedHashMap<>(localNodes);
    for (TopologyNode peerNode : peerNodes.values()) {
      // system.local is authoritative when stale peers metadata repeats the contacted node.
      nodes.putIfAbsent(peerNode.getUri(), peerNode);
    }
    return new ArrayList<>(nodes.values());
  }

  /**
   * Verifies that an endpoint can serve an authenticated, strongly consistent system-table read.
   *
   * @param endpoint node to probe
   */
  void probe(URI endpoint) {
    Objects.requireNonNull(endpoint, "endpoint");
    client.scan(
        ScanRequest.builder()
            .tableName(LOCAL_TABLE)
            .select(Select.COUNT)
            .limit(1)
            .consistentRead(true)
            .overrideConfiguration(builder -> builder.endpointProvider(endpointProvider(endpoint)))
            .build());
  }

  /**
   * Selects topology nodes belonging to a routing scope.
   *
   * <p>Cluster scope retains nodes with missing location metadata. Datacenter and rack scopes do
   * not, because their metadata cannot be matched.
   *
   * @param topology complete topology snapshot
   * @param scope scope to apply client-side
   * @return node URIs matching the scope, in snapshot order
   */
  List<URI> nodesForScope(List<TopologyNode> topology, RoutingScope scope) {
    Objects.requireNonNull(topology, "topology");
    Objects.requireNonNull(scope, "scope");

    ScopeFilter filter = ScopeFilter.from(scope);
    List<URI> nodes = new ArrayList<>();
    for (TopologyNode node : topology) {
      if (node != null && filter.matches(node)) {
        nodes.add(node.getUri());
      }
    }
    return nodes;
  }

  private void scanTable(
      String tableName,
      DynamoDbEndpointProvider endpointProvider,
      Map<URI, TopologyNode> nodes,
      Set<URI> ambiguousNodes,
      long deadlineNanos) {
    Map<String, AttributeValue> startKey = Collections.emptyMap();
    Set<Map<String, AttributeValue>> seenCursors = new HashSet<>();
    do {
      Duration remaining = remainingDuration(deadlineNanos);
      ScanRequest.Builder request =
          ScanRequest.builder()
              .tableName(tableName)
              .projectionExpression(PROJECTION)
              .consistentRead(true)
              .overrideConfiguration(
                  builder ->
                      builder
                          .endpointProvider(endpointProvider)
                          .apiCallTimeout(remaining)
                          .apiCallAttemptTimeout(remaining));
      if (!startKey.isEmpty()) {
        request.exclusiveStartKey(startKey);
      }

      ScanResponse response = client.scan(request.build());
      for (Map<String, AttributeValue> item : response.items()) {
        TopologyNode node = parseNode(item);
        if (node == null || ambiguousNodes.contains(node.getUri())) {
          continue;
        }
        TopologyNode existing = nodes.putIfAbsent(node.getUri(), node);
        if (existing != null && !existing.hasSameLocation(node)) {
          // Scan order is unspecified. Retain the usable endpoint for cluster scope, but clear
          // ambiguous location metadata so it cannot enter a datacenter or rack scope
          // nondeterministically.
          nodes.put(node.getUri(), new TopologyNode(node.getUri(), null, null));
          ambiguousNodes.add(node.getUri());
        }
      }
      Map<String, AttributeValue> nextKey = response.lastEvaluatedKey();
      if (nextKey != null && !nextKey.isEmpty() && !seenCursors.add(new LinkedHashMap<>(nextKey))) {
        throw new IllegalStateException(
            "Topology scan of " + tableName + " returned a repeated pagination cursor");
      }
      startKey = nextKey;
    } while (startKey != null && !startKey.isEmpty());
  }

  private static Duration remainingDuration(long deadlineNanos) {
    if (Thread.currentThread().isInterrupted()) {
      throw new CancellationException("Topology scan interrupted");
    }
    long remainingNanos = deadlineNanos - System.nanoTime();
    if (remainingNanos <= 0) {
      throw new IllegalStateException("Topology scan exceeded its operation timeout");
    }
    return Duration.ofNanos(remainingNanos);
  }

  private static DynamoDbEndpointProvider endpointProvider(URI endpoint) {
    return ignored -> CompletableFuture.completedFuture(Endpoint.builder().url(endpoint).build());
  }

  private TopologyNode parseNode(Map<String, AttributeValue> item) {
    String address = stringAttribute(item, RPC_ADDRESS);
    if (address == null) {
      return null;
    }

    try {
      URI uri = new URI(scheme, null, address.trim(), port, null, null, null);
      if (uri.getHost() == null) {
        return null;
      }
      return new TopologyNode(uri, stringAttribute(item, DATA_CENTER), stringAttribute(item, RACK));
    } catch (URISyntaxException | IllegalArgumentException e) {
      return null;
    }
  }

  private static String stringAttribute(Map<String, AttributeValue> item, String name) {
    if (item == null) {
      return null;
    }
    AttributeValue value = item.get(name);
    if (value == null || value.s() == null || value.s().isEmpty()) {
      return null;
    }
    return value.s();
  }

  private static final class ScopeFilter {
    private final String dataCenter;
    private final String rack;
    private final boolean matchesNothing;

    private ScopeFilter(String dataCenter, String rack) {
      this(dataCenter, rack, false);
    }

    private ScopeFilter(String dataCenter, String rack, boolean matchesNothing) {
      this.dataCenter = dataCenter;
      this.rack = rack;
      this.matchesNothing = matchesNothing;
    }

    static ScopeFilter from(RoutingScope scope) {
      if (scope instanceof ClusterScope) {
        return new ScopeFilter(null, null);
      }
      if (scope instanceof RackScope) {
        RackScope rackScope = (RackScope) scope;
        return new ScopeFilter(rackScope.getDatacenter(), rackScope.getRack());
      }
      if (scope instanceof DatacenterScope) {
        return new ScopeFilter(((DatacenterScope) scope).getDatacenter(), null);
      }
      return fromQuery(scope.getLocalNodesQuery());
    }

    private static ScopeFilter fromQuery(String query) {
      if (query == null) {
        return noMatch();
      }
      String dataCenter = null;
      String rack = null;
      String normalized = query.startsWith("?") ? query.substring(1) : query;
      if (normalized.isEmpty()) {
        return new ScopeFilter(null, null);
      }
      for (String parameter : normalized.split("&", -1)) {
        int separator = parameter.indexOf('=');
        if (parameter.isEmpty() || separator < 0) {
          return noMatch();
        }
        try {
          String name = decode(parameter.substring(0, separator));
          String value = decode(parameter.substring(separator + 1));
          if ("dc".equals(name)) {
            if (dataCenter != null || value.isEmpty()) {
              return noMatch();
            }
            dataCenter = value;
          } else if ("rack".equals(name)) {
            if (rack != null || value.isEmpty()) {
              return noMatch();
            }
            rack = value;
          } else {
            return noMatch();
          }
        } catch (IllegalArgumentException ignored) {
          return noMatch();
        }
      }
      if (dataCenter == null) {
        return noMatch();
      }
      return new ScopeFilter(dataCenter, rack);
    }

    private static ScopeFilter noMatch() {
      return new ScopeFilter(null, null, true);
    }

    private static String decode(String value) {
      return URLDecoder.decode(value, StandardCharsets.UTF_8);
    }

    boolean matches(TopologyNode node) {
      return !matchesNothing
          && (dataCenter == null || dataCenter.equals(node.getDataCenter()))
          && (rack == null || rack.equals(node.getRack()));
    }
  }
}
