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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import com.scylladb.alternator.AlternatorConfig;
import com.scylladb.alternator.routing.ClusterScope;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.junit.Test;
import software.amazon.awssdk.http.SdkHttpClient;

/** Socket-level coverage for the synchronous transport used by LiveNodes polling. */
public class AlternatorLiveNodesSocketLifecycleTest {
  private static final long REUSE_IDLE_TIME_MS = 60_000;
  private static final long SHORT_IDLE_TIME_MS = 25;
  private static final long EXPIRY_WAIT_MS = 200;
  private static final List<String> SIX_LOOPBACK_ROUTES =
      Arrays.asList("127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.4", "127.0.0.5", "127.0.0.6");

  @Test(timeout = 30_000)
  public void pollingConnectionIdentityTracksShortIdleBoundary() throws Exception {
    assertPollingConnectionIdentity(REUSE_IDLE_TIME_MS, 0, false);
    assertPollingConnectionIdentity(SHORT_IDLE_TIME_MS, EXPIRY_WAIT_MS, true);
  }

  private void assertPollingConnectionIdentity(
      long idleTimeMs, long waitBetweenRequestsMs, boolean expectReplacement) throws Exception {
    try (SocketTrackingHttpServer server =
        new SocketTrackingHttpServer(
            request -> topologyResponse(request, Arrays.asList("127.0.0.1")))) {
      server.start();
      AlternatorConfig config =
          AlternatorConfig.builder()
              .withSeedNode(server.uri())
              .withRoutingScope(ClusterScope.create())
              .withNodeHealthDisabled()
              .withMaxConnections(1)
              .withConnectionMaxIdleTimeMs(idleTimeMs)
              .build();
      SdkHttpClient pollingClient = ApacheSyncClientFactory.create(null, config, null);
      AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, pollingClient);
      try {
        liveNodes.refreshDiscoveredNodes();
        if (waitBetweenRequestsMs > 0) {
          Thread.sleep(waitBetweenRequestsMs);
        }
        liveNodes.refreshDiscoveredNodes();

        List<SocketTrackingHttpServer.Request> requests = server.requestsSince(0);
        assertEquals(4, requests.size());
        if (expectReplacement) {
          assertNotEquals(
              "polling should replace a connection after the configured idle threshold",
              requests.get(0).remotePort(),
              requests.get(2).remotePort());
          assertEquals(2, server.acceptedConnections());
          assertEquals(2, server.uniqueRemotePorts().size());
        } else {
          assertEquals(
              "polling should reuse its physical connection before the idle threshold",
              requests.get(0).remotePort(),
              requests.get(2).remotePort());
          assertEquals(1, server.acceptedConnections());
          assertEquals(1, server.uniqueRemotePorts().size());
        }
        server.assertHealthy();
      } finally {
        try {
          liveNodes.shutdownAndWait();
        } finally {
          pollingClient.close();
        }
      }
    }
  }

  @Test(timeout = 30_000)
  public void topologyDiscoveryStopsAfterFirstReachableRoute() throws Exception {
    try (SocketTrackingHttpServer server =
        new SocketTrackingHttpServer(
            InetAddress.getByName("0.0.0.0"),
            request -> topologyResponse(request, SIX_LOOPBACK_ROUTES))) {
      server.start();
      SdkHttpClient pollingClient = ApacheSyncClientFactory.createPollingClient(null, 5);
      AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(pollingConfig(server), pollingClient);
      try {
        liveNodes.refreshDiscoveredNodes();
        assertEquals(6, liveNodes.getDiscoveredNodes().size());

        int requestsBeforeFullRefresh = server.requestCount();
        liveNodes.refreshDiscoveredNodes();
        List<SocketTrackingHttpServer.Request> fullRefresh =
            server.requestsSince(requestsBeforeFullRefresh);

        assertEquals("one topology read should scan local and peers", 2, fullRefresh.size());
        Set<String> destinations = new LinkedHashSet<>();
        for (SocketTrackingHttpServer.Request request : fullRefresh) {
          destinations.add(request.destinationAddress());
        }
        assertEquals(
            "one reachable topology endpoint should satisfy refresh", 1, destinations.size());
        assertTrue(SIX_LOOPBACK_ROUTES.contains(destinations.iterator().next()));
        server.assertHealthy();
      } finally {
        try {
          liveNodes.shutdownAndWait();
        } finally {
          pollingClient.close();
        }
      }
    }
  }

  private AlternatorConfig pollingConfig(SocketTrackingHttpServer server) {
    return AlternatorConfig.builder()
        .withSeedNode(server.uri())
        .withRoutingScope(ClusterScope.create())
        .withNodeHealthDisabled()
        .build();
  }

  private static SocketTrackingHttpServer.Response topologyResponse(
      SocketTrackingHttpServer.Request request, List<String> addresses) {
    String requestBody = new String(request.body(), StandardCharsets.UTF_8);
    boolean local = requestBody.contains(SystemTableTopologyDiscovery.LOCAL_TABLE);
    List<String> selected =
        local ? addresses.subList(0, 1) : addresses.subList(1, addresses.size());
    return SocketTrackingHttpServer.Response.json(scanResponse(selected));
  }

  private static String scanResponse(List<String> addresses) {
    StringBuilder response = new StringBuilder("{\"Items\":[");
    for (int i = 0; i < addresses.size(); i++) {
      if (i > 0) {
        response.append(',');
      }
      response
          .append("{\"rpc_address\":{\"S\":\"")
          .append(addresses.get(i))
          .append("\"},\"data_center\":{\"S\":\"dc1\"},\"rack\":{\"S\":\"rack1\"}}");
    }
    return response
        .append("],\"Count\":")
        .append(addresses.size())
        .append(",\"ScannedCount\":")
        .append(addresses.size())
        .append('}')
        .toString();
  }
}
