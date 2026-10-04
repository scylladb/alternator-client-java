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
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.Test;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpFullResponse;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;

/**
 * Unit tests verifying that AlternatorLiveNodes sends its topology scans through the reusable
 * polling transport.
 */
public class AlternatorLiveNodesKeepAliveTest {

  /** Mock SdkHttpClient that captures topology scan requests and returns valid scan responses. */
  private static class CapturingHttpClient implements SdkHttpClient {
    final List<SdkHttpRequest> capturedRequests = new CopyOnWriteArrayList<>();
    final List<String> capturedBodies = new CopyOnWriteArrayList<>();

    @Override
    public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
      capturedRequests.add(request.httpRequest());
      String requestBody = requestBody(request);
      capturedBodies.add(requestBody);
      return new ExecutableHttpRequest() {
        @Override
        public HttpExecuteResponse call() {
          String responseBody =
              requestBody.contains(SystemTableTopologyDiscovery.LOCAL_TABLE)
                  ? localScanResponse("127.0.0.1")
                  : emptyScanResponse();
          byte[] body = responseBody.getBytes(StandardCharsets.UTF_8);
          return HttpExecuteResponse.builder()
              .response(
                  SdkHttpFullResponse.builder()
                      .statusCode(200)
                      .putHeader("Content-Type", "application/x-amz-json-1.0")
                      .build())
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
      return "CapturingHttpClient";
    }

    private static String requestBody(HttpExecuteRequest request) {
      try {
        return new String(
            request.contentStreamProvider().get().newStream().readAllBytes(),
            StandardCharsets.UTF_8);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Test
  public void testPollingUsesDynamoDbTopologyScans() throws Exception {
    CapturingHttpClient capturingClient = new CapturingHttpClient();
    AlternatorConfig config =
        AlternatorConfig.builder().withSeedNode(URI.create("http://127.0.0.1:8043")).build();
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, capturingClient);

    liveNodes.refreshDiscoveredNodes();

    assertTopologyScans(capturingClient, "http", 1);
  }

  @Test
  public void testMultiplePollingCyclesUseSameDynamoDbRequestShape() throws Exception {
    CapturingHttpClient capturingClient = new CapturingHttpClient();
    AlternatorConfig config =
        AlternatorConfig.builder().withSeedNode(URI.create("http://127.0.0.1:8043")).build();
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, capturingClient);

    for (int i = 0; i < 5; i++) {
      liveNodes.refreshDiscoveredNodes();
    }

    assertTopologyScans(capturingClient, "http", 5);
  }

  @Test
  public void testTopologyScansUseHttps() throws Exception {
    CapturingHttpClient capturingClient = new CapturingHttpClient();
    AlternatorConfig config =
        AlternatorConfig.builder().withSeedNode(URI.create("https://127.0.0.1:8043")).build();
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, capturingClient);

    liveNodes.refreshDiscoveredNodes();

    assertTopologyScans(capturingClient, "https", 1);
  }

  @Test
  public void testMultipleHttpsPollingCyclesUseSameDynamoDbRequestShape() throws Exception {
    CapturingHttpClient capturingClient = new CapturingHttpClient();
    AlternatorConfig config =
        AlternatorConfig.builder().withSeedNode(URI.create("https://127.0.0.1:8043")).build();
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, capturingClient);

    for (int i = 0; i < 5; i++) {
      liveNodes.refreshDiscoveredNodes();
    }

    assertTopologyScans(capturingClient, "https", 5);
  }

  private static void assertTopologyScans(
      CapturingHttpClient capturingClient, String expectedProtocol, int expectedCycles) {
    assertEquals(expectedCycles * 2, capturingClient.capturedRequests.size());
    assertEquals(capturingClient.capturedRequests.size(), capturingClient.capturedBodies.size());

    for (int i = 0; i < capturingClient.capturedRequests.size(); i++) {
      SdkHttpRequest request = capturingClient.capturedRequests.get(i);
      assertEquals(expectedProtocol, request.protocol());
      assertEquals(SdkHttpMethod.POST, request.method());
      assertEquals("/", request.encodedPath());
      assertTrue(
          "request should target DynamoDB Scan",
          request.headers().get("X-Amz-Target").contains("DynamoDB_20120810.Scan"));
      String expectedTable =
          i % 2 == 0
              ? SystemTableTopologyDiscovery.LOCAL_TABLE
              : SystemTableTopologyDiscovery.PEERS_TABLE;
      assertTrue(capturingClient.capturedBodies.get(i).contains(expectedTable));
    }
  }

  private static String localScanResponse(String address) {
    return "{\"Items\":[{\"rpc_address\":{\"S\":\""
        + address
        + "\"},\"data_center\":{\"S\":\"dc1\"},\"rack\":{\"S\":\"rack1\"}}],"
        + "\"Count\":1,\"ScannedCount\":1}";
  }

  private static String emptyScanResponse() {
    return "{\"Items\":[],\"Count\":0,\"ScannedCount\":0}";
  }
}
