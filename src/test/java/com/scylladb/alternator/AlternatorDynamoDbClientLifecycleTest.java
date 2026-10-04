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
package com.scylladb.alternator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.scylladb.alternator.internal.AlternatorLiveNodes;
import com.scylladb.alternator.internal.AsyncClientDetector;
import com.scylladb.alternator.internal.SyncClientDetector;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpFullResponse;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.async.AsyncExecuteRequest;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.utils.AttributeMap;

/** Regression tests for resources owned by the Alternator client builders. */
public class AlternatorDynamoDbClientLifecycleTest {
  private static final URI SEED_URI = URI.create("http://127.0.0.1:8000");
  private static final String LOCAL_TABLE = ".scylla.alternator.system.local";
  private static final String PEERS_TABLE = ".scylla.alternator.system.peers";
  private static final String TEST_ACCESS_KEY = "topology-access-key";
  private static final Region TEST_REGION = Region.of("topology-test-region");
  private static final StaticCredentialsProvider TEST_CREDENTIALS =
      StaticCredentialsProvider.create(
          AwsBasicCredentials.create(TEST_ACCESS_KEY, "topology-secret-key"));

  @Test
  public void syncStandardClientCloseClosesInternalResourcesOnce() {
    TestSyncBuilder builder = new TestSyncBuilder();
    builder.endpointOverride(SEED_URI);

    DynamoDbClient client = builder.build();
    client.close();
    client.close();

    assertEquals(1, builder.mainClient.closeCount.get());
    assertEquals(1, builder.pollingClient.closeCount.get());
    assertEquals(1, builder.liveNodes.shutdownCount.get());
    assertEquals(
        Arrays.asList("live-nodes-start", "live-nodes-stop", "polling-client", "main-client"),
        builder.events);
  }

  @Test
  public void asyncStandardClientCloseClosesInternalResourcesOnce() {
    TestAsyncBuilder builder = new TestAsyncBuilder();
    builder.endpointOverride(SEED_URI);

    DynamoDbAsyncClient client = builder.build();
    client.close();
    client.close();

    assertEquals(1, builder.mainClient.closeCount.get());
    assertEquals(1, builder.pollingClient.closeCount.get());
    assertEquals(1, builder.liveNodes.shutdownCount.get());
    assertEquals(
        Arrays.asList("live-nodes-start", "live-nodes-stop", "polling-client", "main-client"),
        builder.events);
  }

  @Test
  public void syncCallerProvidedClientRemainsCallerOwned() {
    TestSyncBuilder builder = new TestSyncBuilder();
    TrackingSdkHttpClient callerClient = new TrackingSdkHttpClient("caller-client", builder.events);
    builder.endpointOverride(SEED_URI);
    builder.httpClient(callerClient);

    DynamoDbClient client = builder.build();
    client.close();

    assertFalse(builder.mainClientCreated);
    assertEquals(0, callerClient.closeCount.get());
    assertEquals(1, builder.pollingClient.closeCount.get());
    assertEquals(1, builder.liveNodes.shutdownCount.get());

    callerClient.close();
    assertEquals(1, callerClient.closeCount.get());
  }

  @Test
  public void asyncCallerProvidedClientRemainsCallerOwned() {
    TestAsyncBuilder builder = new TestAsyncBuilder();
    TrackingSdkAsyncHttpClient callerClient =
        new TrackingSdkAsyncHttpClient("caller-client", builder.events);
    builder.endpointOverride(SEED_URI);
    builder.httpClient(callerClient);

    DynamoDbAsyncClient client = builder.build();
    client.close();

    assertFalse(builder.mainClientCreated);
    assertEquals(0, callerClient.closeCount.get());
    assertEquals(1, builder.pollingClient.closeCount.get());
    assertEquals(1, builder.liveNodes.shutdownCount.get());

    callerClient.close();
    assertEquals(1, callerClient.closeCount.get());
  }

  @Test
  public void syncCallerProvidedBuilderCreatesOwnedClient() {
    TestSyncBuilder builder = new TestSyncBuilder();
    TrackingSdkHttpClient builtClient = new TrackingSdkHttpClient("built-client", builder.events);
    TrackingSdkHttpClientBuilder clientBuilder = new TrackingSdkHttpClientBuilder(builtClient);
    builder.endpointOverride(SEED_URI);
    builder.httpClientBuilder(clientBuilder);

    DynamoDbClient client = builder.build();
    client.close();

    assertFalse(builder.mainClientCreated);
    assertEquals(1, clientBuilder.buildCount.get());
    assertEquals(1, builtClient.closeCount.get());
    assertEquals(1, builder.pollingClient.closeCount.get());
    assertEquals(1, builder.liveNodes.shutdownCount.get());
  }

  @Test
  public void asyncCallerProvidedBuilderCreatesOwnedClient() {
    TestAsyncBuilder builder = new TestAsyncBuilder();
    TrackingSdkAsyncHttpClient builtClient =
        new TrackingSdkAsyncHttpClient("built-client", builder.events);
    TrackingSdkAsyncHttpClientBuilder clientBuilder =
        new TrackingSdkAsyncHttpClientBuilder(builtClient);
    builder.endpointOverride(SEED_URI);
    builder.httpClientBuilder(clientBuilder);

    DynamoDbAsyncClient client = builder.build();
    client.close();

    assertFalse(builder.mainClientCreated);
    assertEquals(1, clientBuilder.buildCount.get());
    assertEquals(1, builtClient.closeCount.get());
    assertEquals(1, builder.pollingClient.closeCount.get());
    assertEquals(1, builder.liveNodes.shutdownCount.get());
  }

  @Test
  public void wrapperAndServiceClientShareCleanup() {
    TestSyncBuilder syncBuilder = new TestSyncBuilder();
    syncBuilder.endpointOverride(SEED_URI);
    AlternatorDynamoDbClientWrapper syncWrapper = syncBuilder.buildWithAlternatorAPI();

    syncWrapper.getClient().close();
    syncWrapper.close();

    assertEquals(1, syncBuilder.mainClient.closeCount.get());
    assertEquals(1, syncBuilder.pollingClient.closeCount.get());
    assertEquals(1, syncBuilder.liveNodes.shutdownCount.get());

    TestAsyncBuilder asyncBuilder = new TestAsyncBuilder();
    asyncBuilder.endpointOverride(SEED_URI);
    AlternatorDynamoDbAsyncClientWrapper asyncWrapper = asyncBuilder.buildWithAlternatorAPI();

    asyncWrapper.getClient().close();
    asyncWrapper.close();

    assertEquals(1, asyncBuilder.mainClient.closeCount.get());
    assertEquals(1, asyncBuilder.pollingClient.closeCount.get());
    assertEquals(1, asyncBuilder.liveNodes.shutdownCount.get());
  }

  @Test(timeout = 5000)
  public void syncStandardClientCloseStopsPollingThread() throws Exception {
    RunningSyncBuilder builder = new RunningSyncBuilder();
    builder.endpointOverride(SEED_URI);
    builder.withNodeHealthDisabled();
    builder.withActiveRefreshIntervalMs(1000);
    builder.withIdleRefreshIntervalMs(1000);

    DynamoDbClient client = builder.build();
    assertTrue(builder.pollingClient.firstTopologyRefresh.await(1, TimeUnit.SECONDS));
    assertTrue(awaitDiscoveredNode(builder.liveNodes, URI.create("http://127.0.0.2:8000")));

    client.close();

    assertFalse(builder.liveNodes.isAlive());
    assertFalse(builder.liveNodes.isRunning());
    assertEquals(1, builder.pollingClient.closeCount.get());
    assertEquals(1, builder.mainClient.closeCount.get());
    assertPollingHasStopped(builder.pollingClient);
  }

  @Test(timeout = 5000)
  public void asyncStandardClientCloseStopsPollingThread() throws Exception {
    RunningAsyncBuilder builder = new RunningAsyncBuilder();
    builder.endpointOverride(SEED_URI);
    builder.withNodeHealthDisabled();
    builder.withActiveRefreshIntervalMs(1000);
    builder.withIdleRefreshIntervalMs(1000);

    DynamoDbAsyncClient client = builder.build();
    assertTrue(builder.pollingClient.firstTopologyRefresh.await(1, TimeUnit.SECONDS));
    assertTrue(awaitDiscoveredNode(builder.liveNodes, URI.create("http://127.0.0.2:8000")));

    client.close();

    assertFalse(builder.liveNodes.isAlive());
    assertFalse(builder.liveNodes.isRunning());
    assertEquals(1, builder.pollingClient.closeCount.get());
    assertEquals(1, builder.mainClient.closeCount.get());
    assertPollingHasStopped(builder.pollingClient);
  }

  @Test(timeout = 5000)
  public void syncTopologyScansUseConfiguredCredentialsAndRegion() throws Exception {
    RunningSyncBuilder builder = new RunningSyncBuilder();
    builder.endpointOverride(SEED_URI);
    builder.credentialsProvider(TEST_CREDENTIALS);
    builder.region(TEST_REGION);
    builder.withNodeHealthDisabled();
    builder.withActiveRefreshIntervalMs(60_000);
    builder.withIdleRefreshIntervalMs(60_000);

    DynamoDbClient client = builder.build();
    try {
      assertTrue(builder.pollingClient.firstTopologyRefresh.await(1, TimeUnit.SECONDS));
      assertTrue(awaitDiscoveredNode(builder.liveNodes, URI.create("http://127.0.0.2:8000")));
      assertSignedTopologyScans(builder.pollingClient);
    } finally {
      client.close();
    }
  }

  @Test(timeout = 5000)
  public void asyncTopologyScansUseConfiguredCredentialsAndRegion() throws Exception {
    RunningAsyncBuilder builder = new RunningAsyncBuilder();
    builder.endpointOverride(SEED_URI);
    builder.credentialsProvider(TEST_CREDENTIALS);
    builder.region(TEST_REGION);
    builder.withNodeHealthDisabled();
    builder.withActiveRefreshIntervalMs(60_000);
    builder.withIdleRefreshIntervalMs(60_000);

    DynamoDbAsyncClient client = builder.build();
    try {
      assertTrue(builder.pollingClient.firstTopologyRefresh.await(1, TimeUnit.SECONDS));
      assertTrue(awaitDiscoveredNode(builder.liveNodes, URI.create("http://127.0.0.2:8000")));
      assertSignedTopologyScans(builder.pollingClient);
    } finally {
      client.close();
    }
  }

  @Test
  public void syncBuildFailureClosesAlreadyCreatedResources() {
    TestSyncBuilder builder = new TestSyncBuilder();
    TrackingSdkHttpClient builtClient =
        new TrackingSdkHttpClient("built-client", builder.events, true);
    builder.endpointOverride(SEED_URI);
    builder.httpClientBuilder(new TrackingSdkHttpClientBuilder(builtClient));

    try {
      builder.build();
      fail("Expected client construction to fail");
    } catch (IllegalStateException expected) {
      assertEquals("clientName failed", expected.getMessage());
    }

    assertEquals(1, builtClient.closeCount.get());
    assertEquals(1, builder.pollingClient.closeCount.get());
    assertEquals(1, builder.liveNodes.shutdownCount.get());
  }

  @Test
  public void asyncBuildFailureClosesAlreadyCreatedResources() {
    TestAsyncBuilder builder = new TestAsyncBuilder();
    TrackingSdkAsyncHttpClient builtClient =
        new TrackingSdkAsyncHttpClient("built-client", builder.events, true);
    builder.endpointOverride(SEED_URI);
    builder.httpClientBuilder(new TrackingSdkAsyncHttpClientBuilder(builtClient));

    try {
      builder.build();
      fail("Expected client construction to fail");
    } catch (IllegalStateException expected) {
      assertEquals("clientName failed", expected.getMessage());
    }

    assertEquals(1, builtClient.closeCount.get());
    assertEquals(1, builder.pollingClient.closeCount.get());
    assertEquals(1, builder.liveNodes.shutdownCount.get());
  }

  @Test
  public void syncBuildFailureDoesNotCloseCallerProvidedClient() {
    TestSyncBuilder builder = new TestSyncBuilder();
    TrackingSdkHttpClient callerClient =
        new TrackingSdkHttpClient("caller-client", builder.events, true);
    builder.endpointOverride(SEED_URI);
    builder.httpClient(callerClient);

    try {
      builder.build();
      fail("Expected client construction to fail");
    } catch (IllegalStateException expected) {
      assertEquals("clientName failed", expected.getMessage());
    }

    assertEquals(0, callerClient.closeCount.get());
    assertEquals(1, builder.pollingClient.closeCount.get());
    assertEquals(1, builder.liveNodes.shutdownCount.get());
  }

  @Test
  public void asyncBuildFailureDoesNotCloseCallerProvidedClient() {
    TestAsyncBuilder builder = new TestAsyncBuilder();
    TrackingSdkAsyncHttpClient callerClient =
        new TrackingSdkAsyncHttpClient("caller-client", builder.events, true);
    builder.endpointOverride(SEED_URI);
    builder.httpClient(callerClient);

    try {
      builder.build();
      fail("Expected client construction to fail");
    } catch (IllegalStateException expected) {
      assertEquals("clientName failed", expected.getMessage());
    }

    assertEquals(0, callerClient.closeCount.get());
    assertEquals(1, builder.pollingClient.closeCount.get());
    assertEquals(1, builder.liveNodes.shutdownCount.get());
  }

  private static void assertPollingHasStopped(SuccessfulPollingClient pollingClient)
      throws InterruptedException {
    int requestsAfterClose = pollingClient.requestCount.get();
    Thread.sleep(50);
    assertEquals(requestsAfterClose, pollingClient.requestCount.get());
  }

  private static void assertSignedTopologyScans(SuccessfulPollingClient pollingClient) {
    assertTrue(pollingClient.requests.size() >= 2);
    assertTrue(pollingClient.requestBodies.size() >= 2);

    SdkHttpRequest localRequest = pollingClient.requests.get(0);
    SdkHttpRequest peersRequest = pollingClient.requests.get(1);
    assertEquals(SdkHttpMethod.POST, localRequest.method());
    assertEquals(SdkHttpMethod.POST, peersRequest.method());
    assertEquals("DynamoDB_20120810.Scan", localRequest.firstMatchingHeader("X-Amz-Target").get());
    assertEquals("DynamoDB_20120810.Scan", peersRequest.firstMatchingHeader("X-Amz-Target").get());
    assertTrue(pollingClient.requestBodies.get(0).contains(LOCAL_TABLE));
    assertTrue(pollingClient.requestBodies.get(1).contains(PEERS_TABLE));

    for (SdkHttpRequest request : Arrays.asList(localRequest, peersRequest)) {
      String authorization = request.firstMatchingHeader("Authorization").get();
      assertTrue(authorization.contains("Credential=" + TEST_ACCESS_KEY + "/"));
      assertTrue(authorization.contains("/" + TEST_REGION.id() + "/dynamodb/aws4_request"));
    }
  }

  private static boolean awaitDiscoveredNode(AlternatorLiveNodes liveNodes, URI node)
      throws InterruptedException {
    long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
    while (System.nanoTime() < deadlineNanos) {
      if (liveNodes.getDiscoveredNodes().contains(node)) {
        return true;
      }
      Thread.sleep(5);
    }
    return liveNodes.getDiscoveredNodes().contains(node);
  }

  private static final class TestSyncBuilder
      extends AlternatorDynamoDbClient.AlternatorDynamoDbClientBuilder {
    private final List<String> events = new ArrayList<>();
    private final TrackingSdkHttpClient mainClient =
        new TrackingSdkHttpClient("main-client", events);
    private final TrackingSdkHttpClient pollingClient =
        new TrackingSdkHttpClient("polling-client", events);
    private boolean mainClientCreated;
    private TrackingLiveNodes liveNodes;

    @Override
    SdkHttpClient createMainSyncClient(
        SyncClientDetector.SyncClientType clientType,
        AlternatorConfig config,
        TlsConfig tlsConfig) {
      mainClientCreated = true;
      return mainClient;
    }

    @Override
    SdkHttpClient createPollingClient(
        SyncClientDetector.SyncClientType clientType, TlsConfig tlsConfig, int maxConnections) {
      return pollingClient;
    }

    @Override
    AlternatorLiveNodes createLiveNodes(
        AlternatorConfig alternatorConfig, SdkHttpClient pollingClient) {
      liveNodes = new TrackingLiveNodes(alternatorConfig, pollingClient, events);
      return liveNodes;
    }
  }

  private static final class TestAsyncBuilder
      extends AlternatorDynamoDbAsyncClient.AlternatorDynamoDbAsyncClientBuilder {
    private final List<String> events = new ArrayList<>();
    private final TrackingSdkAsyncHttpClient mainClient =
        new TrackingSdkAsyncHttpClient("main-client", events);
    private final TrackingSdkHttpClient pollingClient =
        new TrackingSdkHttpClient("polling-client", events);
    private boolean mainClientCreated;
    private TrackingLiveNodes liveNodes;

    @Override
    SdkAsyncHttpClient createMainAsyncClient(
        AsyncClientDetector.AsyncClientType clientType,
        AlternatorConfig config,
        TlsConfig tlsConfig) {
      mainClientCreated = true;
      return mainClient;
    }

    @Override
    SdkHttpClient createPollingClient(
        SyncClientDetector.SyncClientType clientType, TlsConfig tlsConfig, int maxConnections) {
      return pollingClient;
    }

    @Override
    AlternatorLiveNodes createLiveNodes(
        AlternatorConfig alternatorConfig, SdkHttpClient pollingClient) {
      liveNodes = new TrackingLiveNodes(alternatorConfig, pollingClient, events);
      return liveNodes;
    }
  }

  private static final class RunningSyncBuilder
      extends AlternatorDynamoDbClient.AlternatorDynamoDbClientBuilder {
    private final List<String> events = new ArrayList<>();
    private final TrackingSdkHttpClient mainClient =
        new TrackingSdkHttpClient("main-client", events);
    private final SuccessfulPollingClient pollingClient = new SuccessfulPollingClient();
    private AlternatorLiveNodes liveNodes;

    @Override
    SdkHttpClient createMainSyncClient(
        SyncClientDetector.SyncClientType clientType,
        AlternatorConfig config,
        TlsConfig tlsConfig) {
      return mainClient;
    }

    @Override
    SdkHttpClient createPollingClient(
        SyncClientDetector.SyncClientType clientType, TlsConfig tlsConfig, int maxConnections) {
      return pollingClient;
    }

    @Override
    AlternatorLiveNodes createLiveNodes(
        AlternatorConfig alternatorConfig, SdkHttpClient pollingClient) {
      liveNodes = super.createLiveNodes(alternatorConfig, pollingClient);
      return liveNodes;
    }
  }

  private static final class RunningAsyncBuilder
      extends AlternatorDynamoDbAsyncClient.AlternatorDynamoDbAsyncClientBuilder {
    private final List<String> events = new ArrayList<>();
    private final TrackingSdkAsyncHttpClient mainClient =
        new TrackingSdkAsyncHttpClient("main-client", events);
    private final SuccessfulPollingClient pollingClient = new SuccessfulPollingClient();
    private AlternatorLiveNodes liveNodes;

    @Override
    SdkAsyncHttpClient createMainAsyncClient(
        AsyncClientDetector.AsyncClientType clientType,
        AlternatorConfig config,
        TlsConfig tlsConfig) {
      return mainClient;
    }

    @Override
    SdkHttpClient createPollingClient(
        SyncClientDetector.SyncClientType clientType, TlsConfig tlsConfig, int maxConnections) {
      return pollingClient;
    }

    @Override
    AlternatorLiveNodes createLiveNodes(
        AlternatorConfig alternatorConfig, SdkHttpClient pollingClient) {
      liveNodes = super.createLiveNodes(alternatorConfig, pollingClient);
      return liveNodes;
    }
  }

  private static final class TrackingLiveNodes extends AlternatorLiveNodes {
    private final List<String> events;
    private final AtomicInteger shutdownCount = new AtomicInteger();

    private TrackingLiveNodes(
        AlternatorConfig config, SdkHttpClient pollingClient, List<String> events) {
      super(config, pollingClient);
      this.events = events;
    }

    @Override
    public synchronized void start() {
      events.add("live-nodes-start");
    }

    @Override
    public boolean shutdownAndWait() {
      shutdownCount.incrementAndGet();
      events.add("live-nodes-stop");
      return super.shutdownAndWait();
    }
  }

  private static final class TrackingSdkHttpClient implements SdkHttpClient {
    private final String name;
    private final List<String> events;
    private final boolean failClientName;
    private final AtomicInteger closeCount = new AtomicInteger();

    private TrackingSdkHttpClient(String name, List<String> events) {
      this(name, events, false);
    }

    private TrackingSdkHttpClient(String name, List<String> events, boolean failClientName) {
      this.name = name;
      this.events = events;
      this.failClientName = failClientName;
    }

    @Override
    public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
      closeCount.incrementAndGet();
      events.add(name);
    }

    @Override
    public String clientName() {
      if (failClientName) {
        throw new IllegalStateException("clientName failed");
      }
      return name;
    }
  }

  private static final class TrackingSdkAsyncHttpClient implements SdkAsyncHttpClient {
    private final String name;
    private final List<String> events;
    private final boolean failClientName;
    private final AtomicInteger closeCount = new AtomicInteger();

    private TrackingSdkAsyncHttpClient(String name, List<String> events) {
      this(name, events, false);
    }

    private TrackingSdkAsyncHttpClient(String name, List<String> events, boolean failClientName) {
      this.name = name;
      this.events = events;
      this.failClientName = failClientName;
    }

    @Override
    public CompletableFuture<Void> execute(AsyncExecuteRequest request) {
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public void close() {
      closeCount.incrementAndGet();
      events.add(name);
    }

    @Override
    public String clientName() {
      if (failClientName) {
        throw new IllegalStateException("clientName failed");
      }
      return name;
    }
  }

  private static final class SuccessfulPollingClient implements SdkHttpClient {
    private final CountDownLatch firstTopologyRefresh = new CountDownLatch(2);
    private final AtomicInteger requestCount = new AtomicInteger();
    private final AtomicInteger closeCount = new AtomicInteger();
    private final List<SdkHttpRequest> requests = new CopyOnWriteArrayList<>();
    private final List<String> requestBodies = new CopyOnWriteArrayList<>();

    @Override
    public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
      SdkHttpRequest httpRequest = request.httpRequest();
      String requestBody = readRequestBody(request);
      return new ExecutableHttpRequest() {
        @Override
        public HttpExecuteResponse call() {
          requests.add(httpRequest);
          requestBodies.add(requestBody);
          requestCount.incrementAndGet();
          firstTopologyRefresh.countDown();
          String responseBody =
              requestBody.contains(LOCAL_TABLE)
                  ? localScanResponse("127.0.0.1")
                  : localScanResponse("127.0.0.2");
          byte[] body = responseBody.getBytes(StandardCharsets.UTF_8);
          return HttpExecuteResponse.builder()
              .response(
                  SdkHttpFullResponse.builder()
                      .statusCode(200)
                      .putHeader("Content-Type", "application/x-amz-json-1.0")
                      .putHeader("Content-Length", String.valueOf(body.length))
                      .build())
              .responseBody(AbortableInputStream.create(new ByteArrayInputStream(body)))
              .build();
        }

        @Override
        public void abort() {}
      };
    }

    @Override
    public void close() {
      closeCount.incrementAndGet();
    }

    @Override
    public String clientName() {
      return "successful-polling";
    }

    private static String readRequestBody(HttpExecuteRequest request) {
      if (!request.contentStreamProvider().isPresent()) {
        return "";
      }
      try (InputStream input = request.contentStreamProvider().get().newStream();
          ByteArrayOutputStream output = new ByteArrayOutputStream()) {
        byte[] buffer = new byte[256];
        int read;
        while ((read = input.read(buffer)) != -1) {
          output.write(buffer, 0, read);
        }
        return new String(output.toByteArray(), StandardCharsets.UTF_8);
      } catch (IOException e) {
        throw new IllegalStateException("failed to read topology request", e);
      }
    }

    private static String localScanResponse(String address) {
      return "{\"Items\":[{\"rpc_address\":{\"S\":\""
          + address
          + "\"},\"data_center\":{\"S\":\"dc1\"},\"rack\":{\"S\":\"rack1\"}}],"
          + "\"Count\":1,\"ScannedCount\":1}";
    }
  }

  private static final class TrackingSdkHttpClientBuilder
      implements SdkHttpClient.Builder<TrackingSdkHttpClientBuilder> {
    private final TrackingSdkHttpClient client;
    private final AtomicInteger buildCount = new AtomicInteger();

    private TrackingSdkHttpClientBuilder(TrackingSdkHttpClient client) {
      this.client = client;
    }

    @Override
    public SdkHttpClient build() {
      buildCount.incrementAndGet();
      return client;
    }

    @Override
    public SdkHttpClient buildWithDefaults(AttributeMap serviceDefaults) {
      buildCount.incrementAndGet();
      return client;
    }
  }

  private static final class TrackingSdkAsyncHttpClientBuilder
      implements SdkAsyncHttpClient.Builder<TrackingSdkAsyncHttpClientBuilder> {
    private final TrackingSdkAsyncHttpClient client;
    private final AtomicInteger buildCount = new AtomicInteger();

    private TrackingSdkAsyncHttpClientBuilder(TrackingSdkAsyncHttpClient client) {
      this.client = client;
    }

    @Override
    public SdkAsyncHttpClient build() {
      buildCount.incrementAndGet();
      return client;
    }

    @Override
    public SdkAsyncHttpClient buildWithDefaults(AttributeMap serviceDefaults) {
      buildCount.incrementAndGet();
      return client;
    }
  }
}
