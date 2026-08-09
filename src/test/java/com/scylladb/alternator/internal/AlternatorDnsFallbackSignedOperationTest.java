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
import com.scylladb.alternator.queryplan.BasicQueryPlanInterceptor;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.http.conn.DnsResolver;
import org.junit.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.ListTablesResponse;

/** Signed DynamoDB operation coverage after addressed live-node discovery fallback. */
public class AlternatorDnsFallbackSignedOperationTest {

  @Test(timeout = 15000)
  public void testSignedOperationUsesLearnedLogicalHostAfterAddressFallback() throws Exception {
    InetAddress goodAddress = InetAddress.getByName("127.0.0.1");
    InetAddress badAddress = InetAddress.getByName("127.0.0.2");
    AtomicInteger badPolls = new AtomicInteger();
    AtomicInteger goodPolls = new AtomicInteger();
    AtomicInteger listTablesRequests = new AtomicInteger();
    AtomicReference<String> operationHost = new AtomicReference<>();
    AtomicReference<String> authorization = new AtomicReference<>();
    AtomicReference<String> amzDate = new AtomicReference<>();
    AtomicReference<String> amzTarget = new AtomicReference<>();

    HttpServer goodServer = HttpServer.create(new InetSocketAddress(goodAddress, 0), 0);
    int port = goodServer.getAddress().getPort();
    goodServer.createContext(
        "/localnodes",
        exchange -> {
          goodPolls.incrementAndGet();
          respond(exchange, 200, "[\"learned.test\"]");
        });
    goodServer.createContext(
        "/",
        exchange -> {
          drain(exchange.getRequestBody());
          listTablesRequests.incrementAndGet();
          operationHost.set(exchange.getRequestHeaders().getFirst("Host"));
          authorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
          amzDate.set(exchange.getRequestHeaders().getFirst("X-Amz-Date"));
          amzTarget.set(exchange.getRequestHeaders().getFirst("X-Amz-Target"));
          exchange.getResponseHeaders().set("Content-Type", "application/x-amz-json-1.0");
          respond(exchange, 200, "{\"TableNames\":[\"signed_table\"]}");
        });
    HttpServer badServer = HttpServer.create(new InetSocketAddress(badAddress, port), 0);
    badServer.createContext(
        "/localnodes",
        exchange -> {
          badPolls.incrementAndGet();
          respond(exchange, 503, "temporarily unavailable");
        });
    goodServer.start();
    badServer.start();

    DnsResolver pollingResolver = hostname -> new InetAddress[] {badAddress, goodAddress};
    SdkHttpClient pollingClient =
        ApacheSyncClientFactory.createPollingClient(null, pollingResolver);
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withSeedHost("seed.test")
            .withScheme("http")
            .withPort(port)
            .build();
    AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, pollingClient);
    SdkHttpClient mainHttpClient =
        ApacheHttpClient.builder().dnsResolver(hostname -> new InetAddress[] {goodAddress}).build();
    DynamoDbClient dynamoDb = null;
    try {
      liveNodes.updateLiveNodes();

      assertEquals("learned.test", liveNodes.nextAsURI().getHost());
      assertEquals(1, badPolls.get());
      assertEquals(1, goodPolls.get());

      ClientOverrideConfiguration overrides =
          ClientOverrideConfiguration.builder()
              .addExecutionInterceptor(new BasicQueryPlanInterceptor(liveNodes))
              .build();
      dynamoDb =
          DynamoDbClient.builder()
              .endpointOverride(URI.create("http://seed.test:" + port))
              .httpClient(mainHttpClient)
              .credentialsProvider(
                  StaticCredentialsProvider.create(
                      AwsBasicCredentials.create("test-access-key", "test-secret-key")))
              .region(Region.US_EAST_1)
              .overrideConfiguration(overrides)
              .build();

      ListTablesResponse response = dynamoDb.listTables();

      assertEquals(1, listTablesRequests.get());
      assertEquals("signed_table", response.tableNames().get(0));
      assertEquals("learned.test:" + port, operationHost.get());
      assertNotNull(amzDate.get());
      assertEquals("DynamoDB_20120810.ListTables", amzTarget.get());
      assertNotNull(authorization.get());
      assertTrue(authorization.get().startsWith("AWS4-HMAC-SHA256 Credential=test-access-key/"));
      assertTrue(authorization.get().contains("SignedHeaders="));
    } finally {
      if (dynamoDb != null) {
        dynamoDb.close();
      }
      mainHttpClient.close();
      pollingClient.close();
      goodServer.stop(0);
      badServer.stop(0);
    }
  }

  private static void drain(InputStream input) throws IOException {
    try (InputStream body = input) {
      byte[] buffer = new byte[1024];
      while (body.read(buffer) >= 0) {
        // Consume the signed request before returning the modeled DynamoDB response.
      }
    }
  }

  private static void respond(HttpExchange exchange, int status, String response)
      throws IOException {
    byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
    exchange.sendResponseHeaders(status, bytes.length);
    try (OutputStream output = exchange.getResponseBody()) {
      output.write(bytes);
    }
  }
}
