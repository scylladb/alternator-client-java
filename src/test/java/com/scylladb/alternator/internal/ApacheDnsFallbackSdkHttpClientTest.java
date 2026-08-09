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

import java.io.ByteArrayInputStream;
import java.net.InetAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.http.conn.DnsResolver;
import org.junit.Test;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpFullResponse;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;

/** Lifecycle and logical-endpoint tests for Apache addressed polling requests. */
public class ApacheDnsFallbackSdkHttpClientTest {

  @Test
  public void testAddressRequestKeepsLogicalUriAndClosesOwnerWithBody() throws Exception {
    InetAddress address = InetAddress.getByName("192.0.2.1");
    TrackingClient baseClient = new TrackingClient();
    AtomicReference<TrackingClient> addressClient = new AtomicReference<>();
    DnsResolver resolver = hostname -> new InetAddress[] {address};
    ApacheDnsFallbackSdkHttpClient client =
        new ApacheDnsFallbackSdkHttpClient(
            baseClient,
            resolver,
            (hostname, resolvedAddress) -> {
              TrackingClient created = new TrackingClient();
              addressClient.set(created);
              return created;
            });
    SdkHttpRequest request = logicalRequest();

    HttpExecuteResponse response =
        client
            .prepareRequestForAddress(
                HttpExecuteRequest.builder().request(request).build(), address)
            .call();

    assertEquals(request.getUri(), addressClient.get().capturedRequest.getUri());
    assertEquals(
        "logical.test:8043", addressClient.get().capturedRequest.firstMatchingHeader("Host").get());
    assertEquals(
        "logical-signature",
        addressClient.get().capturedRequest.firstMatchingHeader("Authorization").get());
    assertFalse(addressClient.get().closed.get());

    response.responseBody().get().close();
    assertTrue(addressClient.get().closed.get());
    client.close();
    assertTrue(baseClient.closed.get());
  }

  @Test
  public void testClientCloseClosesActiveAddressRequest() throws Exception {
    InetAddress address = InetAddress.getByName("192.0.2.2");
    TrackingClient baseClient = new TrackingClient();
    AtomicReference<TrackingClient> addressClient = new AtomicReference<>();
    ApacheDnsFallbackSdkHttpClient client =
        new ApacheDnsFallbackSdkHttpClient(
            baseClient,
            hostname -> new InetAddress[] {address},
            (hostname, resolvedAddress) -> {
              TrackingClient created = new TrackingClient();
              addressClient.set(created);
              return created;
            });
    HttpExecuteResponse response =
        client
            .prepareRequestForAddress(
                HttpExecuteRequest.builder().request(logicalRequest()).build(), address)
            .call();

    client.close();

    assertTrue(baseClient.closed.get());
    assertTrue(addressClient.get().closed.get());
    response.responseBody().get().close();
  }

  @Test
  public void testCompletedAddressRequestsDoNotAccumulateClients() throws Exception {
    InetAddress address = InetAddress.getByName("192.0.2.3");
    TrackingClient baseClient = new TrackingClient();
    List<TrackingClient> addressClients = new ArrayList<>();
    ApacheDnsFallbackSdkHttpClient client =
        new ApacheDnsFallbackSdkHttpClient(
            baseClient,
            hostname -> new InetAddress[] {address},
            (hostname, resolvedAddress) -> {
              TrackingClient created = new TrackingClient();
              addressClients.add(created);
              return created;
            });

    for (int i = 0; i < 32; i++) {
      HttpExecuteResponse response =
          client
              .prepareRequestForAddress(
                  HttpExecuteRequest.builder().request(logicalRequest()).build(), address)
              .call();
      response.responseBody().get().close();
      assertTrue(
          "completed request must close its one-shot client", addressClients.get(i).closed.get());
    }

    assertEquals(32, addressClients.size());
    assertFalse(baseClient.closed.get());
    client.close();
    assertTrue(baseClient.closed.get());
  }

  private static SdkHttpRequest logicalRequest() {
    return SdkHttpRequest.builder()
        .uri(URI.create("https://logical.test:8043/localnodes?dc=dc1"))
        .method(SdkHttpMethod.GET)
        .putHeader("Host", "logical.test:8043")
        .putHeader("Authorization", "logical-signature")
        .build();
  }

  private static final class TrackingClient implements SdkHttpClient {
    final AtomicBoolean closed = new AtomicBoolean(false);
    SdkHttpRequest capturedRequest;

    @Override
    public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
      capturedRequest = request.httpRequest();
      return new ExecutableHttpRequest() {
        @Override
        public HttpExecuteResponse call() {
          return HttpExecuteResponse.builder()
              .response(SdkHttpFullResponse.builder().statusCode(200).build())
              .responseBody(AbortableInputStream.create(new ByteArrayInputStream("[]".getBytes())))
              .build();
        }

        @Override
        public void abort() {}
      };
    }

    @Override
    public void close() {
      closed.set(true);
    }

    @Override
    public String clientName() {
      return "TrackingClient";
    }
  }
}
