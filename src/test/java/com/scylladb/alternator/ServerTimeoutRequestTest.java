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
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpFullResponse;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.async.AsyncExecuteRequest;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.services.dynamodb.model.ListTablesRequest;

/** Tests the server-timeout request option through the real AWS SDK request pipeline. */
public class ServerTimeoutRequestTest {
  private static final URI SEED_URI = URI.create("http://127.0.0.1:8000");
  private static final String SEED_HOST = "127.0.0.1";

  @Test
  @CoversRequirements("TIMEOUT-REQ-001")
  public void timeoutIsIsolatedPerRequest() {
    CapturingSdkHttpClient transport = new CapturingSdkHttpClient(false);

    try (AlternatorDynamoDbClientWrapper client = syncClient(transport, false, false)) {
      client
          .getClient()
          .listTables(
              ListTablesRequest.builder()
                  .overrideConfiguration(
                      builder ->
                          builder.putHeader(AlternatorRequestOptions.SERVER_TIMEOUT_HEADER, "5s"))
                  .build());
      client
          .getClient()
          .listTables(
              ListTablesRequest.builder()
                  .overrideConfiguration(
                      AlternatorRequestOptions.serverTimeout(Duration.ofMillis(1500)))
                  .build());
      client.getClient().listTables(ListTablesRequest.builder().build());
      client
          .getClient()
          .listTables(
              ListTablesRequest.builder()
                  .overrideConfiguration(AlternatorRequestOptions.serverTimeout(Duration.ZERO))
                  .build());

      AwsRequestOverrideConfiguration.Builder replacement =
          AwsRequestOverrideConfiguration.builder().putHeader("x-scylla-timeout", "1ms");
      AlternatorRequestOptions.serverTimeout(Duration.ofMillis(3)).accept(replacement);
      client
          .getClient()
          .listTables(
              ListTablesRequest.builder().overrideConfiguration(replacement.build()).build());
    }

    assertEquals(5, transport.requests.size());
    assertEquals(Optional.of("5s"), timeoutHeader(transport.requests.get(0)));
    assertEquals(Optional.of("1500ms"), timeoutHeader(transport.requests.get(1)));
    assertEquals(Optional.empty(), timeoutHeader(transport.requests.get(2)));
    assertEquals(Optional.of("0ms"), timeoutHeader(transport.requests.get(3)));
    assertEquals(Optional.of("3ms"), timeoutHeader(transport.requests.get(4)));
  }

  @Test
  @CoversRequirements("TIMEOUT-REQ-006")
  public void asyncTimeoutMatchesSync() {
    CapturingSdkHttpClient syncTransport = new CapturingSdkHttpClient(false);
    CapturingSdkAsyncHttpClient asyncTransport = new CapturingSdkAsyncHttpClient(false);
    ListTablesRequest request =
        ListTablesRequest.builder()
            .overrideConfiguration(AlternatorRequestOptions.serverTimeout(Duration.ofMillis(1500)))
            .build();

    try (AlternatorDynamoDbClientWrapper client = syncClient(syncTransport, false, false)) {
      client.getClient().listTables(request);
    }
    try (AlternatorDynamoDbAsyncClientWrapper client = asyncClient(asyncTransport, false)) {
      client.getClient().listTables(request).join();
    }

    assertEquals(1, syncTransport.requests.size());
    assertEquals(1, asyncTransport.requests.size());
    assertEquals(
        timeoutHeader(syncTransport.requests.get(0)),
        timeoutHeader(asyncTransport.requests.get(0)));
  }

  @Test
  @CoversRequirements("TIMEOUT-REQ-004")
  public void retryAttemptsPreserveServerTimeout() {
    CapturingSdkHttpClient syncTransport = new CapturingSdkHttpClient(true);
    CapturingSdkAsyncHttpClient asyncTransport = new CapturingSdkAsyncHttpClient(true);
    Duration clientCallTimeout = Duration.ofSeconds(30);
    Duration clientAttemptTimeout = Duration.ofSeconds(10);
    ListTablesRequest request =
        ListTablesRequest.builder()
            .overrideConfiguration(
                options -> {
                  options
                      .apiCallTimeout(clientCallTimeout)
                      .apiCallAttemptTimeout(clientAttemptTimeout);
                  AlternatorRequestOptions.serverTimeout(Duration.ofMillis(2300)).accept(options);
                })
            .build();

    assertEquals(clientCallTimeout, request.overrideConfiguration().get().apiCallTimeout().get());
    assertEquals(
        clientAttemptTimeout, request.overrideConfiguration().get().apiCallAttemptTimeout().get());

    try (AlternatorDynamoDbClientWrapper client = syncClient(syncTransport, false, true)) {
      client.getClient().listTables(request);
    }
    try (AlternatorDynamoDbAsyncClientWrapper client = asyncClient(asyncTransport, true)) {
      client.getClient().listTables(request).join();
    }

    assertTimeoutOnBothAttempts(syncTransport.requests);
    assertTimeoutOnBothAttempts(asyncTransport.requests);
  }

  @Test
  @CoversRequirements("TIMEOUT-REQ-005")
  public void syncTimeoutSurvivesHeaderOptimizationAndSigning() {
    CapturingSdkHttpClient transport = new CapturingSdkHttpClient(false);

    try (AlternatorDynamoDbClientWrapper client = syncClient(transport, true, false)) {
      client
          .getClient()
          .listTables(
              ListTablesRequest.builder()
                  .overrideConfiguration(
                      AlternatorRequestOptions.serverTimeout(Duration.ofMillis(1500)))
                  .build());
    }

    assertEquals(1, transport.requests.size());
    SdkHttpRequest request = transport.requests.get(0);
    assertEquals(Optional.of("1500ms"), timeoutHeader(request));
    assertEquals(Optional.of("gzip"), request.firstMatchingHeader("Content-Encoding"));

    String authorization =
        request.firstMatchingHeader("Authorization").orElseThrow(AssertionError::new);
    String lowercaseAuthorization = authorization.toLowerCase(Locale.ROOT);
    int signedHeadersStart = lowercaseAuthorization.indexOf("signedheaders=");
    assertTrue("Authorization must contain SignedHeaders", signedHeadersStart >= 0);
    int signedHeadersEnd = lowercaseAuthorization.indexOf(',', signedHeadersStart);
    assertTrue(
        "SignedHeaders must be followed by Signature", signedHeadersEnd > signedHeadersStart);
    String signedHeaders =
        lowercaseAuthorization.substring(
            signedHeadersStart + "signedheaders=".length(), signedHeadersEnd);
    assertTrue(
        "server timeout must be signed",
        List.of(signedHeaders.split(";"))
            .contains(AlternatorRequestOptions.SERVER_TIMEOUT_HEADER.toLowerCase(Locale.ROOT)));
  }

  private AlternatorDynamoDbClientWrapper syncClient(
      SdkHttpClient transport, boolean optimizeHeaders, boolean retryOnce) {
    AlternatorDynamoDbClient.AlternatorDynamoDbClientBuilder builder =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(SEED_URI)
            .withSeedHost(SEED_HOST)
            .withNodeHealthDisabled()
            .withOptimizeHeaders(optimizeHeaders)
            .credentialsProvider(testCredentials())
            .httpClient(transport);
    if (retryOnce) {
      builder.overrideConfiguration(retryConfiguration());
    }
    if (optimizeHeaders) {
      builder
          .withCompressionAlgorithm(RequestCompressionAlgorithm.GZIP)
          .withMinCompressionSizeBytes(0)
          .withResponseCompression(ResponseCompressionAlgorithm.GZIP);
    }
    return builder.buildWithAlternatorAPI();
  }

  private AlternatorDynamoDbAsyncClientWrapper asyncClient(
      SdkAsyncHttpClient transport, boolean retryOnce) {
    AlternatorDynamoDbAsyncClient.AlternatorDynamoDbAsyncClientBuilder builder =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(SEED_URI)
            .withSeedHost(SEED_HOST)
            .withNodeHealthDisabled()
            .credentialsProvider(testCredentials())
            .httpClient(transport);
    if (retryOnce) {
      builder.overrideConfiguration(retryConfiguration());
    }
    return builder.buildWithAlternatorAPI();
  }

  private ClientOverrideConfiguration retryConfiguration() {
    return ClientOverrideConfiguration.builder()
        .retryPolicy(RetryPolicy.builder().numRetries(1).build())
        .build();
  }

  private StaticCredentialsProvider testCredentials() {
    return StaticCredentialsProvider.create(AwsBasicCredentials.create("access-key", "secret-key"));
  }

  private Optional<String> timeoutHeader(SdkHttpRequest request) {
    return request.firstMatchingHeader(AlternatorRequestOptions.SERVER_TIMEOUT_HEADER);
  }

  private void assertTimeoutOnBothAttempts(List<SdkHttpRequest> requests) {
    assertEquals(2, requests.size());
    assertEquals(Optional.of("2300ms"), timeoutHeader(requests.get(0)));
    assertEquals(Optional.of("2300ms"), timeoutHeader(requests.get(1)));
  }

  private static final class CapturingSdkHttpClient implements SdkHttpClient {
    private final boolean retryFirstRequest;
    private final List<SdkHttpRequest> requests = new CopyOnWriteArrayList<>();

    private CapturingSdkHttpClient(boolean retryFirstRequest) {
      this.retryFirstRequest = retryFirstRequest;
    }

    @Override
    public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
      requests.add(request.httpRequest());
      int attempt = requests.size();
      return new ExecutableHttpRequest() {
        @Override
        public HttpExecuteResponse call() throws IOException {
          int statusCode = retryFirstRequest && attempt == 1 ? 500 : 200;
          byte[] body = responseBody(statusCode);
          return HttpExecuteResponse.builder()
              .response(response(statusCode, body.length))
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
      return "CapturingSdkHttpClient";
    }
  }

  private static final class CapturingSdkAsyncHttpClient implements SdkAsyncHttpClient {
    private final boolean retryFirstRequest;
    private final List<SdkHttpRequest> requests = new CopyOnWriteArrayList<>();

    private CapturingSdkAsyncHttpClient(boolean retryFirstRequest) {
      this.retryFirstRequest = retryFirstRequest;
    }

    @Override
    public CompletableFuture<Void> execute(AsyncExecuteRequest request) {
      requests.add(request.request());
      int statusCode = retryFirstRequest && requests.size() == 1 ? 500 : 200;
      byte[] body = responseBody(statusCode);
      request.responseHandler().onHeaders(response(statusCode, body.length));
      request.responseHandler().onStream(new ByteArrayPublisher(body));
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public void close() {}

    @Override
    public String clientName() {
      return "CapturingSdkAsyncHttpClient";
    }
  }

  private static SdkHttpFullResponse response(int statusCode, int contentLength) {
    return SdkHttpFullResponse.builder()
        .statusCode(statusCode)
        .putHeader("Content-Type", "application/x-amz-json-1.0")
        .putHeader("Content-Length", String.valueOf(contentLength))
        .build();
  }

  private static byte[] responseBody(int statusCode) {
    String body = statusCode == 500 ? "{\"message\":\"retry\"}" : "{}";
    return body.getBytes(StandardCharsets.UTF_8);
  }

  private static final class ByteArrayPublisher implements Publisher<ByteBuffer> {
    private final byte[] body;

    private ByteArrayPublisher(byte[] body) {
      this.body = body;
    }

    @Override
    public void subscribe(Subscriber<? super ByteBuffer> subscriber) {
      subscriber.onSubscribe(
          new Subscription() {
            private boolean complete;

            @Override
            public void request(long count) {
              if (!complete) {
                complete = true;
                subscriber.onNext(ByteBuffer.wrap(body));
                subscriber.onComplete();
              }
            }

            @Override
            public void cancel() {
              complete = true;
            }
          });
    }
  }
}
