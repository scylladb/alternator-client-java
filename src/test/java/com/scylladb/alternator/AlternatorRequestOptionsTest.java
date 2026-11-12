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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.junit.Test;
import software.amazon.awssdk.awscore.AwsRequest;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbRequest;

public class AlternatorRequestOptionsTest {
  private static final long MAX_CQL_MILLISECONDS = Long.MAX_VALUE / 1_000_000L;

  @Test
  public void serverTimeoutEncodesCanonicalMilliseconds() {
    AwsRequestOverrideConfiguration configuration =
        apply(AlternatorRequestOptions.serverTimeout(Duration.ofSeconds(5)));

    assertEquals(
        Collections.singletonList("5000ms"),
        configuration.headers().get(AlternatorRequestOptions.SERVER_TIMEOUT_HEADER));
  }

  @Test
  public void serverTimeoutAllowsZero() {
    AwsRequestOverrideConfiguration configuration =
        apply(AlternatorRequestOptions.serverTimeout(Duration.ZERO));

    assertEquals(
        Collections.singletonList("0ms"),
        configuration.headers().get(AlternatorRequestOptions.SERVER_TIMEOUT_HEADER));
  }

  @Test
  public void serverTimeoutAllowsLargestWholeMillisecondCqlDuration() {
    AwsRequestOverrideConfiguration configuration =
        apply(AlternatorRequestOptions.serverTimeout(Duration.ofMillis(MAX_CQL_MILLISECONDS)));

    assertEquals(
        Collections.singletonList(MAX_CQL_MILLISECONDS + "ms"),
        configuration.headers().get(AlternatorRequestOptions.SERVER_TIMEOUT_HEADER));
  }

  @Test
  public void serverTimeoutPreservesUnrelatedOverrides() {
    Duration clientTimeout = Duration.ofSeconds(9);
    AwsRequestOverrideConfiguration.Builder builder =
        AwsRequestOverrideConfiguration.builder()
            .putHeader("X-Unrelated", "kept")
            .putRawQueryParameter("unrelated", "kept")
            .apiCallTimeout(clientTimeout);

    AlternatorRequestOptions.serverTimeout(Duration.ofMillis(25)).accept(builder);
    AwsRequestOverrideConfiguration configuration = builder.build();

    assertEquals(Collections.singletonList("kept"), configuration.headers().get("X-Unrelated"));
    assertEquals(
        Collections.singletonList("kept"), configuration.rawQueryParameters().get("unrelated"));
    assertEquals(clientTimeout, configuration.apiCallTimeout().get());
    assertEquals(
        Collections.singletonList("25ms"),
        configuration.headers().get(AlternatorRequestOptions.SERVER_TIMEOUT_HEADER));
  }

  @Test
  public void serverTimeoutReplacesExistingHeaderValues() {
    Map<String, List<String>> existingHeaders = new LinkedHashMap<>();
    existingHeaders.put("x-scylla-timeout", Arrays.asList("1ms", "2ms"));
    existingHeaders.put("X-Unrelated", Collections.singletonList("kept"));
    AwsRequestOverrideConfiguration.Builder builder =
        AwsRequestOverrideConfiguration.builder().headers(existingHeaders);

    AlternatorRequestOptions.serverTimeout(Duration.ofMillis(3)).accept(builder);

    assertEquals(2, builder.build().headers().size());
    assertEquals(
        Collections.singletonList("3ms"),
        builder.build().headers().get(AlternatorRequestOptions.SERVER_TIMEOUT_HEADER));
    assertEquals(Collections.singletonList("kept"), builder.build().headers().get("X-Unrelated"));
  }

  @Test
  @CoversRequirements("TIMEOUT-REQ-002")
  public void serverTimeoutRejectsInvalidDurations() {
    assertInvalidTimeout(null);
    assertInvalidTimeout(Duration.ofMillis(-1));
    assertInvalidTimeout(Duration.ofNanos(1));
    assertInvalidTimeout(Duration.ofNanos(999_999));
    assertInvalidTimeout(Duration.ofNanos(1_000_001));
    assertInvalidTimeout(Duration.ofMillis(MAX_CQL_MILLISECONDS + 1));
  }

  @Test
  @CoversRequirements("TIMEOUT-REQ-003")
  public void allDynamoDbOperationRequestModelsAcceptServerTimeout() throws Exception {
    Set<Class<? extends DynamoDbRequest>> syncRequests = operationRequests(DynamoDbClient.class);
    Set<Class<? extends DynamoDbRequest>> asyncRequests =
        operationRequests(DynamoDbAsyncClient.class);

    assertFalse(syncRequests.isEmpty());
    assertEquals(syncRequests, asyncRequests);

    for (Class<? extends DynamoDbRequest> requestType : syncRequests) {
      Method builderMethod = requestType.getMethod("builder");
      AwsRequest.Builder builder = (AwsRequest.Builder) builderMethod.invoke(null);
      builder.overrideConfiguration(
          AlternatorRequestOptions.serverTimeout(Duration.ofMillis(1750)));
      AwsRequest request = builder.build();

      assertTrue(request.overrideConfiguration().isPresent());
      assertEquals(
          Collections.singletonList("1750ms"),
          request
              .overrideConfiguration()
              .get()
              .headers()
              .get(AlternatorRequestOptions.SERVER_TIMEOUT_HEADER));
    }
  }

  private static AwsRequestOverrideConfiguration apply(
      Consumer<AwsRequestOverrideConfiguration.Builder> override) {
    AwsRequestOverrideConfiguration.Builder builder = AwsRequestOverrideConfiguration.builder();
    override.accept(builder);
    return builder.build();
  }

  private static void assertInvalidTimeout(Duration timeout) {
    assertThrows(
        IllegalArgumentException.class, () -> AlternatorRequestOptions.serverTimeout(timeout));
  }

  @SuppressWarnings("unchecked")
  private static Set<Class<? extends DynamoDbRequest>> operationRequests(Class<?> clientType) {
    return Arrays.stream(clientType.getMethods())
        .flatMap(method -> Arrays.stream(method.getParameterTypes()))
        .filter(DynamoDbRequest.class::isAssignableFrom)
        .map(type -> (Class<? extends DynamoDbRequest>) type)
        .collect(Collectors.toSet());
  }
}
