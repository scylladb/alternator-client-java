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

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;

/**
 * Factory for ScyllaDB Alternator per-operation request overrides.
 *
 * <p>Options returned by this class can be passed to every low-level DynamoDB request builder's
 * {@code overrideConfiguration} method. For example:
 *
 * <pre>{@code
 * GetItemRequest request = GetItemRequest.builder()
 *     .tableName("users")
 *     .key(key)
 *     .overrideConfiguration(
 *         AlternatorRequestOptions.serverTimeout(Duration.ofSeconds(5)))
 *     .build();
 * }</pre>
 *
 * @since 2.1.0
 */
public final class AlternatorRequestOptions {
  /** Header used to send the server-side request timeout for one operation. */
  public static final String SERVER_TIMEOUT_HEADER = "X-Scylla-Timeout";

  private static final long NANOS_PER_MILLISECOND = 1_000_000L;

  private AlternatorRequestOptions() {}

  /**
   * Creates a request override that sets the server-side timeout for one operation.
   *
   * <p>The timeout must be non-negative, have whole-millisecond precision, and fit in the signed
   * 64-bit nanosecond range accepted by CQL durations. Zero is valid.
   *
   * @param timeout server-side timeout
   * @return request override that adds {@value #SERVER_TIMEOUT_HEADER}
   * @throws IllegalArgumentException if the timeout is null, negative, has sub-millisecond
   *     precision, or exceeds the CQL duration range
   * @since 2.1.0
   */
  public static Consumer<AwsRequestOverrideConfiguration.Builder> serverTimeout(Duration timeout) {
    if (timeout == null) {
      throw new IllegalArgumentException("timeout cannot be null");
    }
    if (timeout.isNegative()) {
      throw new IllegalArgumentException("timeout cannot be negative");
    }

    final long totalNanoseconds;
    try {
      totalNanoseconds = timeout.toNanos();
    } catch (ArithmeticException e) {
      throw new IllegalArgumentException("timeout exceeds the signed 64-bit CQL duration range", e);
    }

    if (totalNanoseconds % NANOS_PER_MILLISECOND != 0) {
      throw new IllegalArgumentException("timeout must use whole-millisecond precision");
    }

    String headerValue = timeout.toMillis() + "ms";
    return builder -> {
      Map<String, List<String>> headers = new LinkedHashMap<>(builder.headers());
      headers
          .keySet()
          .removeIf(
              headerName ->
                  headerName != null && headerName.equalsIgnoreCase(SERVER_TIMEOUT_HEADER));
      headers.put(SERVER_TIMEOUT_HEADER, Collections.singletonList(headerValue));
      builder.headers(headers);
    };
  }
}
