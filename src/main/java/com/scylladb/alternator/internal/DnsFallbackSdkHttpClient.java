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

import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.SdkHttpClient;

/**
 * Polling HTTP client capable of targeting one resolved address while retaining the logical request
 * endpoint.
 *
 * <p>{@link AlternatorLiveNodes} uses this capability to validate each DNS address independently.
 * Implementations must retain the logical hostname anywhere it affects HTTP Host, TLS server name
 * indication and certificate verification, or request-signing inputs. The Alternator-provided
 * implementations also enforce bounded DNS and request execution; custom implementations are
 * responsible for honoring the same timeout and cancellation contract.
 *
 * @since 2.0.6
 */
public interface DnsFallbackSdkHttpClient extends SdkHttpClient {

  /**
   * Reports whether addressed execution preserves logical endpoint semantics for the scheme.
   *
   * @param scheme request URI scheme
   * @return true when {@link #prepareRequestForAddress} is safe for the scheme
   */
  boolean supportsDnsFallback(String scheme);

  /**
   * Resolves the logical hostname for the current discovery cycle.
   *
   * @param hostname logical request hostname
   * @return resolved addresses in resolver order
   * @throws IOException if resolution fails
   */
  List<InetAddress> resolve(String hostname) throws IOException;

  /**
   * Resolves the logical hostname without waiting longer than the caller's remaining discovery
   * budget. Implementations with an internal resolver deadline must use the shorter deadline and
   * must not fall back to an unbounded synchronous resolution call.
   *
   * @param hostname logical request hostname
   * @param timeoutMillis caller's remaining resolution budget in milliseconds
   * @return resolved addresses in resolver order
   * @throws IOException if resolution fails or exceeds the deadline
   */
  List<InetAddress> resolve(String hostname, long timeoutMillis) throws IOException;

  /**
   * Resolves a hostname while allowing built-in clients to reserve capacity for configured seeds.
   * Custom clients remain source-compatible and use their normal bounded resolver by default.
   *
   * @param hostname logical request hostname
   * @param timeoutMillis caller's remaining resolution budget in milliseconds
   * @param seedCandidate whether the hostname is a retained configured seed
   * @return resolved addresses in resolver order
   * @throws IOException if resolution fails or exceeds the deadline
   */
  default List<InetAddress> resolve(String hostname, long timeoutMillis, boolean seedCandidate)
      throws IOException {
    return resolve(hostname, timeoutMillis);
  }

  /**
   * Prepares a request that connects to {@code address} while preserving the logical request
   * endpoint.
   *
   * @param request request containing the logical endpoint and Host header
   * @param address concrete address to connect to
   * @return executable request
   */
  ExecutableHttpRequest prepareRequestForAddress(HttpExecuteRequest request, InetAddress address);
}
