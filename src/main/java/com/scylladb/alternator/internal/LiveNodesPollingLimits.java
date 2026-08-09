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

/** Shared work and data limits for live-node discovery. */
final class LiveNodesPollingLimits {

  static final LiveNodesPollingLimits DEFAULT =
      new LiveNodesPollingLimits(
          3_000, 8, 3_000, 5_000, 10_000, 12_000, 30_000, 64 * 1024, 1024 * 1024);

  final int dnsTimeoutMillis;
  final int maxDnsAddresses;
  final int connectTimeoutMillis;
  final int readTimeoutMillis;
  final int attemptTimeoutMillis;
  final int candidateTimeoutMillis;
  final int cycleTimeoutMillis;
  final int maxHeaderBytes;
  final int maxResponseBytes;

  LiveNodesPollingLimits(
      int dnsTimeoutMillis,
      int maxDnsAddresses,
      int connectTimeoutMillis,
      int readTimeoutMillis,
      int attemptTimeoutMillis,
      int candidateTimeoutMillis,
      int cycleTimeoutMillis,
      int maxHeaderBytes,
      int maxResponseBytes) {
    this.dnsTimeoutMillis = requirePositive(dnsTimeoutMillis, "dnsTimeoutMillis");
    this.maxDnsAddresses = requirePositive(maxDnsAddresses, "maxDnsAddresses");
    this.connectTimeoutMillis = requirePositive(connectTimeoutMillis, "connectTimeoutMillis");
    this.readTimeoutMillis = requirePositive(readTimeoutMillis, "readTimeoutMillis");
    this.attemptTimeoutMillis = requirePositive(attemptTimeoutMillis, "attemptTimeoutMillis");
    this.candidateTimeoutMillis = requirePositive(candidateTimeoutMillis, "candidateTimeoutMillis");
    this.cycleTimeoutMillis = requirePositive(cycleTimeoutMillis, "cycleTimeoutMillis");
    this.maxHeaderBytes = requirePositive(maxHeaderBytes, "maxHeaderBytes");
    this.maxResponseBytes = requirePositive(maxResponseBytes, "maxResponseBytes");
  }

  private static int requirePositive(int value, String name) {
    if (value <= 0) {
      throw new IllegalArgumentException(name + " must be positive");
    }
    return value;
  }
}
