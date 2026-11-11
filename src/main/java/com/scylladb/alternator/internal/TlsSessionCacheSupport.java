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

import com.scylladb.alternator.TlsConfig;
import com.scylladb.alternator.TlsSessionCacheConfig;

/** Shared TLS session cache capability checks for AWS SDK HTTP client factories. */
final class TlsSessionCacheSupport {

  private TlsSessionCacheSupport() {}

  static boolean hasCustomSessionCacheConfig(TlsConfig tlsConfig) {
    return tlsConfig != null
        && !TlsSessionCacheConfig.getDefault().equals(tlsConfig.getSessionCacheConfig());
  }

  static void rejectUnsupportedSessionCacheConfig(TlsConfig tlsConfig, String clientName) {
    if (hasCustomSessionCacheConfig(tlsConfig)) {
      throw new UnsupportedOperationException(
          "Custom TLS session cache configuration is not supported with the "
              + clientName
              + " HTTP client.");
    }
  }
}
