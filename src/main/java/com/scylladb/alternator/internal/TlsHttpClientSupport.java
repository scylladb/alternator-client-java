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

/** Shared TLS capability checks for AWS SDK HTTP client factories. */
final class TlsHttpClientSupport {

  private TlsHttpClientSupport() {}

  static boolean requiresHostnameVerificationDisabled(TlsConfig tlsConfig) {
    return tlsConfig != null
        && !tlsConfig.isTrustAllCertificates()
        && !tlsConfig.isVerifyHostname();
  }

  static void rejectUnsupportedHostnameVerification(TlsConfig tlsConfig, String clientName) {
    if (requiresHostnameVerificationDisabled(tlsConfig)) {
      throw new UnsupportedOperationException(
          "Disabling hostname verification while still validating certificates is not supported"
              + " with the "
              + clientName
              + " HTTP client. Use TlsConfig.trustAll() for testing.");
    }
  }
}
