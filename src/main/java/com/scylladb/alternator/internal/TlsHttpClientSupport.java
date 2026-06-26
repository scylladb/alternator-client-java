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
