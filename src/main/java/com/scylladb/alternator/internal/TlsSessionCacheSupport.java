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
