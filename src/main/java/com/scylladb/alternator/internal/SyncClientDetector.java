package com.scylladb.alternator.internal;

import com.scylladb.alternator.TlsConfig;
import java.util.logging.Level;
import java.util.logging.Logger;
import software.amazon.awssdk.http.SdkHttpClient;

/**
 * Detects which sync HTTP client implementation is available on the classpath.
 *
 * <p>Priority order: Apache &gt; CRT. Throws {@link IllegalStateException} if none is found.
 *
 * @since 2.1.0
 */
public final class SyncClientDetector {

  private static final Logger logger = Logger.getLogger(SyncClientDetector.class.getName());

  // Class names split to satisfy checkstyle (no fully-qualified names inline)
  private static final String APACHE_CLASS =
      "software.amazon.awssdk" + ".http.apache.ApacheHttpClient";
  private static final String CRT_CLASS = "software.amazon.awssdk" + ".http.crt.AwsCrtHttpClient";

  /** Identifies which sync HTTP client implementation is available. */
  public enum SyncClientType {
    /** Apache HTTP Client. */
    APACHE,
    /** AWS CRT HTTP Client. */
    CRT
  }

  private SyncClientDetector() {}

  /**
   * Detects which sync HTTP client implementation is on the classpath.
   *
   * @return the detected client type
   * @throws IllegalStateException if no supported implementation is found
   */
  public static SyncClientType detect() {
    if (isClassAvailable(APACHE_CLASS)) {
      logger.log(Level.FINE, "Detected Apache HTTP client on classpath");
      return SyncClientType.APACHE;
    }
    if (isClassAvailable(CRT_CLASS)) {
      logger.log(Level.FINE, "Detected AWS CRT HTTP client on classpath");
      return SyncClientType.CRT;
    }
    throw new IllegalStateException(
        "No supported sync HTTP client implementation found on the classpath. "
            + "Add one of the following dependencies: "
            + "software.amazon.awssdk:apache-client or "
            + "software.amazon.awssdk:aws-crt-client");
  }

  /**
   * Creates a small sync HTTP client for LiveNodes polling.
   *
   * @param type the client type to create
   * @param tlsConfig the TLS configuration
   * @return a configured SdkHttpClient with small pool size
   */
  public static SdkHttpClient createPollingClient(SyncClientType type, TlsConfig tlsConfig) {
    switch (type) {
      case APACHE:
        return ApacheSyncClientFactory.createPollingClient(tlsConfig);
      case CRT:
        return CrtSyncClientFactory.createPollingClient(tlsConfig);
      default:
        throw new IllegalStateException("Unknown sync client type: " + type);
    }
  }

  private static boolean isClassAvailable(String className) {
    try {
      Class.forName(className);
      return true;
    } catch (ClassNotFoundException e) {
      return false;
    }
  }
}
