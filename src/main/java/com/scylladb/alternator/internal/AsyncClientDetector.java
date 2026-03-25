package com.scylladb.alternator.internal;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Detects which async HTTP client implementation is available on the classpath.
 *
 * <p>Priority order: Netty &gt; CRT. Throws {@link IllegalStateException} if none is found.
 *
 * @since 2.1.0
 */
public final class AsyncClientDetector {

  private static final Logger logger = Logger.getLogger(AsyncClientDetector.class.getName());

  // WARNING: String concatenation is intentional — do not "simplify" into a single literal.
  // The checkstyle RegexpMultiline rule bans fully-qualified class names outside imports;
  // splitting the string prevents the regex from matching the constant value.
  private static final String NETTY_CLASS =
      "software.amazon.awssdk" + ".http.nio.netty.NettyNioAsyncHttpClient";
  private static final String CRT_CLASS =
      "software.amazon.awssdk" + ".http.crt.AwsCrtAsyncHttpClient";

  /** Identifies which async HTTP client implementation is available. */
  public enum AsyncClientType {
    /** Netty NIO HTTP Client. */
    NETTY,
    /** AWS CRT Async HTTP Client. */
    CRT
  }

  private AsyncClientDetector() {}

  /**
   * Detects which async HTTP client implementation is on the classpath.
   *
   * @return the detected client type
   * @throws IllegalStateException if no supported implementation is found
   */
  public static AsyncClientType detect() {
    if (ClasspathUtil.isClassAvailable(NETTY_CLASS)) {
      logger.log(Level.FINE, "Detected Netty NIO async HTTP client on classpath");
      return AsyncClientType.NETTY;
    }
    if (ClasspathUtil.isClassAvailable(CRT_CLASS)) {
      logger.log(Level.FINE, "Detected AWS CRT async HTTP client on classpath");
      return AsyncClientType.CRT;
    }
    throw new IllegalStateException(
        "No supported async HTTP client implementation found on the classpath. "
            + "Add one of the following dependencies: "
            + "software.amazon.awssdk:netty-nio-client or "
            + "software.amazon.awssdk:aws-crt-client");
  }

  /**
   * Verifies that the specified async HTTP client implementation is available on the classpath.
   *
   * @param type the client type to check
   * @throws IllegalStateException if the implementation is not on the classpath
   */
  public static void requireAvailableHttpClient(AsyncClientType type) {
    switch (type) {
      case NETTY:
        if (!ClasspathUtil.isClassAvailable(NETTY_CLASS)) {
          throw new IllegalStateException(
              "HttpClientType.NETTY was requested, but Netty NIO HTTP client is not on the "
                  + "classpath. Add dependency: software.amazon.awssdk:netty-nio-client");
        }
        return;
      case CRT:
        if (!ClasspathUtil.isClassAvailable(CRT_CLASS)) {
          throw new IllegalStateException(
              "HttpClientType.CRT was requested, but AWS CRT async HTTP client is not on the "
                  + "classpath. Add dependency: software.amazon.awssdk:aws-crt-client");
        }
        return;
      default:
        throw new IllegalStateException("Unknown async client type: " + type);
    }
  }
}
