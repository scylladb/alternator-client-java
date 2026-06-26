package com.scylladb.alternator;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.Properties;
import java.util.function.UnaryOperator;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.http.SdkHttpRequest;

/** Adds this Alternator wrapper's identity to AWS SDK user-agent strings. */
final class AlternatorUserAgent {
  static final String HEADER_NAME = "User-Agent";
  static final String PRODUCT_NAME = "scylladb-alternator-client-java";
  private static final String VERSION_RESOURCE =
      "META-INF/maven/com.scylladb.alternator/load-balancing/pom.properties";
  private static final String UNKNOWN_VERSION = "unknown";
  private static final String USER_AGENT_TOKEN = PRODUCT_NAME + "/" + resolveVersion();

  private AlternatorUserAgent() {}

  static void applyDefaultSuffixTo(ClientOverrideConfiguration.Builder overrideBuilder) {
    Objects.requireNonNull(overrideBuilder, "overrideBuilder");

    String existingSuffix =
        overrideBuilder.advancedOptions().get(SdkAdvancedClientOption.USER_AGENT_SUFFIX);
    overrideBuilder.putAdvancedOption(
        SdkAdvancedClientOption.USER_AGENT_SUFFIX, appendToken(existingSuffix, USER_AGENT_TOKEN));
  }

  static String userAgentToken() {
    return USER_AGENT_TOKEN;
  }

  static UnaryOperator<String> replaceWith(String userAgent) {
    requireValidUserAgent(userAgent);
    return current -> userAgent;
  }

  static UnaryOperator<String> disable() {
    return current -> null;
  }

  static void requireValidUserAgent(String userAgent) {
    if (userAgent == null || userAgent.trim().isEmpty()) {
      throw new IllegalArgumentException("userAgent cannot be null or blank");
    }
  }

  static <T> T requireUserAgentTransformer(T userAgentTransformer) {
    if (userAgentTransformer == null) {
      throw new IllegalArgumentException("userAgentTransformer cannot be null");
    }
    return userAgentTransformer;
  }

  static String appendToken(String existingSuffix, String token) {
    if (existingSuffix == null || existingSuffix.trim().isEmpty()) {
      return token;
    }

    String trimmed = existingSuffix.trim();
    if (trimmed.contains(token)) {
      return trimmed;
    }
    return trimmed + " " + token;
  }

  static SdkHttpRequest transform(SdkHttpRequest request, UnaryOperator<String> transformer) {
    String currentUserAgent = request.firstMatchingHeader(HEADER_NAME).orElse("");
    String replacement = transformer.apply(currentUserAgent);

    SdkHttpRequest.Builder requestBuilder = request.toBuilder();
    requestBuilder.clearHeaders();
    request
        .headers()
        .forEach(
            (headerName, values) -> {
              if (!HEADER_NAME.equalsIgnoreCase(headerName)) {
                values.forEach(value -> requestBuilder.appendHeader(headerName, value));
              }
            });

    if (replacement != null && !replacement.trim().isEmpty()) {
      requestBuilder.appendHeader(HEADER_NAME, replacement);
    }
    return requestBuilder.build();
  }

  private static String resolveVersion() {
    String version = AlternatorUserAgent.class.getPackage().getImplementationVersion();
    if (version == null || version.trim().isEmpty()) {
      version = loadVersionFromPomProperties();
    }
    if (version == null || version.trim().isEmpty()) {
      version = UNKNOWN_VERSION;
    }
    return version.trim().replaceAll("\\s+", "_");
  }

  private static String loadVersionFromPomProperties() {
    try (InputStream input =
        AlternatorUserAgent.class.getClassLoader().getResourceAsStream(VERSION_RESOURCE)) {
      if (input == null) {
        return null;
      }
      Properties properties = new Properties();
      properties.load(input);
      return properties.getProperty("version");
    } catch (IOException e) {
      return null;
    }
  }
}
