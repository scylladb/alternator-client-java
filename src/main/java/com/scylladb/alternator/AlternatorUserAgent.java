package com.scylladb.alternator;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.Properties;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;

/** Adds this Alternator wrapper's identity to AWS SDK user-agent strings. */
final class AlternatorUserAgent {
  static final String PRODUCT_NAME = "scylladb-alternator-client-java";
  private static final String VERSION_RESOURCE =
      "META-INF/maven/com.scylladb.alternator/load-balancing/pom.properties";
  private static final String UNKNOWN_VERSION = "unknown";
  private static final String USER_AGENT_TOKEN = PRODUCT_NAME + "/" + resolveVersion();

  private AlternatorUserAgent() {}

  static void applyTo(ClientOverrideConfiguration.Builder overrideBuilder) {
    Objects.requireNonNull(overrideBuilder, "overrideBuilder");

    String existingSuffix =
        overrideBuilder.advancedOptions().get(SdkAdvancedClientOption.USER_AGENT_SUFFIX);
    overrideBuilder.putAdvancedOption(
        SdkAdvancedClientOption.USER_AGENT_SUFFIX, appendToken(existingSuffix, USER_AGENT_TOKEN));
  }

  static String userAgentToken() {
    return USER_AGENT_TOKEN;
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
