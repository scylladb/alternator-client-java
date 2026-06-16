package com.scylladb.alternator;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

/**
 * Centralized configuration for integration tests, reading all settings from environment variables
 * in one place.
 *
 * <p>Environment variables:
 *
 * <ul>
 *   <li>{@code ALTERNATOR_HOST} - Host address (default: 172.39.0.2)
 *   <li>{@code ALTERNATOR_PORT} - HTTP port (default: 9998)
 *   <li>{@code ALTERNATOR_HTTPS_PORT} - HTTPS port (default: 9999); set to empty string to disable
 *       HTTPS test variants
 *   <li>{@code ALTERNATOR_DATACENTER} - Datacenter name (default: datacenter1)
 *   <li>{@code ALTERNATOR_RACK} - Rack name (default: rack1)
 *   <li>{@code INTEGRATION_TESTS} - Set to "true" to enable integration tests
 *   <li>{@code ALTERNATOR_CA_CERT_PATH} - Optional path to CA certificate for TLS tests
 * </ul>
 */
public final class IntegrationTestConfig {

  public static final String HOST = System.getenv().getOrDefault("ALTERNATOR_HOST", "172.39.0.2");

  public static final int HTTP_PORT =
      Integer.parseInt(System.getenv().getOrDefault("ALTERNATOR_PORT", "9998"));

  /**
   * HTTPS port, or -1 if {@code ALTERNATOR_HTTPS_PORT} was set to an empty string (disables HTTPS
   * variants).
   */
  public static final int HTTPS_PORT;

  public static final String DATACENTER =
      System.getenv().getOrDefault("ALTERNATOR_DATACENTER", "datacenter1");

  public static final String RACK = System.getenv().getOrDefault("ALTERNATOR_RACK", "rack1");

  public static final boolean ENABLED =
      Boolean.parseBoolean(System.getenv().getOrDefault("INTEGRATION_TESTS", "false"));

  public static final Path CA_CERT_PATH;

  public static final URI HTTP_SEED_URI;
  public static final URI HTTPS_SEED_URI;

  public static final StaticCredentialsProvider CREDENTIALS =
      StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"));

  static {
    String caCertEnv = System.getenv("ALTERNATOR_CA_CERT_PATH");
    CA_CERT_PATH = (caCertEnv != null && !caCertEnv.isEmpty()) ? Path.of(caCertEnv) : null;

    String httpsPortEnv = System.getenv("ALTERNATOR_HTTPS_PORT");
    if (httpsPortEnv != null && httpsPortEnv.isEmpty()) {
      HTTPS_PORT = -1;
    } else {
      HTTPS_PORT = Integer.parseInt(httpsPortEnv != null ? httpsPortEnv : "9999");
    }

    try {
      HTTP_SEED_URI = new URI("http://" + HOST + ":" + HTTP_PORT);
      HTTPS_SEED_URI = HTTPS_PORT != -1 ? new URI("https://" + HOST + ":" + HTTPS_PORT) : null;
    } catch (URISyntaxException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private IntegrationTestConfig() {}

  /**
   * Returns parameterized data for {@code @Parameters} — one entry per enabled protocol. The HTTPS
   * entry is omitted when {@code ALTERNATOR_HTTPS_PORT} is set to an empty string.
   */
  public static Collection<Object[]> httpAndHttpsEndpoints() {
    List<Object[]> params = new ArrayList<>();
    params.add(new Object[] {"http", HTTP_SEED_URI});
    if (HTTPS_PORT != -1) {
      params.add(new Object[] {"https", HTTPS_SEED_URI});
    }
    return params;
  }
}
