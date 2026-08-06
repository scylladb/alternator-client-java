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
package com.scylladb.alternator;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
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
 *   <li>{@code ALTERNATOR_HTTPS_PORT} - HTTPS port (default: 9999)
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

  public static final int HTTPS_PORT =
      Integer.parseInt(System.getenv().getOrDefault("ALTERNATOR_HTTPS_PORT", "9999"));

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

    try {
      HTTP_SEED_URI = new URI("http://" + HOST + ":" + HTTP_PORT);
      HTTPS_SEED_URI = new URI("https://" + HOST + ":" + HTTPS_PORT);
    } catch (URISyntaxException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private IntegrationTestConfig() {}

  /** Returns parameterized data for {@code @Parameters} — one entry per protocol (HTTP, HTTPS). */
  public static Collection<Object[]> httpAndHttpsEndpoints() {
    return Arrays.asList(
        new Object[] {"http", HTTP_SEED_URI}, new Object[] {"https", HTTPS_SEED_URI});
  }
}
