package com.scylladb.alternator.internal;

import static org.junit.Assert.*;

import com.scylladb.alternator.AlternatorConfig;
import com.scylladb.alternator.TlsConfig;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Test;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;

/**
 * Unit tests for {@link CrtAsyncClientFactory}.
 *
 * <p>Verifies that the factory properly creates CRT async HTTP clients with config defaults, user
 * customizers, and TLS settings.
 */
public class CrtAsyncClientFactoryTest {

  @Test
  public void testCreateWithNullConfigAndNullCustomizer() {
    SdkAsyncHttpClient client = CrtAsyncClientFactory.create(null, null, null);
    assertNotNull("Should create client with null config and customizer", client);
    client.close();
  }

  @Test
  public void testCreateWithDefaultConfig() {
    AlternatorConfig config = AlternatorConfig.builder().build();
    SdkAsyncHttpClient client = CrtAsyncClientFactory.create(null, config, null);
    assertNotNull("Should create client with default config", client);
    client.close();
  }

  @Test
  public void testCreateWithCustomConnectionPoolSettings() {
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withMaxConnections(100)
            .withConnectionMaxIdleTimeMs(30000)
            .build();
    SdkAsyncHttpClient client = CrtAsyncClientFactory.create(null, config, null);
    assertNotNull("Should create client with custom pool settings", client);
    client.close();
  }

  @Test
  public void testCustomizerIsInvoked() {
    AtomicBoolean customizerCalled = new AtomicBoolean(false);
    SdkAsyncHttpClient client =
        CrtAsyncClientFactory.create(
            builder -> {
              customizerCalled.set(true);
              builder.maxConcurrency(50);
            },
            null,
            null);
    assertTrue("Customizer should have been called", customizerCalled.get());
    assertNotNull(client);
    client.close();
  }

  @Test
  public void testCustomizerOverridesConfigDefaults() {
    AtomicBoolean customizerCalled = new AtomicBoolean(false);
    AlternatorConfig config = AlternatorConfig.builder().withMaxConnections(100).build();
    SdkAsyncHttpClient client =
        CrtAsyncClientFactory.create(
            builder -> {
              customizerCalled.set(true);
              builder.maxConcurrency(200);
            },
            config,
            null);
    assertTrue("Customizer should have been called", customizerCalled.get());
    assertNotNull(client);
    client.close();
  }

  @Test
  public void testCreateWithTrustAllTls() {
    TlsConfig tlsConfig = TlsConfig.trustAll();
    SdkAsyncHttpClient client = CrtAsyncClientFactory.create(null, null, tlsConfig);
    assertNotNull("Should create client with trust-all TLS", client);
    client.close();
  }

  @Test
  public void testCreateWithSystemDefaultTls() {
    TlsConfig tlsConfig = TlsConfig.systemDefault();
    SdkAsyncHttpClient client = CrtAsyncClientFactory.create(null, null, tlsConfig);
    assertNotNull("Should create client with system-default TLS", client);
    client.close();
  }

  @Test
  public void testCreateWithConfigAndTls() {
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withMaxConnections(50)
            .withConnectionMaxIdleTimeMs(10000)
            .build();
    TlsConfig tlsConfig = TlsConfig.trustAll();
    SdkAsyncHttpClient client = CrtAsyncClientFactory.create(null, config, tlsConfig);
    assertNotNull("Should create client with config and TLS", client);
    client.close();
  }

  @Test
  public void testCreateWithConfigCustomizerAndTls() {
    AtomicBoolean customizerCalled = new AtomicBoolean(false);
    AlternatorConfig config = AlternatorConfig.builder().withMaxConnections(50).build();
    TlsConfig tlsConfig = TlsConfig.trustAll();
    SdkAsyncHttpClient client =
        CrtAsyncClientFactory.create(builder -> customizerCalled.set(true), config, tlsConfig);
    assertTrue("Customizer should be called with all params present", customizerCalled.get());
    assertNotNull(client);
    client.close();
  }

  @Test
  public void testConnectionTimeToLiveNotSupported() {
    // CRT doesn't support connectionTimeToLive, verify it doesn't cause errors
    AlternatorConfig config = AlternatorConfig.builder().withConnectionTimeToLiveMs(60000).build();
    SdkAsyncHttpClient client = CrtAsyncClientFactory.create(null, config, null);
    assertNotNull("CRT should handle config with TTL gracefully", client);
    client.close();
  }

  @Test(expected = RuntimeException.class)
  public void testCreateWithInvalidCaCertPathThrows() {
    TlsConfig tlsConfig =
        TlsConfig.builder()
            .withCaCertPath(Path.of("/non/existent/ca.pem"))
            .withTrustSystemCaCerts(false)
            .build();
    CrtAsyncClientFactory.create(null, null, tlsConfig);
  }

  @Test(expected = RuntimeException.class)
  public void testCreateWithInvalidCaCertContentThrows() throws Exception {
    Path tempFile = Files.createTempFile("invalid-ca-", ".pem");
    try {
      Files.writeString(tempFile, "This is not a valid certificate");
      TlsConfig tlsConfig =
          TlsConfig.builder().withCaCertPath(tempFile).withTrustSystemCaCerts(false).build();
      CrtAsyncClientFactory.create(null, null, tlsConfig);
    } finally {
      Files.deleteIfExists(tempFile);
    }
  }
}
