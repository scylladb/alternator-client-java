package com.scylladb.alternator.internal;

import static org.junit.Assert.*;

import com.scylladb.alternator.AlternatorConfig;
import com.scylladb.alternator.TlsConfig;
import com.scylladb.alternator.TlsSessionCacheConfig;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.junit.Test;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;

/**
 * Unit tests for {@link ApacheSyncClientFactory}.
 *
 * <p>Verifies that the factory properly creates Apache HTTP clients with config defaults, user
 * customizers, and TLS settings.
 */
public class ApacheSyncClientFactoryTest {

  @Test
  public void testCreateWithNullConfigAndNullCustomizer() {
    SdkHttpClient client = ApacheSyncClientFactory.create(null, null, null);
    assertNotNull("Should create client with null config and customizer", client);
    client.close();
  }

  @Test
  public void testCreateWithDefaultConfig() {
    AlternatorConfig config = AlternatorConfig.builder().build();
    SdkHttpClient client = ApacheSyncClientFactory.create(null, config, null);
    assertNotNull("Should create client with default config", client);
    client.close();
  }

  @Test
  public void testCreateWithCustomConnectionPoolSettings() {
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withMaxConnections(100)
            .withConnectionMaxIdleTimeMs(30000)
            .withConnectionTimeToLiveMs(60000)
            .withConnectionAcquisitionTimeoutMs(5000)
            .withConnectionTimeoutMs(5000)
            .build();
    SdkHttpClient client = ApacheSyncClientFactory.create(null, config, null);
    assertNotNull("Should create client with custom pool settings", client);
    client.close();
  }

  @Test
  public void testCustomizerIsInvoked() {
    AtomicBoolean customizerCalled = new AtomicBoolean(false);
    SdkHttpClient client =
        ApacheSyncClientFactory.create(
            builder -> {
              customizerCalled.set(true);
              builder.maxConnections(50);
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
    SdkHttpClient client =
        ApacheSyncClientFactory.create(
            builder -> {
              customizerCalled.set(true);
              // Override config's maxConnections with a different value
              builder.maxConnections(200);
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
    SdkHttpClient client = ApacheSyncClientFactory.create(null, null, tlsConfig);
    assertNotNull("Should create client with trust-all TLS", client);
    client.close();
  }

  @Test
  public void testCreateWithSystemDefaultTls() {
    TlsConfig tlsConfig = TlsConfig.systemDefault();
    SdkHttpClient client = ApacheSyncClientFactory.create(null, null, tlsConfig);
    assertNotNull("Should create client with system-default TLS", client);
    client.close();
  }

  @Test
  public void testCreateWithHostnameVerificationDisabledUsesCustomSocketFactory() {
    TlsConfig tlsConfig = TlsConfig.builder().withVerifyHostname(false).build();
    AtomicReference<ConnectionSocketFactory> socketFactory = new AtomicReference<>();
    SdkHttpClient client =
        ApacheSyncClientFactory.create(
            builder -> socketFactory.set(readSocketFactory(builder)), null, tlsConfig);

    assertTrue(
        "Should use a custom SSL socket factory when hostname verification is disabled",
        socketFactory.get() instanceof SSLConnectionSocketFactory);
    client.close();
  }

  @Test
  public void testCreateWithCustomSessionCacheUsesCustomSocketFactory() {
    TlsConfig tlsConfig =
        TlsConfig.builder()
            .withSessionCacheConfig(
                TlsSessionCacheConfig.builder()
                    .withSessionCacheSize(64)
                    .withSessionTimeoutSeconds(300)
                    .build())
            .build();
    AtomicReference<ConnectionSocketFactory> socketFactory = new AtomicReference<>();
    SdkHttpClient client =
        ApacheSyncClientFactory.create(
            builder -> socketFactory.set(readSocketFactory(builder)), null, tlsConfig);

    assertTrue(
        "Should use a custom SSL socket factory for custom TLS session cache config",
        socketFactory.get() instanceof SSLConnectionSocketFactory);
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
    SdkHttpClient client = ApacheSyncClientFactory.create(null, config, tlsConfig);
    assertNotNull("Should create client with config and TLS", client);
    client.close();
  }

  @Test
  public void testCreateWithConfigCustomizerAndTls() {
    AtomicBoolean customizerCalled = new AtomicBoolean(false);
    AlternatorConfig config = AlternatorConfig.builder().withMaxConnections(50).build();
    TlsConfig tlsConfig = TlsConfig.trustAll();
    SdkHttpClient client =
        ApacheSyncClientFactory.create(builder -> customizerCalled.set(true), config, tlsConfig);
    assertTrue("Customizer should be called with all params present", customizerCalled.get());
    assertNotNull(client);
    client.close();
  }

  @Test
  public void testCreatePollingClient() {
    SdkHttpClient client = ApacheSyncClientFactory.createPollingClient(null);
    assertNotNull("Should create polling client", client);
    client.close();
  }

  @Test
  public void testCreatePollingClientWithTrustAllTls() {
    SdkHttpClient client = ApacheSyncClientFactory.createPollingClient(TlsConfig.trustAll());
    assertNotNull("Should create polling client with trust-all TLS", client);
    client.close();
  }

  @Test
  public void testCreatePollingClientWithSystemDefaultTls() {
    SdkHttpClient client = ApacheSyncClientFactory.createPollingClient(TlsConfig.systemDefault());
    assertNotNull("Should create polling client with system-default TLS", client);
    client.close();
  }

  @Test
  public void testCreatePollingClientWithHostnameVerificationDisabled() {
    TlsConfig tlsConfig = TlsConfig.builder().withVerifyHostname(false).build();
    SdkHttpClient client = ApacheSyncClientFactory.createPollingClient(tlsConfig);
    assertNotNull("Should create polling client with hostname verification disabled", client);
    client.close();
  }

  @Test
  public void testCreatePollingClientWithCustomSessionCache() {
    TlsConfig tlsConfig =
        TlsConfig.builder().withSessionCacheConfig(TlsSessionCacheConfig.disabled()).build();
    SdkHttpClient client = ApacheSyncClientFactory.createPollingClient(tlsConfig);
    assertNotNull("Should create polling client with custom TLS session cache config", client);
    client.close();
  }

  @Test(expected = RuntimeException.class)
  public void testCreateWithInvalidCaCertPathThrows() {
    TlsConfig tlsConfig =
        TlsConfig.builder()
            .withCaCertPath(Path.of("/non/existent/ca.pem"))
            .withTrustSystemCaCerts(false)
            .build();
    ApacheSyncClientFactory.create(null, null, tlsConfig);
  }

  @Test(expected = RuntimeException.class)
  public void testCreateWithInvalidClientCertificatePathThrows() {
    TlsConfig tlsConfig =
        TlsConfig.builder()
            .withTrustAllCertificates(true)
            .withClientCertificate(
                Path.of("/non/existent/client.crt"), Path.of("/non/existent/client.key"))
            .build();
    ApacheSyncClientFactory.create(null, null, tlsConfig);
  }

  @Test(expected = RuntimeException.class)
  public void testCreateWithInvalidCaCertContentThrows() throws Exception {
    Path tempFile = Files.createTempFile("invalid-ca-", ".pem");
    try {
      Files.writeString(tempFile, "This is not a valid certificate");
      TlsConfig tlsConfig =
          TlsConfig.builder().withCaCertPath(tempFile).withTrustSystemCaCerts(false).build();
      ApacheSyncClientFactory.create(null, null, tlsConfig);
    } finally {
      Files.deleteIfExists(tempFile);
    }
  }

  @Test(expected = RuntimeException.class)
  public void testCreatePollingClientWithInvalidCaCertPathThrows() {
    TlsConfig tlsConfig =
        TlsConfig.builder()
            .withCaCertPath(Path.of("/non/existent/ca.pem"))
            .withTrustSystemCaCerts(false)
            .build();
    ApacheSyncClientFactory.createPollingClient(tlsConfig);
  }

  @Test(expected = RuntimeException.class)
  public void testCreatePollingClientWithInvalidClientCertificatePathThrows() {
    TlsConfig tlsConfig =
        TlsConfig.builder()
            .withTrustAllCertificates(true)
            .withClientCertificate(
                Path.of("/non/existent/client.crt"), Path.of("/non/existent/client.key"))
            .build();
    ApacheSyncClientFactory.createPollingClient(tlsConfig);
  }

  @Test
  public void testCreateWithCustomCaCertAndSystemCAs() throws Exception {
    // With only system CAs (no custom cert path), should create successfully
    TlsConfig tlsConfig = TlsConfig.builder().withTrustSystemCaCerts(true).build();
    SdkHttpClient client = ApacheSyncClientFactory.create(null, null, tlsConfig);
    assertNotNull("Should create client with system CAs", client);
    client.close();
  }

  private static ConnectionSocketFactory readSocketFactory(ApacheHttpClient.Builder builder) {
    try {
      Field socketFactoryField = builder.getClass().getDeclaredField("socketFactory");
      socketFactoryField.setAccessible(true);
      return (ConnectionSocketFactory) socketFactoryField.get(builder);
    } catch (ReflectiveOperationException e) {
      throw new AssertionError(e);
    }
  }
}
