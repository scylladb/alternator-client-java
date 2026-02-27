package com.scylladb.alternator.internal;

import static org.junit.Assert.*;

import com.scylladb.alternator.TlsConfig;
import org.junit.Test;
import software.amazon.awssdk.http.SdkHttpClient;

/**
 * Unit tests for {@link SyncClientDetector}.
 *
 * <p>Verifies classpath detection and polling client creation for sync HTTP client implementations.
 */
public class SyncClientDetectorTest {

  @Test
  public void testDetectFindsImplementation() {
    // Both Apache and CRT are on the test classpath, so detection should succeed
    SyncClientDetector.SyncClientType type = SyncClientDetector.detect();
    assertNotNull("Should detect a sync client type", type);
  }

  @Test
  public void testDetectPrefersApacheOverCrt() {
    // Both are on classpath; Apache should win by priority
    SyncClientDetector.SyncClientType type = SyncClientDetector.detect();
    assertEquals(
        "Apache should be preferred when both are available",
        SyncClientDetector.SyncClientType.APACHE,
        type);
  }

  @Test
  public void testCreatePollingClientApache() {
    SdkHttpClient client =
        SyncClientDetector.createPollingClient(SyncClientDetector.SyncClientType.APACHE, null);
    assertNotNull("Should create Apache polling client", client);
    client.close();
  }

  @Test
  public void testCreatePollingClientCrt() {
    SdkHttpClient client =
        SyncClientDetector.createPollingClient(SyncClientDetector.SyncClientType.CRT, null);
    assertNotNull("Should create CRT polling client", client);
    client.close();
  }

  @Test
  public void testCreatePollingClientWithTrustAllTls() {
    TlsConfig tlsConfig = TlsConfig.trustAll();
    for (SyncClientDetector.SyncClientType type : SyncClientDetector.SyncClientType.values()) {
      SdkHttpClient client = SyncClientDetector.createPollingClient(type, tlsConfig);
      assertNotNull("Should create polling client with trust-all TLS for " + type, client);
      client.close();
    }
  }

  @Test
  public void testSyncClientTypeValues() {
    SyncClientDetector.SyncClientType[] values = SyncClientDetector.SyncClientType.values();
    assertEquals("Should have exactly 2 sync client types", 2, values.length);
    assertEquals(SyncClientDetector.SyncClientType.APACHE, values[0]);
    assertEquals(SyncClientDetector.SyncClientType.CRT, values[1]);
  }
}
