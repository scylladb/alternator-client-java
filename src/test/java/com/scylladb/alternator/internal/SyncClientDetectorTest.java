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
    SyncClientDetector.SyncClientType type = SyncClientDetector.detect();
    assertNotNull("Should detect a sync client type", type);
  }

  @Test
  public void testDetectPrefersApacheOverCrt() {
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
  public void testRequireAvailableHttpClientApache() {
    SyncClientDetector.requireAvailableHttpClient(SyncClientDetector.SyncClientType.APACHE);
  }

  @Test
  public void testRequireAvailableHttpClientCrt() {
    SyncClientDetector.requireAvailableHttpClient(SyncClientDetector.SyncClientType.CRT);
  }

  @Test
  public void testSyncClientTypeValues() {
    SyncClientDetector.SyncClientType[] values = SyncClientDetector.SyncClientType.values();
    assertEquals("Should have exactly 2 sync client types", 2, values.length);
    assertEquals(SyncClientDetector.SyncClientType.APACHE, values[0]);
    assertEquals(SyncClientDetector.SyncClientType.CRT, values[1]);
  }
}
