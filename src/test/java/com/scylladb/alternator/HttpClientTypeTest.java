package com.scylladb.alternator;

import static org.junit.Assert.*;

import com.scylladb.alternator.internal.AsyncClientDetector;
import com.scylladb.alternator.internal.SyncClientDetector;
import org.junit.Test;

/** Unit tests for {@link HttpClientType} enum. */
public class HttpClientTypeTest {

  @Test
  public void testEnumValues() {
    HttpClientType[] values = HttpClientType.values();
    assertEquals(4, values.length);
    assertNotNull(HttpClientType.valueOf("AUTO"));
    assertNotNull(HttpClientType.valueOf("APACHE"));
    assertNotNull(HttpClientType.valueOf("NETTY"));
    assertNotNull(HttpClientType.valueOf("CRT"));
  }

  @Test
  public void testAutoSupportsBothSyncAndAsync() {
    assertTrue(HttpClientType.AUTO.supportsSync());
    assertTrue(HttpClientType.AUTO.supportsAsync());
  }

  @Test
  public void testApacheSupportsSyncOnly() {
    assertTrue(HttpClientType.APACHE.supportsSync());
    assertFalse(HttpClientType.APACHE.supportsAsync());
  }

  @Test
  public void testNettySupportAsyncOnly() {
    assertFalse(HttpClientType.NETTY.supportsSync());
    assertTrue(HttpClientType.NETTY.supportsAsync());
  }

  @Test
  public void testCrtSupportsBothSyncAndAsync() {
    assertTrue(HttpClientType.CRT.supportsSync());
    assertTrue(HttpClientType.CRT.supportsAsync());
  }

  @Test
  public void testToSyncClientTypeApache() {
    assertEquals(
        SyncClientDetector.SyncClientType.APACHE, HttpClientType.APACHE.toSyncClientType());
  }

  @Test
  public void testToSyncClientTypeCrt() {
    assertEquals(SyncClientDetector.SyncClientType.CRT, HttpClientType.CRT.toSyncClientType());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testToSyncClientTypeAutoThrows() {
    HttpClientType.AUTO.toSyncClientType();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testToSyncClientTypeNettyThrows() {
    HttpClientType.NETTY.toSyncClientType();
  }

  @Test
  public void testToAsyncClientTypeNetty() {
    assertEquals(
        AsyncClientDetector.AsyncClientType.NETTY, HttpClientType.NETTY.toAsyncClientType());
  }

  @Test
  public void testToAsyncClientTypeCrt() {
    assertEquals(AsyncClientDetector.AsyncClientType.CRT, HttpClientType.CRT.toAsyncClientType());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testToAsyncClientTypeAutoThrows() {
    HttpClientType.AUTO.toAsyncClientType();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testToAsyncClientTypeApacheThrows() {
    HttpClientType.APACHE.toAsyncClientType();
  }
}
