package com.scylladb.alternator;

import static org.junit.Assert.*;

import org.junit.Test;

/**
 * Unit tests for AlternatorConfig connection pool configuration.
 *
 * @author dmitry.kropachev
 */
public class AlternatorConfigConnectionPoolTest {

  @Test
  public void testDefaultConnectionPoolSettings() {
    AlternatorConfig config = AlternatorConfig.builder().build();

    assertEquals(AlternatorConfig.CONNECTION_POOL_DEFAULT, config.getMaxConnections());
    assertEquals(AlternatorConfig.CONNECTION_POOL_DEFAULT, config.getConnectionMaxIdleTimeMs());
    assertEquals(AlternatorConfig.CONNECTION_POOL_DEFAULT, config.getConnectionTimeToLiveMs());
    assertFalse(config.hasCustomConnectionPoolSettings());
  }

  @Test
  public void testConnectionPoolDefaultConstantIsZero() {
    assertEquals(0, AlternatorConfig.CONNECTION_POOL_DEFAULT);
  }

  @Test
  public void testCustomMaxConnections() {
    AlternatorConfig config = AlternatorConfig.builder().withMaxConnections(100).build();

    assertEquals(100, config.getMaxConnections());
    assertTrue(config.hasCustomConnectionPoolSettings());
  }

  @Test
  public void testCustomConnectionMaxIdleTime() {
    AlternatorConfig config = AlternatorConfig.builder().withConnectionMaxIdleTimeMs(30000).build();

    assertEquals(30000, config.getConnectionMaxIdleTimeMs());
    assertTrue(config.hasCustomConnectionPoolSettings());
  }

  @Test
  public void testCustomConnectionTimeToLive() {
    AlternatorConfig config = AlternatorConfig.builder().withConnectionTimeToLiveMs(60000).build();

    assertEquals(60000, config.getConnectionTimeToLiveMs());
    assertTrue(config.hasCustomConnectionPoolSettings());
  }

  @Test
  public void testAllConnectionPoolSettingsTogether() {
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withMaxConnections(200)
            .withConnectionMaxIdleTimeMs(30000)
            .withConnectionTimeToLiveMs(60000)
            .build();

    assertEquals(200, config.getMaxConnections());
    assertEquals(30000, config.getConnectionMaxIdleTimeMs());
    assertEquals(60000, config.getConnectionTimeToLiveMs());
    assertTrue(config.hasCustomConnectionPoolSettings());
  }

  @Test
  public void testZeroValuesAreValid() {
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withMaxConnections(0)
            .withConnectionMaxIdleTimeMs(0)
            .withConnectionTimeToLiveMs(0)
            .build();

    assertEquals(0, config.getMaxConnections());
    assertEquals(0, config.getConnectionMaxIdleTimeMs());
    assertEquals(0, config.getConnectionTimeToLiveMs());
    assertFalse(config.hasCustomConnectionPoolSettings());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNegativeMaxConnectionsThrowsException() {
    AlternatorConfig.builder().withMaxConnections(-1).build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNegativeConnectionMaxIdleTimeThrowsException() {
    AlternatorConfig.builder().withConnectionMaxIdleTimeMs(-1).build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNegativeConnectionTimeToLiveThrowsException() {
    AlternatorConfig.builder().withConnectionTimeToLiveMs(-1).build();
  }

  @Test
  public void testHasCustomConnectionPoolSettingsWithOnlyMaxConnections() {
    AlternatorConfig config = AlternatorConfig.builder().withMaxConnections(50).build();

    assertTrue(config.hasCustomConnectionPoolSettings());
  }

  @Test
  public void testHasCustomConnectionPoolSettingsWithOnlyIdleTime() {
    AlternatorConfig config = AlternatorConfig.builder().withConnectionMaxIdleTimeMs(10000).build();

    assertTrue(config.hasCustomConnectionPoolSettings());
  }

  @Test
  public void testHasCustomConnectionPoolSettingsWithOnlyTtl() {
    AlternatorConfig config = AlternatorConfig.builder().withConnectionTimeToLiveMs(120000).build();

    assertTrue(config.hasCustomConnectionPoolSettings());
  }
}
