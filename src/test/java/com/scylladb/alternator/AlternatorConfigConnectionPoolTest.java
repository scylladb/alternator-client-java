package com.scylladb.alternator;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
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

    assertEquals(AlternatorConfig.DEFAULT_MAX_CONNECTIONS, config.getMaxConnections());
    assertEquals(
        AlternatorConfig.DEFAULT_CONNECTION_MAX_IDLE_TIME_MS, config.getConnectionMaxIdleTimeMs());
    assertEquals(
        AlternatorConfig.DEFAULT_CONNECTION_TIME_TO_LIVE_MS, config.getConnectionTimeToLiveMs());
    assertEquals(
        AlternatorConfig.DEFAULT_CONNECTION_ACQUISITION_TIMEOUT_MS,
        config.getConnectionAcquisitionTimeoutMs());
    assertEquals(AlternatorConfig.DEFAULT_CONNECTION_TIMEOUT_MS, config.getConnectionTimeoutMs());
  }

  @Test
  public void testDefaultConstants() {
    assertEquals(400, AlternatorConfig.DEFAULT_MAX_CONNECTIONS);
    assertEquals(600_000, AlternatorConfig.DEFAULT_CONNECTION_MAX_IDLE_TIME_MS);
    assertEquals(0, AlternatorConfig.DEFAULT_CONNECTION_TIME_TO_LIVE_MS);
    assertEquals(10_000, AlternatorConfig.DEFAULT_CONNECTION_ACQUISITION_TIMEOUT_MS);
    assertEquals(15_000, AlternatorConfig.DEFAULT_CONNECTION_TIMEOUT_MS);
  }

  @Test
  public void testCustomMaxConnections() {
    AlternatorConfig config = AlternatorConfig.builder().withMaxConnections(100).build();

    assertEquals(100, config.getMaxConnections());
  }

  @Test
  public void testCustomConnectionMaxIdleTime() {
    AlternatorConfig config = AlternatorConfig.builder().withConnectionMaxIdleTimeMs(30000).build();

    assertEquals(30000, config.getConnectionMaxIdleTimeMs());
  }

  @Test
  public void testCustomConnectionTimeToLive() {
    AlternatorConfig config = AlternatorConfig.builder().withConnectionTimeToLiveMs(60000).build();

    assertEquals(60000, config.getConnectionTimeToLiveMs());
  }

  @Test
  public void testCustomConnectionAcquisitionTimeout() {
    AlternatorConfig config =
        AlternatorConfig.builder().withConnectionAcquisitionTimeoutMs(5000).build();

    assertEquals(5000, config.getConnectionAcquisitionTimeoutMs());
  }

  @Test
  public void testCustomConnectionTimeout() {
    AlternatorConfig config = AlternatorConfig.builder().withConnectionTimeoutMs(3000).build();

    assertEquals(3000, config.getConnectionTimeoutMs());
  }

  @Test
  public void testAllConnectionPoolSettingsTogether() {
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withMaxConnections(200)
            .withConnectionMaxIdleTimeMs(30000)
            .withConnectionTimeToLiveMs(60000)
            .withConnectionAcquisitionTimeoutMs(5000)
            .withConnectionTimeoutMs(3000)
            .build();

    assertEquals(200, config.getMaxConnections());
    assertEquals(30000, config.getConnectionMaxIdleTimeMs());
    assertEquals(60000, config.getConnectionTimeToLiveMs());
    assertEquals(5000, config.getConnectionAcquisitionTimeoutMs());
    assertEquals(3000, config.getConnectionTimeoutMs());
  }

  @Test
  public void testZeroValuesAreValidForTimeSettings() {
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withConnectionMaxIdleTimeMs(0)
            .withConnectionTimeToLiveMs(0)
            .withConnectionAcquisitionTimeoutMs(0)
            .withConnectionTimeoutMs(0)
            .build();

    assertEquals(0, config.getConnectionMaxIdleTimeMs());
    assertEquals(0, config.getConnectionTimeToLiveMs());
    assertEquals(0, config.getConnectionAcquisitionTimeoutMs());
    assertEquals(0, config.getConnectionTimeoutMs());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testZeroMaxConnectionsThrowsException() {
    AlternatorConfig.builder().withMaxConnections(0).build();
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

  @Test(expected = IllegalArgumentException.class)
  public void testNegativeConnectionAcquisitionTimeoutThrowsException() {
    AlternatorConfig.builder().withConnectionAcquisitionTimeoutMs(-1).build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNegativeConnectionTimeoutThrowsException() {
    AlternatorConfig.builder().withConnectionTimeoutMs(-1).build();
  }

  @Test
  public void testZeroIdleTimeWarnsAboutStaleConnections() {
    List<LogRecord> records = new ArrayList<>();
    Logger logger = Logger.getLogger(AlternatorConfig.class.getName());
    Handler handler = new CapturingHandler(records);
    logger.addHandler(handler);
    try {
      AlternatorConfig.builder().withConnectionMaxIdleTimeMs(0).build();

      assertEquals("Expected exactly one warning", 1, records.size());
      LogRecord record = records.get(0);
      assertEquals(Level.WARNING, record.getLevel());
      assertTrue(
          "Warning should mention disabling idle connection eviction",
          record.getMessage().contains("disables idle connection eviction"));
    } finally {
      logger.removeHandler(handler);
    }
  }

  @Test
  public void testDefaultIdleTimeNoWarning() {
    List<LogRecord> records = new ArrayList<>();
    Logger logger = Logger.getLogger(AlternatorConfig.class.getName());
    Handler handler = new CapturingHandler(records);
    logger.addHandler(handler);
    try {
      AlternatorConfig.builder().build();

      assertTrue("No warning expected for default config", records.isEmpty());
    } finally {
      logger.removeHandler(handler);
    }
  }

  @Test
  public void testNonZeroIdleTimeNoWarning() {
    List<LogRecord> records = new ArrayList<>();
    Logger logger = Logger.getLogger(AlternatorConfig.class.getName());
    Handler handler = new CapturingHandler(records);
    logger.addHandler(handler);
    try {
      AlternatorConfig.builder().withConnectionMaxIdleTimeMs(30000).build();

      assertTrue("No warning expected for non-zero idle time", records.isEmpty());
    } finally {
      logger.removeHandler(handler);
    }
  }

  private static class CapturingHandler extends Handler {
    private final List<LogRecord> records;

    CapturingHandler(List<LogRecord> records) {
      this.records = records;
    }

    @Override
    public void publish(LogRecord record) {
      records.add(record);
    }

    @Override
    public void flush() {}

    @Override
    public void close() {}
  }
}
