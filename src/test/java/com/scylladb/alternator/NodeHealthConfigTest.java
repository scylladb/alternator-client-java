package com.scylladb.alternator;

import static org.junit.Assert.*;

import org.junit.Test;

public class NodeHealthConfigTest {
  @Test
  public void defaultConsecutiveFailureThresholdIsTen() {
    assertEquals(10, NodeHealthConfig.DEFAULT_CONSECUTIVE_FAILURE_THRESHOLD);
    assertEquals(10, NodeHealthConfig.getDefault().getConsecutiveFailureThreshold());
    assertEquals(10, NodeHealthConfig.builder().build().getConsecutiveFailureThreshold());
  }
}
