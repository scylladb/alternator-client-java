package com.scylladb.alternator;

/** Configuration for Alternator node health tracking. */
public final class NodeHealthConfig {
  /** Default consecutive health failure threshold before a node is marked down. */
  public static final int DEFAULT_CONSECUTIVE_FAILURE_THRESHOLD = 10;

  /** Default consecutive successful down-node probes before a node enters quarantine. */
  public static final int DEFAULT_DOWN_NODE_RECOVERY_SUCCESS_THRESHOLD = 3;

  /** Default consecutive successful-contact threshold before a quarantined node is promoted. */
  public static final int DEFAULT_QUARANTINE_SUCCESS_THRESHOLD = 10;

  /** Default background period for probing down nodes. */
  public static final long DEFAULT_DOWN_NODE_PROBE_PERIOD_MS = 30_000;

  /** Default routing-attempt interval for sampling quarantined nodes. */
  public static final int DEFAULT_QUARANTINE_TRAFFIC_INTERVAL = 10;

  private final int consecutiveFailureThreshold;
  private final int downNodeRecoverySuccessThreshold;
  private final int quarantineSuccessThreshold;
  private final long downNodeProbePeriodMs;
  private final int quarantineTrafficInterval;
  private final boolean disabled;

  private NodeHealthConfig(
      int consecutiveFailureThreshold,
      int downNodeRecoverySuccessThreshold,
      int quarantineSuccessThreshold,
      long downNodeProbePeriodMs,
      int quarantineTrafficInterval,
      boolean disabled) {
    this.consecutiveFailureThreshold = Math.max(1, consecutiveFailureThreshold);
    this.downNodeRecoverySuccessThreshold = Math.max(1, downNodeRecoverySuccessThreshold);
    this.quarantineSuccessThreshold = Math.max(1, quarantineSuccessThreshold);
    this.downNodeProbePeriodMs = downNodeProbePeriodMs;
    this.quarantineTrafficInterval = Math.max(1, quarantineTrafficInterval);
    this.disabled = disabled;
  }

  /**
   * Returns the default node health configuration.
   *
   * @return default configuration
   */
  public static NodeHealthConfig getDefault() {
    return builder().build();
  }

  /**
   * Returns a configuration with health tracking disabled.
   *
   * @return disabled configuration
   */
  public static NodeHealthConfig disabled() {
    return builder().withDisabled(true).build();
  }

  /**
   * Creates a new builder.
   *
   * @return a builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns the consecutive health failure threshold.
   *
   * @return threshold
   */
  public int getConsecutiveFailureThreshold() {
    return consecutiveFailureThreshold;
  }

  /**
   * Returns the consecutive successful down-node probe threshold for entering quarantine.
   *
   * @return threshold
   */
  public int getDownNodeRecoverySuccessThreshold() {
    return downNodeRecoverySuccessThreshold;
  }

  /**
   * Returns the consecutive successful-contact promotion threshold.
   *
   * @return threshold
   */
  public int getQuarantineSuccessThreshold() {
    return quarantineSuccessThreshold;
  }

  /**
   * Returns the background period for probing down nodes.
   *
   * @return probe period in milliseconds; non-positive disables background probing
   */
  public long getDownNodeProbePeriodMs() {
    return downNodeProbePeriodMs;
  }

  /**
   * Returns the quarantine sampling interval.
   *
   * @return routing-attempt interval
   */
  public int getQuarantineTrafficInterval() {
    return quarantineTrafficInterval;
  }

  /**
   * Returns whether node health tracking is disabled.
   *
   * @return true when disabled
   */
  public boolean isDisabled() {
    return disabled;
  }

  /** Builder for {@link NodeHealthConfig}. */
  public static final class Builder {
    private int consecutiveFailureThreshold = DEFAULT_CONSECUTIVE_FAILURE_THRESHOLD;
    private int downNodeRecoverySuccessThreshold = DEFAULT_DOWN_NODE_RECOVERY_SUCCESS_THRESHOLD;
    private int quarantineSuccessThreshold = DEFAULT_QUARANTINE_SUCCESS_THRESHOLD;
    private long downNodeProbePeriodMs = DEFAULT_DOWN_NODE_PROBE_PERIOD_MS;
    private int quarantineTrafficInterval = DEFAULT_QUARANTINE_TRAFFIC_INTERVAL;
    private boolean disabled = false;

    private Builder() {}

    /**
     * Sets the consecutive health failure threshold. Values less than one are normalized to one.
     *
     * @param threshold threshold value
     * @return this builder
     */
    public Builder withConsecutiveFailureThreshold(int threshold) {
      this.consecutiveFailureThreshold = threshold;
      return this;
    }

    /**
     * Sets the consecutive successful down-node probe threshold for entering quarantine. Values
     * less than one are normalized to one.
     *
     * @param threshold threshold value
     * @return this builder
     */
    public Builder withDownNodeRecoverySuccessThreshold(int threshold) {
      this.downNodeRecoverySuccessThreshold = threshold;
      return this;
    }

    /**
     * Sets the consecutive successful-contact promotion threshold. Values less than one are
     * normalized to one.
     *
     * @param threshold threshold value
     * @return this builder
     */
    public Builder withQuarantineSuccessThreshold(int threshold) {
      this.quarantineSuccessThreshold = threshold;
      return this;
    }

    /**
     * Sets the background period for probing down nodes.
     *
     * @param periodMs period in milliseconds; non-positive disables background probing
     * @return this builder
     */
    public Builder withDownNodeProbePeriodMs(long periodMs) {
      this.downNodeProbePeriodMs = periodMs;
      return this;
    }

    /**
     * Sets the quarantine sampling interval. Values less than one are normalized to one.
     *
     * @param interval routing-attempt interval
     * @return this builder
     */
    public Builder withQuarantineTrafficInterval(int interval) {
      this.quarantineTrafficInterval = interval;
      return this;
    }

    /**
     * Enables or disables node health tracking.
     *
     * @param disabled true to disable health tracking
     * @return this builder
     */
    public Builder withDisabled(boolean disabled) {
      this.disabled = disabled;
      return this;
    }

    /**
     * Builds a node health configuration.
     *
     * @return node health configuration
     */
    public NodeHealthConfig build() {
      return new NodeHealthConfig(
          consecutiveFailureThreshold,
          downNodeRecoverySuccessThreshold,
          quarantineSuccessThreshold,
          downNodeProbePeriodMs,
          quarantineTrafficInterval,
          disabled);
    }
  }
}
