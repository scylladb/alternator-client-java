package com.scylladb.alternator;

/** Configuration for Alternator node health tracking. */
public final class NodeHealthConfig {
  /**
   * Default consecutive DynamoDB traffic failure threshold before an active node is marked down.
   */
  public static final int DEFAULT_CONSECUTIVE_FAILURE_THRESHOLD = 10;

  /** Default consecutive successful down-node probes before a node enters quarantine. */
  public static final int DEFAULT_DOWN_NODE_RECOVERY_SUCCESS_THRESHOLD = 3;

  /** Default consecutive successful-contact threshold before a quarantined node is promoted. */
  public static final int DEFAULT_QUARANTINE_SUCCESS_THRESHOLD = 10;

  /** Default consecutive traffic failure threshold before a quarantined node is marked down. */
  public static final int DEFAULT_QUARANTINE_FAILURE_THRESHOLD = 3;

  /** Default background period for probing down nodes. */
  public static final long DEFAULT_DOWN_NODE_PROBE_PERIOD_MS = 30_000;

  /** Default logical-query interval for sampling quarantined nodes. */
  public static final int DEFAULT_QUARANTINE_TRAFFIC_INTERVAL = 10;

  /** Default client-traffic idle period after which quarantine sampling is armed. */
  public static final long DEFAULT_QUARANTINE_TRAFFIC_IDLE_PERIOD_MS = 100;

  private final int consecutiveFailureThreshold;
  private final int downNodeRecoverySuccessThreshold;
  private final int quarantineSuccessThreshold;
  private final int quarantineFailureThreshold;
  private final long downNodeProbePeriodMs;
  private final int quarantineTrafficInterval;
  private final long quarantineTrafficIdlePeriodMs;
  private final boolean disabled;

  private NodeHealthConfig(
      int consecutiveFailureThreshold,
      int downNodeRecoverySuccessThreshold,
      int quarantineSuccessThreshold,
      int quarantineFailureThreshold,
      long downNodeProbePeriodMs,
      int quarantineTrafficInterval,
      long quarantineTrafficIdlePeriodMs,
      boolean disabled) {
    if (downNodeProbePeriodMs <= 0) {
      throw new IllegalArgumentException(
          "downNodeProbePeriodMs must be positive, but was: " + downNodeProbePeriodMs);
    }
    this.consecutiveFailureThreshold = Math.max(1, consecutiveFailureThreshold);
    this.downNodeRecoverySuccessThreshold = Math.max(1, downNodeRecoverySuccessThreshold);
    this.quarantineSuccessThreshold = Math.max(1, quarantineSuccessThreshold);
    this.quarantineFailureThreshold = Math.max(1, quarantineFailureThreshold);
    this.downNodeProbePeriodMs = downNodeProbePeriodMs;
    this.quarantineTrafficInterval = Math.max(1, quarantineTrafficInterval);
    this.quarantineTrafficIdlePeriodMs = quarantineTrafficIdlePeriodMs;
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
   * Returns the consecutive DynamoDB traffic failure threshold for active nodes.
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
   * Returns the consecutive traffic failure threshold before a quarantined node is marked down.
   *
   * @return threshold
   */
  public int getQuarantineFailureThreshold() {
    return quarantineFailureThreshold;
  }

  /**
   * Returns the background period for probing down nodes.
   *
   * @return positive probe period in milliseconds
   */
  public long getDownNodeProbePeriodMs() {
    return downNodeProbePeriodMs;
  }

  /**
   * Returns the number of logical DynamoDB queries between quarantine samples.
   *
   * @return logical-query interval
   */
  public int getQuarantineTrafficInterval() {
    return quarantineTrafficInterval;
  }

  /**
   * Returns the client-traffic idle period after which quarantine sampling is armed.
   *
   * @return idle period in milliseconds; non-positive disables idle-triggered sampling
   */
  public long getQuarantineTrafficIdlePeriodMs() {
    return quarantineTrafficIdlePeriodMs;
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
    private int quarantineFailureThreshold = DEFAULT_QUARANTINE_FAILURE_THRESHOLD;
    private long downNodeProbePeriodMs = DEFAULT_DOWN_NODE_PROBE_PERIOD_MS;
    private int quarantineTrafficInterval = DEFAULT_QUARANTINE_TRAFFIC_INTERVAL;
    private long quarantineTrafficIdlePeriodMs = DEFAULT_QUARANTINE_TRAFFIC_IDLE_PERIOD_MS;
    private boolean disabled = false;

    private Builder() {}

    /**
     * Sets the consecutive DynamoDB traffic failure threshold for active nodes. Values less than
     * one are normalized to one.
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
     * Sets the consecutive traffic failure threshold before a quarantined node is marked down.
     * Values less than one are normalized to one.
     *
     * @param threshold threshold value
     * @return this builder
     */
    public Builder withQuarantineFailureThreshold(int threshold) {
      this.quarantineFailureThreshold = threshold;
      return this;
    }

    /**
     * Sets the background period for probing down nodes.
     *
     * @param periodMs period in milliseconds; must be positive
     * @return this builder
     */
    public Builder withDownNodeProbePeriodMs(long periodMs) {
      this.downNodeProbePeriodMs = periodMs;
      return this;
    }

    /**
     * Sets the number of logical DynamoDB queries between quarantine samples. A logical query is
     * counted once; retries of that query do not advance the interval. Values less than one are
     * normalized to one.
     *
     * @param interval logical-query interval
     * @return this builder
     */
    public Builder withQuarantineTrafficInterval(int interval) {
      this.quarantineTrafficInterval = interval;
      return this;
    }

    /**
     * Sets the client-traffic idle period after which quarantine sampling is armed.
     *
     * @param periodMs idle period in milliseconds; non-positive disables idle-triggered sampling
     * @return this builder
     */
    public Builder withQuarantineTrafficIdlePeriodMs(long periodMs) {
      this.quarantineTrafficIdlePeriodMs = periodMs;
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
     * @throws IllegalArgumentException if the down-node probe period is not positive
     */
    public NodeHealthConfig build() {
      return new NodeHealthConfig(
          consecutiveFailureThreshold,
          downNodeRecoverySuccessThreshold,
          quarantineSuccessThreshold,
          quarantineFailureThreshold,
          downNodeProbePeriodMs,
          quarantineTrafficInterval,
          quarantineTrafficIdlePeriodMs,
          disabled);
    }
  }
}
