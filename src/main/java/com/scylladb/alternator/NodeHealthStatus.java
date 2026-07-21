package com.scylladb.alternator;

/** Snapshot of one node's health state. */
public final class NodeHealthStatus {
  private final NodeHealthState state;
  private final int consecutiveFailures;
  private final int consecutiveSuccesses;
  private final long updatedAtNanos;

  /**
   * Creates a node health status snapshot.
   *
   * @param state current node state
   * @param consecutiveFailures consecutive health failure observations
   * @param consecutiveSuccesses consecutive successful observations in the current recovery state
   * @param updatedAtNanos monotonic timestamp from {@link System#nanoTime()}
   */
  public NodeHealthStatus(
      NodeHealthState state,
      int consecutiveFailures,
      int consecutiveSuccesses,
      long updatedAtNanos) {
    this.state = state;
    this.consecutiveFailures = consecutiveFailures;
    this.consecutiveSuccesses = consecutiveSuccesses;
    this.updatedAtNanos = updatedAtNanos;
  }

  /**
   * Returns the node state.
   *
   * @return the node state
   */
  public NodeHealthState getState() {
    return state;
  }

  /**
   * Returns consecutive health failure observations.
   *
   * @return consecutive health failure observations
   */
  public int getConsecutiveFailures() {
    return consecutiveFailures;
  }

  /**
   * Returns consecutive successful observations in the current recovery state.
   *
   * @return consecutive successful observations in the current recovery state
   */
  public int getConsecutiveSuccesses() {
    return consecutiveSuccesses;
  }

  /**
   * Returns the monotonic update timestamp.
   *
   * @return timestamp from {@link System#nanoTime()}
   */
  public long getUpdatedAtNanos() {
    return updatedAtNanos;
  }
}
