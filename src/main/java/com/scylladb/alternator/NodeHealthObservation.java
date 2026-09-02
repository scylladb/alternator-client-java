package com.scylladb.alternator;

/** Result observed for an Alternator node health decision. */
public enum NodeHealthObservation {
  /** A routed DynamoDB request produced a response considered healthy for routing purposes. */
  TRAFFIC_SUCCESS,

  /** A routed DynamoDB request failed before receiving an HTTP response. */
  TRAFFIC_FAILURE,

  /** A local node-health probe, such as {@code GET /localnodes}, completed successfully. */
  PROBE_SUCCESS,

  /** A local node-health probe failed or returned an unhealthy response. */
  PROBE_FAILURE
}
