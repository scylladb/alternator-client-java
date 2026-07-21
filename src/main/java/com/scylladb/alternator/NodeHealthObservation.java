package com.scylladb.alternator;

/** Result observed for an Alternator node health decision. */
public enum NodeHealthObservation {
  /** A routed DynamoDB request completed without a node-health failure. */
  TRAFFIC_SUCCESS,

  /** A routed DynamoDB request hit a transport or authentication health failure. */
  TRAFFIC_FAILURE,

  /** A local node-health probe, such as {@code GET /localnodes}, completed successfully. */
  PROBE_SUCCESS,

  /** A local node-health probe failed or returned an unhealthy response. */
  PROBE_FAILURE
}
