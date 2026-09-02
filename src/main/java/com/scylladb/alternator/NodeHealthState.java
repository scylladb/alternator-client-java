package com.scylladb.alternator;

/** Current routing state for an Alternator node. */
public enum NodeHealthState {
  /** Node receives normal traffic. */
  ACTIVE,

  /** Node recovered from down state and receives sampled verification traffic. */
  QUARANTINED,

  /** Node is excluded from normal routing and is probed in the background. */
  DOWN
}
