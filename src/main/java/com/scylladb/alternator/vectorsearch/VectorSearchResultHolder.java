// Copyright 2026-present ScyllaDB
//
// SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

package com.scylladb.alternator.vectorsearch;

import java.util.List;

/**
 * Mutable holder used by {@link VectorSearchInterceptor} to pass extra response fields back to
 * {@link VectorSearchSupport} without exposing internal state to callers.
 */
final class VectorSearchResultHolder {

  private List<Double> scores;
  private List<VectorIndex> vectorIndexes;

  List<Double> getScores() {
    return scores;
  }

  void setScores(List<Double> scores) {
    this.scores = scores;
  }

  List<VectorIndex> getVectorIndexes() {
    return vectorIndexes;
  }

  void setVectorIndexes(List<VectorIndex> vectorIndexes) {
    this.vectorIndexes = vectorIndexes;
  }
}
