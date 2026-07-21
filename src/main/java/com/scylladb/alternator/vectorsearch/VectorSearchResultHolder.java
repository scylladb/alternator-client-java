/*
 * Copyright ScyllaDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
