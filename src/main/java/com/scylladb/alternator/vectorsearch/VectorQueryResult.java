// Copyright 2026-present ScyllaDB
//
// SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

package com.scylladb.alternator.vectorsearch;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConsumedCapacity;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;

/**
 * The result of a vector search {@code Query} request.
 *
 * <p>Wraps the standard {@link QueryResponse} and adds access to per-item similarity scores
 * returned by Alternator when {@link VectorSearch#returnScores()} is {@code true}.
 *
 * <p>Similarity scores are in the same order as the items returned by {@link #items()}.
 */
public final class VectorQueryResult {

  private final QueryResponse response;
  private final List<Double> scores;

  public VectorQueryResult(QueryResponse response, List<Double> scores) {
    this.response = response;
    this.scores = scores != null ? Collections.unmodifiableList(scores) : Collections.emptyList();
  }

  /** Returns the underlying {@link QueryResponse}. */
  public QueryResponse response() {
    return response;
  }

  /**
   * Returns the list of items returned by the query.
   *
   * <p>Convenience delegate for {@link QueryResponse#items()}.
   */
  public List<Map<String, AttributeValue>> items() {
    return response.items();
  }

  /**
   * Returns the number of items that matched the query before any {@code Limit} was applied.
   *
   * <p>Convenience delegate for {@link QueryResponse#count()}.
   */
  public int count() {
    return response.count();
  }

  /**
   * Returns per-item similarity scores in the same order as {@link #items()}, or an empty list if
   * scores were not requested or the server did not return them.
   */
  public List<Double> scores() {
    return scores;
  }

  /** Returns the consumed capacity, or {@code null} if not requested. */
  public ConsumedCapacity consumedCapacity() {
    return response.consumedCapacity();
  }
}
