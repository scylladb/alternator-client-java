// Copyright 2026-present ScyllaDB
//
// SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

package com.scylladb.alternator.vectorsearch;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * Parameters for a vector similarity search in a {@code Query} request.
 *
 * <p>Carries the query vector and optional flags that control the search behavior. Set this on a
 * {@code QueryRequest} via {@link VectorSearchSupport#query}.
 *
 * <p>The query vector may be supplied in two forms:
 *
 * <ul>
 *   <li>{@code float[]} — serialized as the compact {@code FLOAT32VECTOR} wire format.
 *   <li>{@link AttributeValue} — serialized as the standard DynamoDB JSON format; useful when the
 *       attribute was stored without the {@code FLOAT32VECTOR} optimization.
 * </ul>
 *
 * <p>Example:
 *
 * <pre>{@code
 * VectorSearch vs = VectorSearch.builder()
 *     .queryVector(new float[]{0.1f, 0.2f, 0.3f})
 *     .returnScores(true)
 *     .build();
 * }</pre>
 */
public final class VectorSearch {

  private final float[] queryVectorFloats;
  private final AttributeValue queryVectorAttributeValue;
  private final boolean returnScores;

  private VectorSearch(Builder builder) {
    this.queryVectorFloats = builder.queryVectorFloats;
    this.queryVectorAttributeValue = builder.queryVectorAttributeValue;
    this.returnScores = builder.returnScores;
  }

  /**
   * Returns the query vector as a {@code float[]}, or {@code null} if the vector was provided as an
   * {@link AttributeValue}.
   */
  public float[] queryVectorFloats() {
    return queryVectorFloats;
  }

  /**
   * Returns the query vector as an {@link AttributeValue}, or {@code null} if the vector was
   * provided as a {@code float[]}.
   */
  public AttributeValue queryVectorAttributeValue() {
    return queryVectorAttributeValue;
  }

  /**
   * Returns whether the server should return per-item similarity scores alongside the results.
   * Scores are accessible via {@link VectorQueryResult#scores()}.
   */
  public boolean returnScores() {
    return returnScores;
  }

  /** Returns a new builder for {@link VectorSearch}. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builder for {@link VectorSearch}. */
  public static final class Builder {
    private float[] queryVectorFloats;
    private AttributeValue queryVectorAttributeValue;
    private boolean returnScores;

    private Builder() {}

    /**
     * Sets the query vector as a float array. It will be sent to Alternator using the compact
     * {@code FLOAT32VECTOR} wire encoding.
     */
    public Builder queryVector(float... queryVector) {
      this.queryVectorFloats = queryVector;
      this.queryVectorAttributeValue = null;
      return this;
    }

    /**
     * Sets the query vector as an {@link AttributeValue}. Use this when the vectors in the table
     * were stored using the standard DynamoDB list type rather than the {@code FLOAT32VECTOR}
     * format.
     */
    public Builder queryVector(AttributeValue queryVector) {
      this.queryVectorAttributeValue = queryVector;
      this.queryVectorFloats = null;
      return this;
    }

    /**
     * When {@code true}, asks the server to include per-item similarity scores in the response.
     * Access them via {@link VectorQueryResult#scores()}.
     */
    public Builder returnScores(boolean returnScores) {
      this.returnScores = returnScores;
      return this;
    }

    /** Builds the {@link VectorSearch}. */
    public VectorSearch build() {
      if (queryVectorFloats == null && queryVectorAttributeValue == null) {
        throw new IllegalStateException("queryVector must be set");
      }
      return new VectorSearch(this);
    }
  }
}
