// Copyright 2026-present ScyllaDB
//
// SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

package com.scylladb.alternator.vectorsearch;

import software.amazon.awssdk.services.dynamodb.model.Projection;

/**
 * Describes a vector index for Alternator's vector search feature.
 *
 * <p>Used in {@code CreateTable} and {@code UpdateTable} requests to define a vector index, and
 * returned in {@code DescribeTable} and {@code CreateTable} responses.
 *
 * <p>Example:
 *
 * <pre>{@code
 * VectorIndex vi = VectorIndex.builder()
 *     .indexName("my-vector-index")
 *     .vectorAttribute(VectorAttribute.builder()
 *         .attributeName("embedding")
 *         .dimensions(128)
 *         .build())
 *     .similarityFunction("COSINE")
 *     .build();
 * }</pre>
 */
public final class VectorIndex {

  private final String indexName;
  private final VectorAttribute vectorAttribute;
  private final Projection projection;
  private final String similarityFunction;
  // Response-only fields:
  private final String indexStatus;
  private final Boolean backfilling;

  private VectorIndex(Builder builder) {
    this.indexName = builder.indexName;
    this.vectorAttribute = builder.vectorAttribute;
    this.projection = builder.projection;
    this.similarityFunction = builder.similarityFunction;
    this.indexStatus = builder.indexStatus;
    this.backfilling = builder.backfilling;
  }

  /** Returns the name of this vector index. */
  public String indexName() {
    return indexName;
  }

  /** Returns the vector attribute specification. */
  public VectorAttribute vectorAttribute() {
    return vectorAttribute;
  }

  /** Returns the projection for this index, or {@code null} if using the default (ALL). */
  public Projection projection() {
    return projection;
  }

  /**
   * Returns the similarity function (e.g., {@code "COSINE"}, {@code "DOT_PRODUCT"}, {@code
   * "EUCLIDEAN"}), or {@code null} to use the server default.
   */
  public String similarityFunction() {
    return similarityFunction;
  }

  /**
   * Returns the index status as reported by the server (e.g., {@code "CREATING"}, {@code
   * "ACTIVE"}). Only populated in responses from {@code DescribeTable} or {@code CreateTable}.
   */
  public String indexStatus() {
    return indexStatus;
  }

  /**
   * Returns whether the index is backfilling, or {@code null} if not reported by the server. Only
   * populated in responses from {@code DescribeTable} or {@code CreateTable}.
   */
  public Boolean backfilling() {
    return backfilling;
  }

  /** Returns a new builder for {@link VectorIndex}. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builder for {@link VectorIndex}. */
  public static final class Builder {
    private String indexName;
    private VectorAttribute vectorAttribute;
    private Projection projection;
    private String similarityFunction;
    private String indexStatus;
    private Boolean backfilling;

    private Builder() {}

    /** Sets the name of the vector index. */
    public Builder indexName(String indexName) {
      this.indexName = indexName;
      return this;
    }

    /** Sets the vector attribute specification. */
    public Builder vectorAttribute(VectorAttribute vectorAttribute) {
      this.vectorAttribute = vectorAttribute;
      return this;
    }

    /** Sets the projection for this index. If not set, the server defaults to ALL. */
    public Builder projection(Projection projection) {
      this.projection = projection;
      return this;
    }

    /**
     * Sets the similarity function (e.g., {@code "COSINE"}, {@code "DOT_PRODUCT"}, {@code
     * "EUCLIDEAN"}). If not set, the server uses its default.
     */
    public Builder similarityFunction(String similarityFunction) {
      this.similarityFunction = similarityFunction;
      return this;
    }

    /** Sets the index status (populated from server responses). */
    public Builder indexStatus(String indexStatus) {
      this.indexStatus = indexStatus;
      return this;
    }

    /** Sets the backfilling flag (populated from server responses). */
    public Builder backfilling(Boolean backfilling) {
      this.backfilling = backfilling;
      return this;
    }

    /** Builds the {@link VectorIndex}. */
    public VectorIndex build() {
      if (indexName == null) {
        throw new IllegalStateException("indexName must be set");
      }
      if (vectorAttribute == null) {
        throw new IllegalStateException("vectorAttribute must be set");
      }
      return new VectorIndex(this);
    }
  }
}
