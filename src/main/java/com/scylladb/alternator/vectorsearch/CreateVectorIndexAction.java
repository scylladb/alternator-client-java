// Copyright 2026-present ScyllaDB
//
// SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

package com.scylladb.alternator.vectorsearch;

import software.amazon.awssdk.services.dynamodb.model.Projection;

/**
 * Describes a new vector index to create via {@code UpdateTable}.
 *
 * @see VectorIndexUpdate
 */
public final class CreateVectorIndexAction {

  private final String indexName;
  private final VectorAttribute vectorAttribute;
  private final Projection projection;
  private final String similarityFunction;

  private CreateVectorIndexAction(Builder builder) {
    this.indexName = builder.indexName;
    this.vectorAttribute = builder.vectorAttribute;
    this.projection = builder.projection;
    this.similarityFunction = builder.similarityFunction;
  }

  /** Returns the name of the vector index to create. */
  public String indexName() {
    return indexName;
  }

  /** Returns the vector attribute specification. */
  public VectorAttribute vectorAttribute() {
    return vectorAttribute;
  }

  /** Returns the projection, or {@code null} for the server default (ALL). */
  public Projection projection() {
    return projection;
  }

  /** Returns the similarity function, or {@code null} for the server default. */
  public String similarityFunction() {
    return similarityFunction;
  }

  /** Returns a new builder for {@link CreateVectorIndexAction}. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builder for {@link CreateVectorIndexAction}. */
  public static final class Builder {
    private String indexName;
    private VectorAttribute vectorAttribute;
    private Projection projection;
    private String similarityFunction;

    private Builder() {}

    /** Sets the index name. */
    public Builder indexName(String indexName) {
      this.indexName = indexName;
      return this;
    }

    /** Sets the vector attribute specification. */
    public Builder vectorAttribute(VectorAttribute vectorAttribute) {
      this.vectorAttribute = vectorAttribute;
      return this;
    }

    /** Sets the projection. */
    public Builder projection(Projection projection) {
      this.projection = projection;
      return this;
    }

    /** Sets the similarity function. */
    public Builder similarityFunction(String similarityFunction) {
      this.similarityFunction = similarityFunction;
      return this;
    }

    /** Builds the {@link CreateVectorIndexAction}. */
    public CreateVectorIndexAction build() {
      if (indexName == null) {
        throw new IllegalStateException("indexName must be set");
      }
      if (vectorAttribute == null) {
        throw new IllegalStateException("vectorAttribute must be set");
      }
      return new CreateVectorIndexAction(this);
    }
  }
}
