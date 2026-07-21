// Copyright 2026-present ScyllaDB
//
// SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

package com.scylladb.alternator.vectorsearch;

/**
 * Describes a vector index to delete via {@code UpdateTable}.
 *
 * @see VectorIndexUpdate
 */
public final class DeleteVectorIndexAction {

  private final String indexName;

  private DeleteVectorIndexAction(Builder builder) {
    this.indexName = builder.indexName;
  }

  /** Returns the name of the vector index to delete. */
  public String indexName() {
    return indexName;
  }

  /** Returns a new builder for {@link DeleteVectorIndexAction}. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builder for {@link DeleteVectorIndexAction}. */
  public static final class Builder {
    private String indexName;

    private Builder() {}

    /** Sets the index name. */
    public Builder indexName(String indexName) {
      this.indexName = indexName;
      return this;
    }

    /** Builds the {@link DeleteVectorIndexAction}. */
    public DeleteVectorIndexAction build() {
      if (indexName == null) {
        throw new IllegalStateException("indexName must be set");
      }
      return new DeleteVectorIndexAction(this);
    }
  }
}
