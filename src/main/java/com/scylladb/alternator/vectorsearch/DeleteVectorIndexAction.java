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
