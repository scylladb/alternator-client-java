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
 * Represents a single vector index change in an {@code UpdateTable} request.
 *
 * <p>Exactly one of {@link #create()} or {@link #delete()} must be set.
 *
 * <p>Example:
 *
 * <pre>{@code
 * VectorIndexUpdate update = VectorIndexUpdate.builder()
 *     .create(CreateVectorIndexAction.builder()
 *         .indexName("my-index")
 *         .vectorAttribute(VectorAttribute.builder()
 *             .attributeName("embedding")
 *             .dimensions(128)
 *             .build())
 *         .build())
 *     .build();
 * }</pre>
 */
public final class VectorIndexUpdate {

  private final CreateVectorIndexAction create;
  private final DeleteVectorIndexAction delete;

  private VectorIndexUpdate(Builder builder) {
    this.create = builder.create;
    this.delete = builder.delete;
  }

  /** Returns the create action, or {@code null} if this is a delete update. */
  public CreateVectorIndexAction create() {
    return create;
  }

  /** Returns the delete action, or {@code null} if this is a create update. */
  public DeleteVectorIndexAction delete() {
    return delete;
  }

  /** Returns a new builder for {@link VectorIndexUpdate}. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builder for {@link VectorIndexUpdate}. */
  public static final class Builder {
    private CreateVectorIndexAction create;
    private DeleteVectorIndexAction delete;

    private Builder() {}

    /** Sets the create action. */
    public Builder create(CreateVectorIndexAction create) {
      this.create = create;
      return this;
    }

    /** Sets the delete action. */
    public Builder delete(DeleteVectorIndexAction delete) {
      this.delete = delete;
      return this;
    }

    /** Builds the {@link VectorIndexUpdate}. */
    public VectorIndexUpdate build() {
      if (create == null && delete == null) {
        throw new IllegalStateException("exactly one of create or delete must be set");
      }
      if (create != null && delete != null) {
        throw new IllegalStateException("exactly one of create or delete must be set, not both");
      }
      return new VectorIndexUpdate(this);
    }
  }
}
