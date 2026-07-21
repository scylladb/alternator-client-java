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
 * Describes the vector attribute for a {@link VectorIndex}.
 *
 * <p>Specifies the item attribute that holds vector data and its dimensionality.
 */
public final class VectorAttribute {

  private final String attributeName;
  private final int dimensions;

  private VectorAttribute(Builder builder) {
    this.attributeName = builder.attributeName;
    this.dimensions = builder.dimensions;
  }

  /** Returns the name of the item attribute that stores vector data. */
  public String attributeName() {
    return attributeName;
  }

  /** Returns the number of dimensions in the vector. */
  public int dimensions() {
    return dimensions;
  }

  /** Returns a new builder for {@link VectorAttribute}. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builder for {@link VectorAttribute}. */
  public static final class Builder {
    private String attributeName;
    private int dimensions;

    private Builder() {}

    /** Sets the name of the attribute that stores vector data. */
    public Builder attributeName(String attributeName) {
      this.attributeName = attributeName;
      return this;
    }

    /** Sets the number of dimensions in the vector. */
    public Builder dimensions(int dimensions) {
      this.dimensions = dimensions;
      return this;
    }

    /** Builds the {@link VectorAttribute}. */
    public VectorAttribute build() {
      if (attributeName == null) {
        throw new IllegalStateException("attributeName must be set");
      }
      if (dimensions <= 0) {
        throw new IllegalStateException("dimensions must be a positive integer");
      }
      return new VectorAttribute(this);
    }
  }
}
