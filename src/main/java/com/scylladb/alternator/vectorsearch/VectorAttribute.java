// Copyright 2026-present ScyllaDB
//
// SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

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
