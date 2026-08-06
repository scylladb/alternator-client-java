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
package com.scylladb.alternator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** Enumeration of supported HTTP response compression algorithms. */
public enum ResponseCompressionAlgorithm {
  /** GZIP response compression. */
  GZIP("gzip"),

  /** DEFLATE response compression. */
  DEFLATE("deflate");

  private static final List<ResponseCompressionAlgorithm> SUPPORTED_ALGORITHMS =
      Collections.unmodifiableList(Arrays.asList(GZIP, DEFLATE));

  private final String contentEncoding;

  ResponseCompressionAlgorithm(String contentEncoding) {
    this.contentEncoding = contentEncoding;
  }

  /**
   * Returns the HTTP {@code Content-Encoding} and {@code Accept-Encoding} token for this algorithm.
   *
   * @return the encoding token
   */
  public String contentEncoding() {
    return contentEncoding;
  }

  static List<ResponseCompressionAlgorithm> supportedAlgorithms() {
    return SUPPORTED_ALGORITHMS;
  }

  static List<ResponseCompressionAlgorithm> validatedList(
      Collection<ResponseCompressionAlgorithm> algorithms) {
    if (algorithms == null || algorithms.isEmpty()) {
      throw new IllegalArgumentException(
          "responseCompressionAlgorithms cannot be null or empty. "
              + "Use withResponseCompressionDisabled() to disable response compression.");
    }

    Set<ResponseCompressionAlgorithm> uniqueAlgorithms = new LinkedHashSet<>();
    for (ResponseCompressionAlgorithm algorithm : algorithms) {
      if (algorithm == null) {
        throw new IllegalArgumentException("responseCompressionAlgorithms cannot contain null");
      }
      uniqueAlgorithms.add(algorithm);
    }
    return Collections.unmodifiableList(new ArrayList<>(uniqueAlgorithms));
  }

  static String acceptEncoding(Collection<ResponseCompressionAlgorithm> algorithms) {
    return algorithms.stream()
        .map(ResponseCompressionAlgorithm::contentEncoding)
        .collect(Collectors.joining(", "));
  }

  static Optional<ResponseCompressionAlgorithm> fromContentEncoding(String value) {
    if (value == null) {
      return Optional.empty();
    }
    String token = value.split(";", 2)[0].trim().toLowerCase(Locale.ROOT);
    for (ResponseCompressionAlgorithm algorithm : values()) {
      if (algorithm.contentEncoding.equals(token)) {
        return Optional.of(algorithm);
      }
    }
    return Optional.empty();
  }
}
