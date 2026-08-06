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

import static org.junit.Assert.*;

import org.junit.Test;

/**
 * Unit tests for RequestCompressionAlgorithm enum.
 *
 * @author dmitry.kropachev
 */
public class RequestCompressionAlgorithmTest {

  @Test
  public void testNoneIsNotEnabled() {
    assertFalse(RequestCompressionAlgorithm.NONE.isEnabled());
  }

  @Test
  public void testGzipIsEnabled() {
    assertTrue(RequestCompressionAlgorithm.GZIP.isEnabled());
  }

  @Test
  public void testAllValuesExist() {
    // Verify the expected enum values exist
    RequestCompressionAlgorithm none = RequestCompressionAlgorithm.NONE;
    RequestCompressionAlgorithm gzip = RequestCompressionAlgorithm.GZIP;

    assertNotNull(none);
    assertNotNull(gzip);

    // Verify we have exactly 2 values
    assertEquals(2, RequestCompressionAlgorithm.values().length);
  }

  @Test
  public void testValueOf() {
    assertEquals(RequestCompressionAlgorithm.NONE, RequestCompressionAlgorithm.valueOf("NONE"));
    assertEquals(RequestCompressionAlgorithm.GZIP, RequestCompressionAlgorithm.valueOf("GZIP"));
  }

  @Test
  public void testIsEnabledConsistency() {
    // All algorithms except NONE should be enabled
    for (RequestCompressionAlgorithm algorithm : RequestCompressionAlgorithm.values()) {
      if (algorithm == RequestCompressionAlgorithm.NONE) {
        assertFalse(algorithm.name() + " should not be enabled", algorithm.isEnabled());
      } else {
        assertTrue(algorithm.name() + " should be enabled", algorithm.isEnabled());
      }
    }
  }
}
