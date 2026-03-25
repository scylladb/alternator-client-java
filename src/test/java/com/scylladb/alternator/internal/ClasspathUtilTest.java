package com.scylladb.alternator.internal;

import static org.junit.Assert.*;

import org.junit.Test;

/** Unit tests for {@link ClasspathUtil}. */
public class ClasspathUtilTest {

  @Test
  public void testKnownClassIsAvailable() {
    assertTrue(ClasspathUtil.isClassAvailable("java.lang.String"));
  }

  @Test
  public void testNonexistentClassIsNotAvailable() {
    assertFalse(ClasspathUtil.isClassAvailable("com.nonexistent.NoSuchClass"));
  }
}
