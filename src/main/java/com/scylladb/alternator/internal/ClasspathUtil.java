package com.scylladb.alternator.internal;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Utility for classpath presence checks.
 *
 * @since 2.1.0
 */
final class ClasspathUtil {

  private static final Logger logger = Logger.getLogger(ClasspathUtil.class.getName());

  private ClasspathUtil() {}

  /**
   * Checks whether a class is available on the classpath without triggering static initializers.
   *
   * <p>Uses the thread's context classloader so that library code running inside application
   * servers, OSGi containers, or other environments with classloader hierarchies can see the
   * application's dependencies.
   *
   * @param className fully-qualified class name
   * @return {@code true} if the class can be loaded
   */
  static boolean isClassAvailable(String className) {
    try {
      ClassLoader cl = Thread.currentThread().getContextClassLoader();
      if (cl == null) {
        cl = ClasspathUtil.class.getClassLoader();
      }
      Class.forName(className, false, cl);
      return true;
    } catch (ClassNotFoundException e) {
      return false;
    } catch (LinkageError e) {
      logger.log(
          Level.FINE,
          "Class {0} found but failed to link: {1}",
          new Object[] {className, e.getMessage()});
      return false;
    } catch (SecurityException e) {
      logger.log(
          Level.WARNING,
          "Class {0} blocked by security manager: {1}",
          new Object[] {className, e.getMessage()});
      return false;
    }
  }
}
