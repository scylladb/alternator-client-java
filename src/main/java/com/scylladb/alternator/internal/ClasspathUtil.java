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
