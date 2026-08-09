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

import java.util.Locale;

/** Logical-host formatting that keeps HTTP authority separate from TLS peer identity. */
final class LogicalHost {

  private LogicalHost() {}

  static String tlsPeerName(String host) {
    String peer = withoutBrackets(host);
    if (!isIpLiteral(peer) && peer.endsWith(".") && peer.length() > 1) {
      peer = peer.substring(0, peer.length() - 1);
    }
    return peer;
  }

  static String authority(String host, int port) {
    String unbracketed = withoutBrackets(host);
    String authorityHost = unbracketed.indexOf(':') >= 0 ? "[" + unbracketed + "]" : unbracketed;
    return port >= 0 ? authorityHost + ":" + port : authorityHost;
  }

  static boolean sameEndpoint(String first, String second) {
    return comparisonName(first).equals(comparisonName(second));
  }

  static boolean isIpLiteral(String host) {
    String unbracketed = withoutBrackets(host);
    if (unbracketed.indexOf(':') >= 0) {
      return true;
    }
    String[] components = unbracketed.split("\\.", -1);
    if (components.length != 4) {
      return false;
    }
    for (String component : components) {
      if (component.isEmpty()
          || component.length() > 3
          || (component.length() > 1 && component.charAt(0) == '0')) {
        return false;
      }
      int value = 0;
      for (int i = 0; i < component.length(); i++) {
        char ch = component.charAt(i);
        if (ch < '0' || ch > '9') {
          return false;
        }
        value = value * 10 + ch - '0';
      }
      if (value > 255) {
        return false;
      }
    }
    return true;
  }

  static String withoutBrackets(String host) {
    if (host != null
        && host.length() >= 2
        && host.charAt(0) == '['
        && host.charAt(host.length() - 1) == ']') {
      return host.substring(1, host.length() - 1);
    }
    return host;
  }

  private static String comparisonName(String host) {
    String result = withoutBrackets(host).toLowerCase(Locale.ROOT);
    if (!isIpLiteral(result) && result.endsWith(".") && result.length() > 1) {
      result = result.substring(0, result.length() - 1);
    }
    return result;
  }
}
