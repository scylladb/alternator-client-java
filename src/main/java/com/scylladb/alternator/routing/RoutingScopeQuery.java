package com.scylladb.alternator.routing;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

final class RoutingScopeQuery {

  private RoutingScopeQuery() {}

  static String encodeValue(String value) {
    return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
  }
}
