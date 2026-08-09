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

import java.io.IOException;
import java.net.IDN;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

final class LocalNodesResponseParser {
  private final String alternatorScheme;
  private final int alternatorPort;
  private final int maximumNodes;

  private static final Logger logger = Logger.getLogger(LocalNodesResponseParser.class.getName());

  LocalNodesResponseParser(String alternatorScheme, int alternatorPort) {
    this(alternatorScheme, alternatorPort, LiveNodesPollingLimits.DEFAULT_MAX_SNAPSHOT_NODES);
  }

  LocalNodesResponseParser(String alternatorScheme, int alternatorPort, int maximumNodes) {
    this.alternatorScheme = alternatorScheme;
    this.alternatorPort = alternatorPort;
    this.maximumNodes = maximumNodes;
  }

  List<URI> parse(String responseBody) throws InvalidLocalNodesResponseException {
    Set<URI> nodes = new LinkedHashSet<>();
    Set<String> hosts = new JsonStringArrayParser(responseBody, maximumNodes).parse();
    int invalidHosts = 0;
    String invalidSample = null;
    for (String host : hosts) {
      try {
        nodes.add(hostToURI(host));
      } catch (URISyntaxException | MalformedURLException e) {
        invalidHosts++;
        if (invalidSample == null) {
          invalidSample = boundedLogSample(host);
        }
      }
    }
    if (invalidHosts > 0) {
      logger.log(
          Level.WARNING,
          "Ignored {0} invalid unique /localnodes host entries; first sample: {1}",
          new Object[] {invalidHosts, invalidSample});
    }
    if (!hosts.isEmpty() && nodes.isEmpty()) {
      throw new InvalidLocalNodesResponseException(
          "/localnodes response contained no usable host entries");
    }
    return new ArrayList<>(nodes);
  }

  static String boundedLogSample(String value) {
    final int maximumCharacters = 160;
    String source = value == null ? "null" : value;
    StringBuilder sample = new StringBuilder(Math.min(source.length(), maximumCharacters) + 3);
    for (int i = 0; i < source.length() && sample.length() < maximumCharacters; i++) {
      char character = source.charAt(i);
      sample.append(Character.isISOControl(character) ? '?' : character);
    }
    if (sample.length() < source.length()) {
      sample.append("...");
    }
    return sample.toString();
  }

  URI hostToURI(String host) throws URISyntaxException, MalformedURLException {
    boolean bracketed = host != null && host.startsWith("[") && host.endsWith("]");
    String logicalHost = LogicalHost.withoutBrackets(host);
    if (logicalHost == null || logicalHost.isEmpty()) {
      throw new URISyntaxException(String.valueOf(host), "Host is empty");
    }
    if (bracketed && logicalHost.indexOf(':') < 0) {
      throw new URISyntaxException(String.valueOf(host), "Only IPv6 literals may be bracketed");
    }
    String normalizedHost = normalizeDnsHost(logicalHost);
    URI uri = new URI(alternatorScheme, null, normalizedHost, alternatorPort, null, null, null);
    if (uri.getHost() == null
        || !LogicalHost.sameEndpoint(normalizedHost, uri.getHost())
        || uri.getUserInfo() != null
        || (uri.getRawPath() != null && !uri.getRawPath().isEmpty())
        || uri.getRawQuery() != null
        || uri.getRawFragment() != null
        || uri.getPort() != alternatorPort) {
      throw new URISyntaxException(String.valueOf(host), "Invalid host-only endpoint");
    }
    uri.toURL();
    return uri;
  }

  private static String normalizeDnsHost(String host) throws URISyntaxException {
    if (host.indexOf(':') >= 0 || LogicalHost.isIpLiteral(host)) {
      return host;
    }
    boolean absolute = host.endsWith(".");
    String dnsName = absolute ? host.substring(0, host.length() - 1) : host;
    if (dnsName.isEmpty()) {
      throw new URISyntaxException(host, "DNS hostname is empty");
    }
    final String asciiName;
    try {
      asciiName = IDN.toASCII(dnsName, IDN.USE_STD3_ASCII_RULES);
    } catch (IllegalArgumentException e) {
      throw new URISyntaxException(host, "Invalid DNS hostname: " + e.getMessage());
    }
    if (asciiName.isEmpty() || asciiName.length() > 253) {
      throw new URISyntaxException(host, "DNS hostname exceeds the maximum length");
    }
    if (looksLikeLegacyIpv4Literal(asciiName)) {
      throw new URISyntaxException(host, "Ambiguous numeric IPv4 host syntax is not allowed");
    }
    return absolute ? asciiName + "." : asciiName;
  }

  private static boolean looksLikeLegacyIpv4Literal(String host) {
    String[] components = host.split("\\.", -1);
    if (components.length > 4) {
      return false;
    }
    for (String component : components) {
      if (component.isEmpty()) {
        return false;
      }
      int offset =
          component.length() > 2
                  && component.charAt(0) == '0'
                  && (component.charAt(1) == 'x' || component.charAt(1) == 'X')
              ? 2
              : 0;
      if (offset == component.length()) {
        return false;
      }
      for (int index = offset; index < component.length(); index++) {
        char character = component.charAt(index);
        boolean digit = character >= '0' && character <= '9';
        boolean hexadecimal =
            offset == 2
                && ((character >= 'a' && character <= 'f')
                    || (character >= 'A' && character <= 'F'));
        if (!digit && !hexadecimal) {
          return false;
        }
      }
    }
    return true;
  }

  static class InvalidLocalNodesResponseException extends IOException {
    private InvalidLocalNodesResponseException(String message) {
      super(message);
    }
  }

  private static final class JsonStringArrayParser {
    private final String body;
    private final int maximumValues;
    private int pos = 0;

    private JsonStringArrayParser(String body, int maximumValues) {
      this.body = body != null ? body : "";
      this.maximumValues = maximumValues;
    }

    private Set<String> parse() throws InvalidLocalNodesResponseException {
      Set<String> values = new LinkedHashSet<>();
      expect('[');
      skipWhitespace();
      if (peek(']')) {
        pos++;
        expectEnd();
        return values;
      }

      while (true) {
        if (values.add(parseString()) && values.size() > maximumValues) {
          throw invalid(
              "/localnodes response contains more than " + maximumValues + " unique nodes");
        }
        skipWhitespace();
        if (peek(']')) {
          pos++;
          expectEnd();
          return values;
        }
        expect(',');
      }
    }

    private String parseString() throws InvalidLocalNodesResponseException {
      expect('"');
      StringBuilder value = new StringBuilder();
      while (pos < body.length()) {
        char ch = body.charAt(pos++);
        if (ch == '"') {
          return value.toString();
        }
        if (ch != '\\') {
          if (ch < 0x20) {
            throw invalid("unescaped control character in /localnodes JSON response");
          }
          value.append(ch);
          continue;
        }
        value.append(parseEscapedCharacter());
      }
      throw invalid("unterminated /localnodes JSON response");
    }

    private char parseEscapedCharacter() throws InvalidLocalNodesResponseException {
      if (pos >= body.length()) {
        throw invalid("invalid escape in /localnodes JSON response");
      }
      char escaped = body.charAt(pos++);
      switch (escaped) {
        case '"':
        case '\\':
        case '/':
          return escaped;
        case 'b':
          return '\b';
        case 'f':
          return '\f';
        case 'n':
          return '\n';
        case 'r':
          return '\r';
        case 't':
          return '\t';
        case 'u':
          return parseUnicodeEscape();
        default:
          throw invalid("unsupported escape in /localnodes JSON response");
      }
    }

    private char parseUnicodeEscape() throws InvalidLocalNodesResponseException {
      if (pos + 4 > body.length()) {
        throw invalid("invalid unicode escape in /localnodes JSON response");
      }
      int codePoint = 0;
      for (int i = 0; i < 4; i++) {
        int digit = Character.digit(body.charAt(pos++), 16);
        if (digit < 0) {
          throw invalid("invalid unicode escape in /localnodes JSON response");
        }
        codePoint = (codePoint << 4) + digit;
      }
      return (char) codePoint;
    }

    private void expect(char expected) throws InvalidLocalNodesResponseException {
      skipWhitespace();
      if (pos >= body.length() || body.charAt(pos) != expected) {
        throw invalid("invalid /localnodes JSON response");
      }
      pos++;
    }

    private void expectEnd() throws InvalidLocalNodesResponseException {
      skipWhitespace();
      if (pos != body.length()) {
        throw invalid("invalid trailing data in /localnodes JSON response");
      }
    }

    private boolean peek(char expected) {
      return pos < body.length() && body.charAt(pos) == expected;
    }

    private void skipWhitespace() {
      while (pos < body.length() && isJsonWhitespace(body.charAt(pos))) {
        pos++;
      }
    }

    private static boolean isJsonWhitespace(char character) {
      return character == ' ' || character == '\t' || character == '\r' || character == '\n';
    }

    private InvalidLocalNodesResponseException invalid(String message) {
      return new InvalidLocalNodesResponseException(message);
    }
  }
}
