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

  private static final Logger logger = Logger.getLogger(LocalNodesResponseParser.class.getName());

  LocalNodesResponseParser(String alternatorScheme, int alternatorPort) {
    this.alternatorScheme = alternatorScheme;
    this.alternatorPort = alternatorPort;
  }

  List<URI> parse(String responseBody) throws InvalidLocalNodesResponseException {
    Set<URI> nodes = new LinkedHashSet<>();
    Set<String> hosts = new JsonStringArrayParser(responseBody).parse();
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
    URI uri =
        new URI(
            alternatorScheme,
            null,
            LogicalHost.withoutBrackets(host),
            alternatorPort,
            null,
            null,
            null);
    uri.toURL();
    return uri;
  }

  static class InvalidLocalNodesResponseException extends IOException {
    private InvalidLocalNodesResponseException(String message) {
      super(message);
    }
  }

  private static final class JsonStringArrayParser {
    private final String body;
    private int pos = 0;

    private JsonStringArrayParser(String body) {
      this.body = body != null ? body : "";
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
        values.add(parseString());
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
      while (pos < body.length() && Character.isWhitespace(body.charAt(pos))) {
        pos++;
      }
    }

    private InvalidLocalNodesResponseException invalid(String message) {
      return new InvalidLocalNodesResponseException(message);
    }
  }
}
