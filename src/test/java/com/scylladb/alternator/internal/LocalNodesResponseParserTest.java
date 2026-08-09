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

import static org.junit.Assert.*;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.Test;

public class LocalNodesResponseParserTest {
  private final LocalNodesResponseParser parser = new LocalNodesResponseParser("http", 8000);

  @Test
  public void parsesEmptyArrays() throws Exception {
    assertTrue(parser.parse("[]").isEmpty());
    assertTrue(parser.parse(" [ ] ").isEmpty());
  }

  @Test
  public void parsesValidMultiHostResponses() throws Exception {
    List<URI> nodes = parser.parse("[\"node1.example.com\",\"node2.example.com\"]");

    assertHosts(nodes, "node1.example.com", "node2.example.com");
  }

  @Test
  public void parsesEscapedHostStrings() throws Exception {
    List<URI> nodes =
        parser.parse(" [ \"node\\u0031.example.com\" , \"node\\u0032.example.com\" ] ");

    assertHosts(nodes, "node1.example.com", "node2.example.com");
  }

  @Test
  public void preservesAbsoluteDnsAndNormalizesInternationalDnsNames() throws Exception {
    List<URI> nodes = parser.parse("[\"node.example.com.\",\"münchen.example\",\"node123\"]");

    assertHosts(nodes, "node.example.com.", "xn--mnchen-3ya.example", "node123");
  }

  @Test
  public void rejectsLegacyNumericIpv4AliasesBeforeRuntimeResolution() throws Exception {
    assertMalformed("[\"127.1\"]");
    assertMalformed("[\"2130706433\"]");
    assertMalformed("[\"0x7f000001\"]");
    assertMalformed("[\"0177.0.0.1\"]");
    assertMalformed("[\"127.0.0.1.\"]");
    assertMalformed("[\"１２７.１\"]");
    assertHosts(parser.parse("[\"127.0.0.1\"]"), "127.0.0.1");
  }

  @Test
  public void skipsInvalidHostEntries() throws Exception {
    List<URI> nodes = parser.parse("[\"node1.example.com\",\"bad host\",\"node2.example.com\"]");

    assertHosts(nodes, "node1.example.com", "node2.example.com");
  }

  @Test(timeout = 5000)
  public void deduplicatesHugeDuplicateAndInvalidResponsesWithoutLogAmplification()
      throws Exception {
    StringBuilder body = new StringBuilder("[");
    for (int i = 0; i < 20_000; i++) {
      if (i > 0) {
        body.append(',');
      }
      body.append(i % 2 == 0 ? "\"node.example.com\"" : "\"bad host\"");
    }
    body.append(']');

    Logger parserLogger = Logger.getLogger(LocalNodesResponseParser.class.getName());
    List<LogRecord> records = new ArrayList<>();
    Handler handler =
        new Handler() {
          @Override
          public void publish(LogRecord record) {
            records.add(record);
          }

          @Override
          public void flush() {}

          @Override
          public void close() {}
        };
    parserLogger.addHandler(handler);
    try {
      List<URI> nodes = parser.parse(body.toString());

      assertHosts(nodes, "node.example.com");
    } finally {
      parserLogger.removeHandler(handler);
    }

    assertEquals("invalid duplicates must produce one aggregate warning", 1, records.size());
    assertNull("aggregate warning must not retain an exception stack", records.get(0).getThrown());
    assertTrue(records.get(0).getMessage().length() < 256);
  }

  @Test
  public void rejectsNonemptyArraysWithNoUsableHosts() throws Exception {
    assertMalformed("[\"bad host\"]");
    assertMalformed("[\"bad host\",\"also bad host\"]");
  }

  @Test
  public void rejectsMalformedBodies() throws Exception {
    assertMalformed("");
    assertMalformed("not-json");
    assertMalformed("[");
    assertMalformed("[bad]");
    assertMalformed("[\"node1.example.com\",bad]");
  }

  @Test
  public void rejectsNonJsonWhitespaceAndUnescapedControlCharacters() throws Exception {
    assertMalformed("[\f\"node1.example.com\"]");
    assertMalformed("[\"node1.example.com\",\"bad\nhost\"]");
  }

  @Test
  public void rejectsHostsReinterpretedAsAuthorityPathOrQuery() throws Exception {
    assertMalformed("[\"user@poison.test\"]");
    assertMalformed("[\"poison.test/path\"]");
    assertMalformed("[\"poison.test?query\"]");
    assertMalformed("[\"poison.test#fragment\"]");
    assertMalformed("[\"poison.test:9000\"]");
  }

  @Test
  public void rejectsSyntacticallyParseableButUnusableDnsAuthorities() throws Exception {
    assertMalformed("[\".\"]");
    assertMalformed("[\"bad..name\"]");
    assertMalformed("[\"-bad.name\"]");
    assertMalformed("[\"bad-.name\"]");
    assertMalformed("[\"bad_name\"]");
    assertMalformed("[\"[not-an-ip]\"]");
    assertMalformed("[\"bad%2ename\"]");
    assertMalformed("[\"" + "a".repeat(64) + ".name\"]");
  }

  private void assertMalformed(String body) throws Exception {
    try {
      parser.parse(body);
      fail("Expected malformed /localnodes body: " + body);
    } catch (LocalNodesResponseParser.InvalidLocalNodesResponseException e) {
      // expected
    }
  }

  private static void assertHosts(List<URI> nodes, String... expectedHosts) {
    assertEquals(expectedHosts.length, nodes.size());
    for (int i = 0; i < expectedHosts.length; i++) {
      assertEquals(expectedHosts[i], nodes.get(i).getHost());
      assertEquals("http", nodes.get(i).getScheme());
      assertEquals(8000, nodes.get(i).getPort());
    }
  }
}
