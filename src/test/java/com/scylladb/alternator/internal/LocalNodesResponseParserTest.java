package com.scylladb.alternator.internal;

import static org.junit.Assert.*;

import java.net.URI;
import java.util.List;
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
  public void skipsInvalidHostEntries() throws Exception {
    List<URI> nodes = parser.parse("[\"node1.example.com\",\"bad host\",\"node2.example.com\"]");

    assertHosts(nodes, "node1.example.com", "node2.example.com");
  }

  @Test
  public void rejectsMalformedBodies() throws Exception {
    assertMalformed("");
    assertMalformed("not-json");
    assertMalformed("[");
    assertMalformed("[bad]");
    assertMalformed("[\"node1.example.com\",bad]");
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
