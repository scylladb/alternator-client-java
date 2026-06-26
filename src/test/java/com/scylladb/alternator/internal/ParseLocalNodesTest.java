package com.scylladb.alternator.internal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.scylladb.alternator.AlternatorConfig;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Tests for {@link AlternatorLiveNodes#parseLocalNodes(String)}: parameterized edge-case table
 * ({@link EdgeCases}) and a mutation-based fuzz suite ({@link Fuzz}).
 */
public class ParseLocalNodesTest {

  @RunWith(Parameterized.class)
  public static class EdgeCases {

    @Parameters(name = "{0}")
    public static Collection<Object[]> data() {
      return Arrays.asList(
          new Object[][] {
            // label, input, expectedSize
            {"null", null, 0},
            {"empty string", "", 0},
            {"whitespace only", "   \t\n", 0},
            {"JSON object", "{\"host\":\"10.0.0.1\"}", 0},
            {"plain string literal", "\"10.0.0.1\"", 0},
            {"bare word", "localnodes", 0},
            {"empty array", "[]", 0},
            {"empty array with whitespace", "  [  ]  ", 0},
            {"single entry", "[\"10.0.0.1\"]", 1},
            {"three entries", "[\"10.0.0.1\",\"10.0.0.2\",\"10.0.0.3\"]", 3},
            {"whitespace inside array", "[ \"10.0.0.1\" , \"10.0.0.2\" ]", 2},
            {"newlines inside array", "[\n  \"10.0.0.1\",\n  \"10.0.0.2\"\n]", 2},
            {"trailing comma", "[\"10.0.0.1\",]", 1},
            {"empty string entry skipped", "[\"\",\"10.0.0.1\"]", 1},
            {"all empty string entries", "[\"\",\"\"]", 0},
            {"invalid host skipped, valid kept", "[\"host with spaces\",\"10.0.0.1\"]", 1},
            {"all invalid hosts", "[\"host with spaces\",\"another bad host\"]", 0},
            {"unexpected char — empty result", "[invalid]", 0},
            {"unexpected char after valid entry", "[\"10.0.0.1\",invalid]", 1},
            {"truncated — no closing bracket", "[\"10.0.0.1\"", 1},
            // ch=='\\' && i+1 >= n: backslash is last char, condition false → not treated as escape
            {"backslash at end of string", "[\"host\\", 0},
            // ch=='\\' && i+1 < n: escape sequence; appended char makes a valid IP host
            {"backslash escape resolves to valid host", "[\"10.0.0\\.1\"]", 1},
          });
    }

    private final String label;
    private final String input;
    private final int expectedSize;

    private AlternatorLiveNodes liveNodes;

    public EdgeCases(String label, String input, int expectedSize) {
      this.label = label;
      this.input = input;
      this.expectedSize = expectedSize;
    }

    @Before
    public void setUp() {
      liveNodes =
          new AlternatorLiveNodes(
              AlternatorConfig.builder().withSeedNode(URI.create("http://10.0.0.1:8000")).build());
    }

    @Test
    public void resultSizeMatchesExpectation() {
      List<URI> result = liveNodes.parseLocalNodes(input);
      assertEquals(
          "input=" + label + ": expected " + expectedSize + " URIs but got " + result,
          expectedSize,
          result.size());
    }

    @Test
    public void schemeAndPortArePreservedForValidEntries() {
      List<URI> result = liveNodes.parseLocalNodes(input);
      for (URI uri : result) {
        assertEquals("http", uri.getScheme());
        assertEquals(8000, uri.getPort());
      }
    }
  }

  public static class Fuzz {

    private static final long SEED = 0xDEADBEEFCAFEBABEL;
    private static final int ITERATIONS = 50_000;

    private static final String[] CORPUS = {
      "[]",
      "[\"10.0.0.1\"]",
      "[\"10.0.0.1\",\"10.0.0.2\",\"10.0.0.3\"]",
      "[ \"10.0.0.1\" , \"10.0.0.2\" ]",
      "[\"10.0.0.1\",]",
      "[\"\",\"10.0.0.1\"]",
      "{\"host\":\"10.0.0.1\"}",
      "[invalid]",
      "",
      "[\"10.0.0.1\"",
    };

    private AlternatorLiveNodes liveNodes;

    @Before
    public void setUp() {
      liveNodes =
          new AlternatorLiveNodes(
              AlternatorConfig.builder().withSeedNode(URI.create("http://10.0.0.1:8000")).build());
    }

    @Test
    public void parserNeverThrowsAndAlwaysReturnsWellFormedURIs() {
      Random rng = new Random(SEED);
      for (int i = 0; i < ITERATIONS; i++) {
        String input = mutate(CORPUS[rng.nextInt(CORPUS.length)], rng);
        List<URI> result;
        try {
          result = liveNodes.parseLocalNodes(input);
        } catch (Exception e) {
          throw new AssertionError(
              "parseLocalNodes threw for input (iteration " + i + "): " + escape(input), e);
        }
        assertNotNull("null returned for input: " + escape(input), result);
        for (URI uri : result) {
          assertEquals(
              "wrong scheme in URI " + uri + " (input=" + escape(input) + ")",
              "http",
              uri.getScheme());
          assertEquals(
              "wrong port in URI " + uri + " (input=" + escape(input) + ")", 8000, uri.getPort());
        }
      }
    }

    @Test
    public void nullInputAlwaysReturnsEmptyListNotNull() {
      List<URI> result = liveNodes.parseLocalNodes(null);
      assertNotNull(result);
      assertTrue(result.isEmpty());
    }

    private static String mutate(String s, Random rng) {
      StringBuilder sb = new StringBuilder(s);
      int mutations = 1 + rng.nextInt(4);
      for (int m = 0; m < mutations; m++) {
        if (sb.length() == 0) {
          sb.append((char) rng.nextInt(128));
          continue;
        }
        int pos = rng.nextInt(sb.length());
        switch (rng.nextInt(5)) {
          case 0:
            sb.deleteCharAt(pos);
            break;
          case 1:
            sb.insert(pos, (char) rng.nextInt(128));
            break;
          case 2:
            sb.setCharAt(pos, (char) rng.nextInt(128));
            break;
          case 3:
            sb.insert(pos, CORPUS[rng.nextInt(CORPUS.length)]);
            break;
          case 4:
            sb.replace(pos, Math.min(pos + 1 + rng.nextInt(4), sb.length()), "\"");
            break;
          default:
            break;
        }
      }
      return sb.toString();
    }

    private static String escape(String s) {
      if (s == null) return "<null>";
      return s.replace("\n", "\\n").replace("\t", "\\t").replace("\r", "\\r");
    }
  }
}
