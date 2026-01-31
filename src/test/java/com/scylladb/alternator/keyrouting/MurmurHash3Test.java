package com.scylladb.alternator.keyrouting;

import static org.junit.Assert.*;

import java.nio.charset.StandardCharsets;
import org.junit.Test;

/**
 * Unit tests for MurmurHash3.
 *
 * <p>Tests verify that the Java implementation produces the same hashes as the Go reference
 * implementation for various inputs.
 *
 * @author dmitry.kropachev
 */
public class MurmurHash3Test {

  /**
   * Test vectors verified against Go alternator-client implementation.
   *
   * <p>These values were verified using the Go murmur3 package with the same inputs and seed 0:
   *
   * <pre>{@code
   * import "github.com/spaolacci/murmur3"
   * h1, _ := murmur3.Sum128([]byte("test"))
   * fmt.Printf("0x%016xL\n", h1)
   * }</pre>
   *
   * <p>Cross-language compatibility is ensured for key route affinity feature.
   */
  @Test
  public void testGoCompatibility_emptyInput() {
    // Empty input with seed 0 produces h1 = 0
    byte[] data = new byte[0];
    long hash = MurmurHash3.hash(data);
    assertEquals("Empty input should produce 0 with seed 0", 0L, hash);
  }

  @Test
  public void testGoCompatibility_knownValues() {
    // Test vectors - these specific values ensure consistent hashing across Java and Go clients.
    // The hash function is MurmurHash3 x64 128-bit, returning the first 64 bits (h1).
    // Verified against Go alternator-client implementation using github.com/spaolacci/murmur3.

    // "test"
    assertEquals(
        "Hash of 'test'",
        0xac7d28cc74bde19dL,
        MurmurHash3.hash("test".getBytes(StandardCharsets.UTF_8)));

    // "hello"
    assertEquals(
        "Hash of 'hello'",
        0xcbd8a7b341bd9b02L,
        MurmurHash3.hash("hello".getBytes(StandardCharsets.UTF_8)));

    // "user_123" - typical partition key
    assertEquals(
        "Hash of 'user_123'",
        0x104832bf621f0137L,
        MurmurHash3.hash("user_123".getBytes(StandardCharsets.UTF_8)));

    // 16 bytes (exactly one block)
    assertEquals(
        "Hash of '0123456789abcdef'",
        0x4be06d94cf4ad1a7L,
        MurmurHash3.hash("0123456789abcdef".getBytes(StandardCharsets.UTF_8)));
  }

  @Test
  public void testGoCompatibility_binaryWithHighBytes() {
    // Test with bytes >= 0x80 to verify unsigned byte handling
    // This is critical because Java bytes are signed, Go bytes are unsigned
    byte[] data = new byte[] {(byte) 0xff, (byte) 0x80, (byte) 0x7f, (byte) 0x00};
    long hash = MurmurHash3.hash(data);
    // This specific value ensures the & 0xff masking is correct
    assertEquals("Hash of binary data with high bytes", 0x3408b0fbe4cb130cL, hash);
  }

  @Test
  public void testEmptyByteArray() {
    byte[] data = new byte[0];
    long hash = MurmurHash3.hash(data);
    // Empty input with seed 0 should produce 0
    assertEquals(0L, hash);
  }

  @Test
  public void testSingleByte() {
    byte[] data = new byte[] {0x42};
    long hash = MurmurHash3.hash(data);
    // Should produce a non-zero hash for non-empty input
    assertNotEquals(0L, hash);
  }

  @Test
  public void testShortString() {
    byte[] data = "hello".getBytes(StandardCharsets.UTF_8);
    long hash = MurmurHash3.hash(data);
    assertNotEquals(0L, hash);
  }

  @Test
  public void testExactly16Bytes() {
    // Exactly one block
    byte[] data = "0123456789abcdef".getBytes(StandardCharsets.UTF_8);
    assertEquals(16, data.length);
    long hash = MurmurHash3.hash(data);
    assertNotEquals(0L, hash);
  }

  @Test
  public void test17Bytes() {
    // One block plus one tail byte
    byte[] data = "0123456789abcdefg".getBytes(StandardCharsets.UTF_8);
    assertEquals(17, data.length);
    long hash = MurmurHash3.hash(data);
    assertNotEquals(0L, hash);
  }

  @Test
  public void testLongerString() {
    byte[] data = "this is a longer string that exceeds 16 bytes".getBytes(StandardCharsets.UTF_8);
    long hash = MurmurHash3.hash(data);
    assertNotEquals(0L, hash);
  }

  @Test
  public void testConsistency() {
    // Same input should always produce the same hash
    byte[] data = "partition_key_value".getBytes(StandardCharsets.UTF_8);
    long hash1 = MurmurHash3.hash(data);
    long hash2 = MurmurHash3.hash(data);
    assertEquals("Same input should produce same hash", hash1, hash2);
  }

  @Test
  public void testDifferentInputsProduceDifferentHashes() {
    byte[] data1 = "user_123".getBytes(StandardCharsets.UTF_8);
    byte[] data2 = "user_456".getBytes(StandardCharsets.UTF_8);
    long hash1 = MurmurHash3.hash(data1);
    long hash2 = MurmurHash3.hash(data2);
    assertNotEquals("Different inputs should produce different hashes", hash1, hash2);
  }

  @Test
  public void testWithOffset() {
    byte[] data = "prefixHELLOsuffix".getBytes(StandardCharsets.UTF_8);
    byte[] hello = "HELLO".getBytes(StandardCharsets.UTF_8);

    long fullHash = MurmurHash3.hash(hello);
    long offsetHash = MurmurHash3.hash(data, 6, 5);

    assertEquals("Offset hash should match direct hash", fullHash, offsetHash);
  }

  @Test
  public void test32Bytes() {
    // Two full blocks
    byte[] data = "01234567890123456789012345678901".getBytes(StandardCharsets.UTF_8);
    assertEquals(32, data.length);
    long hash = MurmurHash3.hash(data);
    assertNotEquals(0L, hash);
  }

  @Test
  public void test48Bytes() {
    // Three full blocks
    byte[] data =
        "012345678901234567890123456789012345678901234567".getBytes(StandardCharsets.UTF_8);
    assertEquals(48, data.length);
    long hash = MurmurHash3.hash(data);
    assertNotEquals(0L, hash);
  }

  @Test
  public void testAllTailLengths() {
    // Test all possible tail lengths (1-15 bytes)
    for (int tailLen = 1; tailLen < 16; tailLen++) {
      byte[] data = new byte[16 + tailLen];
      for (int i = 0; i < data.length; i++) {
        data[i] = (byte) i;
      }
      long hash = MurmurHash3.hash(data);
      assertNotEquals("Tail length " + tailLen + " should produce non-zero hash", 0L, hash);
    }
  }

  @Test
  public void testQuickBrownFox() {
    byte[] data = "The quick brown fox jumps over the lazy dog".getBytes(StandardCharsets.UTF_8);
    long hash = MurmurHash3.hash(data);
    // This is a known string, hash should be consistent
    long hash2 = MurmurHash3.hash(data);
    assertEquals(hash, hash2);
  }

  @Test
  public void testBinaryData() {
    // Test with binary data including null bytes
    byte[] data = new byte[] {0x00, 0x01, 0x02, (byte) 0xff, (byte) 0xfe, (byte) 0xfd};
    long hash = MurmurHash3.hash(data);
    assertNotEquals(0L, hash);
  }

  @Test
  public void testPartitionKeySimulation() {
    // Simulate typical partition key values
    String[] keys = {"user_id12345", "order_98765", "pk_abc_123", "session_xyz_789"};

    for (String key : keys) {
      byte[] data = key.getBytes(StandardCharsets.UTF_8);
      long hash = MurmurHash3.hash(data);
      // Verify hash can be used as a positive seed
      assertTrue("Hash should be usable for node selection", hash != 0 || key.isEmpty());
    }
  }
}
