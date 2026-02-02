package com.scylladb.alternator.keyrouting;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Test;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * Unit tests for AttributeValueHasher.
 *
 * @author dmitry.kropachev
 */
public class AttributeValueHasherTest {

  @Test
  public void testNullValue() {
    long hash = AttributeValueHasher.hash(null);
    assertEquals(0L, hash);
  }

  @Test
  public void testStringValue() {
    AttributeValue value = AttributeValue.builder().s("hello").build();
    long hash = AttributeValueHasher.hash(value);
    assertNotEquals(0L, hash);
  }

  @Test
  public void testStringConsistency() {
    AttributeValue value1 = AttributeValue.builder().s("user_123").build();
    AttributeValue value2 = AttributeValue.builder().s("user_123").build();
    assertEquals(AttributeValueHasher.hash(value1), AttributeValueHasher.hash(value2));
  }

  @Test
  public void testDifferentStrings() {
    AttributeValue value1 = AttributeValue.builder().s("user_123").build();
    AttributeValue value2 = AttributeValue.builder().s("user_456").build();
    assertNotEquals(AttributeValueHasher.hash(value1), AttributeValueHasher.hash(value2));
  }

  @Test
  public void testNumberValue() {
    AttributeValue value = AttributeValue.builder().n("12345").build();
    long hash = AttributeValueHasher.hash(value);
    assertNotEquals(0L, hash);
  }

  @Test
  public void testNumberConsistency() {
    AttributeValue value1 = AttributeValue.builder().n("42").build();
    AttributeValue value2 = AttributeValue.builder().n("42").build();
    assertEquals(AttributeValueHasher.hash(value1), AttributeValueHasher.hash(value2));
  }

  @Test
  public void testBinaryValue() {
    byte[] data = new byte[] {0x01, 0x02, 0x03, 0x04};
    AttributeValue value = AttributeValue.builder().b(SdkBytes.fromByteArray(data)).build();
    long hash = AttributeValueHasher.hash(value);
    assertNotEquals(0L, hash);
  }

  @Test
  public void testTypicalPartitionKeys() {
    // Test common partition key patterns
    String[] keys = {"user_12345", "ORDER#2024-01-15", "pk_abc123", "session-uuid-here"};

    for (String key : keys) {
      AttributeValue value = AttributeValue.builder().s(key).build();
      long hash = AttributeValueHasher.hash(value);
      // Verify consistency
      assertEquals(hash, AttributeValueHasher.hash(value));
    }
  }

  @Test
  public void testNumericPartitionKeys() {
    // Some tables use numeric partition keys
    String[] keys = {"12345", "98765432", "0", "-1"};

    for (String key : keys) {
      AttributeValue value = AttributeValue.builder().n(key).build();
      long hash = AttributeValueHasher.hash(value);
      assertEquals(hash, AttributeValueHasher.hash(value));
    }
  }

  // ========== Type Collision Prevention Tests ==========
  // These tests verify that different types with the same value produce different hashes.

  /** Verifies that String and Number types with the same value produce different hashes. */
  @Test
  public void testStringNumberNoCollision() {
    // String "12345" and Number "12345" should hash to different values
    // because they have different type prefixes
    AttributeValue stringVal = AttributeValue.builder().s("12345").build();
    AttributeValue numberVal = AttributeValue.builder().n("12345").build();

    long stringHash = AttributeValueHasher.hash(stringVal);
    long numberHash = AttributeValueHasher.hash(numberVal);

    assertNotEquals("String and Number with same value should NOT collide", stringHash, numberHash);
  }

  /**
   * Verifies that String and Binary types with the same byte representation produce different
   * hashes.
   */
  @Test
  public void testStringBinaryNoCollision() {
    // String "hello" and Binary containing UTF-8 bytes of "hello"
    AttributeValue stringVal = AttributeValue.builder().s("hello").build();
    AttributeValue binaryVal =
        AttributeValue.builder()
            .b(SdkBytes.fromByteArray("hello".getBytes(java.nio.charset.StandardCharsets.UTF_8)))
            .build();

    long stringHash = AttributeValueHasher.hash(stringVal);
    long binaryHash = AttributeValueHasher.hash(binaryVal);

    assertNotEquals("String and Binary with same bytes should NOT collide", stringHash, binaryHash);
  }

  // ========== Binary Partition Key Tests ==========

  @Test
  public void testBinaryPartitionKeyConsistency() {
    byte[] data = new byte[] {0x01, 0x02, 0x03, (byte) 0xff, (byte) 0xfe};
    AttributeValue value1 = AttributeValue.builder().b(SdkBytes.fromByteArray(data)).build();
    AttributeValue value2 =
        AttributeValue.builder().b(SdkBytes.fromByteArray(data.clone())).build();

    assertEquals(
        "Binary partition keys with same data should hash consistently",
        AttributeValueHasher.hash(value1),
        AttributeValueHasher.hash(value2));
  }

  @Test
  public void testDifferentBinaryValues() {
    AttributeValue value1 =
        AttributeValue.builder().b(SdkBytes.fromByteArray(new byte[] {1, 2, 3})).build();
    AttributeValue value2 =
        AttributeValue.builder().b(SdkBytes.fromByteArray(new byte[] {1, 2, 4})).build();

    assertNotEquals(
        "Different binary values should produce different hashes",
        AttributeValueHasher.hash(value1),
        AttributeValueHasher.hash(value2));
  }

  @Test
  public void testBinaryWithHighBytes() {
    // Test binary data with bytes >= 0x80 to verify unsigned handling
    byte[] data = new byte[] {(byte) 0xff, (byte) 0x80, (byte) 0x7f, (byte) 0x00};
    AttributeValue value = AttributeValue.builder().b(SdkBytes.fromByteArray(data)).build();
    long hash = AttributeValueHasher.hash(value);

    // Verify consistency
    assertEquals(hash, AttributeValueHasher.hash(value));
    assertNotEquals("Binary with high bytes should produce non-zero hash", 0L, hash);
  }

  // ========== Edge Case Tests ==========

  @Test
  public void testEmptyString() {
    AttributeValue value = AttributeValue.builder().s("").build();
    long hash = AttributeValueHasher.hash(value);
    // Empty string should still produce a non-zero hash due to type prefix
    assertNotEquals("Empty string should produce non-zero hash (has type prefix)", 0L, hash);
  }

  @Test
  public void testUnicodeString() {
    AttributeValue value =
        AttributeValue.builder().s("\u3053\u3093\u306b\u3061\u306f\u4e16\u754c").build();
    long hash = AttributeValueHasher.hash(value);
    assertNotEquals(0L, hash);
    // Verify consistency
    assertEquals(hash, AttributeValueHasher.hash(value));
  }

  @Test
  public void testVeryLongString() {
    // Test with a string longer than 16 bytes (MurmurHash3 block size)
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 100; i++) {
      sb.append("partition_key_segment_").append(i).append("_");
    }
    AttributeValue value = AttributeValue.builder().s(sb.toString()).build();
    long hash = AttributeValueHasher.hash(value);
    assertNotEquals(0L, hash);
    assertEquals(hash, AttributeValueHasher.hash(value));
  }

  @Test
  public void testNegativeNumber() {
    AttributeValue value = AttributeValue.builder().n("-12345.6789").build();
    long hash = AttributeValueHasher.hash(value);
    assertNotEquals(0L, hash);
    assertEquals(hash, AttributeValueHasher.hash(value));
  }

  @Test
  public void testScientificNotationNumber() {
    AttributeValue value = AttributeValue.builder().n("1.23E10").build();
    long hash = AttributeValueHasher.hash(value);
    assertNotEquals(0L, hash);
    assertEquals(hash, AttributeValueHasher.hash(value));
  }

  // ========== All Partition Key Types Unique Tests ==========

  /** Verifies that all supported partition key types produce unique hashes. */
  @Test
  public void testAllPartitionKeyTypesUnique() {
    // Create one value of each supported type with similar-looking data
    AttributeValue stringVal = AttributeValue.builder().s("1").build();
    AttributeValue numberVal = AttributeValue.builder().n("1").build();
    AttributeValue binaryVal =
        AttributeValue.builder()
            .b(SdkBytes.fromByteArray(new byte[] {0x31})) // ASCII '1'
            .build();

    long hashS = AttributeValueHasher.hash(stringVal);
    long hashN = AttributeValueHasher.hash(numberVal);
    long hashB = AttributeValueHasher.hash(binaryVal);

    // All should be different
    assertNotEquals("S vs N", hashS, hashN);
    assertNotEquals("S vs B", hashS, hashB);
    assertNotEquals("N vs B", hashN, hashB);
  }

  // ========== Unsupported Type Tests ==========

  /** Verifies that Boolean type throws an exception. */
  @Test(expected = IllegalArgumentException.class)
  public void testBooleanThrowsException() {
    AttributeValue value = AttributeValue.builder().bool(true).build();
    AttributeValueHasher.hash(value);
  }

  /** Verifies that NULL type throws an exception. */
  @Test(expected = IllegalArgumentException.class)
  public void testNullTypeThrowsException() {
    AttributeValue value = AttributeValue.builder().nul(true).build();
    AttributeValueHasher.hash(value);
  }

  /** Verifies that String Set throws an exception. */
  @Test(expected = IllegalArgumentException.class)
  public void testStringSetThrowsException() {
    AttributeValue value =
        AttributeValue.builder().ss(Arrays.asList("apple", "banana", "cherry")).build();
    AttributeValueHasher.hash(value);
  }

  /** Verifies that Number Set throws an exception. */
  @Test(expected = IllegalArgumentException.class)
  public void testNumberSetThrowsException() {
    AttributeValue value = AttributeValue.builder().ns(Arrays.asList("1", "2", "3")).build();
    AttributeValueHasher.hash(value);
  }

  /** Verifies that Binary Set throws an exception. */
  @Test(expected = IllegalArgumentException.class)
  public void testBinarySetThrowsException() {
    AttributeValue value =
        AttributeValue.builder()
            .bs(
                Arrays.asList(
                    SdkBytes.fromByteArray(new byte[] {1, 2}),
                    SdkBytes.fromByteArray(new byte[] {3, 4})))
            .build();
    AttributeValueHasher.hash(value);
  }

  /** Verifies that List throws an exception. */
  @Test(expected = IllegalArgumentException.class)
  public void testListThrowsException() {
    AttributeValue value =
        AttributeValue.builder()
            .l(
                Arrays.asList(
                    AttributeValue.builder().s("item1").build(),
                    AttributeValue.builder().n("42").build()))
            .build();
    AttributeValueHasher.hash(value);
  }

  /** Verifies that Map throws an exception. */
  @Test(expected = IllegalArgumentException.class)
  public void testMapThrowsException() {
    Map<String, AttributeValue> map = new HashMap<>();
    map.put("name", AttributeValue.builder().s("John").build());
    map.put("age", AttributeValue.builder().n("30").build());
    AttributeValue value = AttributeValue.builder().m(map).build();
    AttributeValueHasher.hash(value);
  }

  // ========== Thread Safety Tests ==========

  /** Verifies that concurrent access from multiple threads produces consistent results. */
  @Test
  public void testConcurrentAccess() throws InterruptedException {
    final int threadCount = 10;
    final int iterationsPerThread = 1000;

    // Create test values of supported types
    final AttributeValue stringValue = AttributeValue.builder().s("test_user_123").build();
    final AttributeValue numberValue = AttributeValue.builder().n("12345").build();
    final AttributeValue binaryValue =
        AttributeValue.builder().b(SdkBytes.fromByteArray(new byte[] {0x01, 0x02, 0x03})).build();

    // Compute expected hashes
    final long expectedStringHash = AttributeValueHasher.hash(stringValue);
    final long expectedNumberHash = AttributeValueHasher.hash(numberValue);
    final long expectedBinaryHash = AttributeValueHasher.hash(binaryValue);

    final AtomicBoolean failed = new AtomicBoolean(false);
    final CountDownLatch startLatch = new CountDownLatch(1);
    final CountDownLatch doneLatch = new CountDownLatch(threadCount);

    ExecutorService executor = Executors.newFixedThreadPool(threadCount);

    for (int t = 0; t < threadCount; t++) {
      executor.submit(
          () -> {
            try {
              startLatch.await();
              for (int i = 0; i < iterationsPerThread && !failed.get(); i++) {
                long stringHash = AttributeValueHasher.hash(stringValue);
                long numberHash = AttributeValueHasher.hash(numberValue);
                long binaryHash = AttributeValueHasher.hash(binaryValue);

                if (stringHash != expectedStringHash
                    || numberHash != expectedNumberHash
                    || binaryHash != expectedBinaryHash) {
                  failed.set(true);
                }
              }
            } catch (Exception e) {
              failed.set(true);
            } finally {
              doneLatch.countDown();
            }
          });
    }

    // Start all threads simultaneously
    startLatch.countDown();

    // Wait for all threads to complete
    assertTrue("Threads did not complete in time", doneLatch.await(30, TimeUnit.SECONDS));

    executor.shutdown();

    assertFalse("Concurrent hash computation produced inconsistent results", failed.get());
  }
}
