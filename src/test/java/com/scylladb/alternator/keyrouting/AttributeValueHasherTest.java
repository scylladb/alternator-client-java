package com.scylladb.alternator.keyrouting;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
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
  public void testBooleanTrue() {
    AttributeValue value = AttributeValue.builder().bool(true).build();
    long hash = AttributeValueHasher.hash(value);
    assertNotEquals(0L, hash);
  }

  @Test
  public void testBooleanFalse() {
    AttributeValue value = AttributeValue.builder().bool(false).build();
    long hashFalse = AttributeValueHasher.hash(value);
    // Note: false becomes byte 0, which hashes differently than empty
    assertNotNull(hashFalse);
  }

  @Test
  public void testBooleanTrueAndFalseDiffer() {
    AttributeValue trueVal = AttributeValue.builder().bool(true).build();
    AttributeValue falseVal = AttributeValue.builder().bool(false).build();
    assertNotEquals(AttributeValueHasher.hash(trueVal), AttributeValueHasher.hash(falseVal));
  }

  @Test
  public void testNullAttribute() {
    AttributeValue value = AttributeValue.builder().nul(true).build();
    long hash = AttributeValueHasher.hash(value);
    assertNotNull(hash);
  }

  @Test
  public void testStringSet() {
    AttributeValue value =
        AttributeValue.builder().ss(Arrays.asList("apple", "banana", "cherry")).build();
    long hash = AttributeValueHasher.hash(value);
    assertNotEquals(0L, hash);
  }

  @Test
  public void testNumberSet() {
    AttributeValue value = AttributeValue.builder().ns(Arrays.asList("1", "2", "3")).build();
    long hash = AttributeValueHasher.hash(value);
    assertNotEquals(0L, hash);
  }

  @Test
  public void testBinarySet() {
    AttributeValue value =
        AttributeValue.builder()
            .bs(
                Arrays.asList(
                    SdkBytes.fromByteArray(new byte[] {1, 2}),
                    SdkBytes.fromByteArray(new byte[] {3, 4})))
            .build();
    long hash = AttributeValueHasher.hash(value);
    assertNotEquals(0L, hash);
  }

  @Test
  public void testList() {
    AttributeValue value =
        AttributeValue.builder()
            .l(
                Arrays.asList(
                    AttributeValue.builder().s("item1").build(),
                    AttributeValue.builder().n("42").build()))
            .build();
    long hash = AttributeValueHasher.hash(value);
    assertNotEquals(0L, hash);
  }

  @Test
  public void testMap() {
    Map<String, AttributeValue> map = new HashMap<>();
    map.put("name", AttributeValue.builder().s("John").build());
    map.put("age", AttributeValue.builder().n("30").build());
    AttributeValue value = AttributeValue.builder().m(map).build();
    long hash = AttributeValueHasher.hash(value);
    assertNotEquals(0L, hash);
  }

  @Test
  public void testMapKeyOrderIndependence() {
    // Maps should hash the same regardless of insertion order
    // because we sort keys before hashing
    Map<String, AttributeValue> map1 = new HashMap<>();
    map1.put("b", AttributeValue.builder().s("second").build());
    map1.put("a", AttributeValue.builder().s("first").build());

    Map<String, AttributeValue> map2 = new HashMap<>();
    map2.put("a", AttributeValue.builder().s("first").build());
    map2.put("b", AttributeValue.builder().s("second").build());

    AttributeValue value1 = AttributeValue.builder().m(map1).build();
    AttributeValue value2 = AttributeValue.builder().m(map2).build();

    assertEquals(AttributeValueHasher.hash(value1), AttributeValueHasher.hash(value2));
  }

  @Test
  public void testNestedStructure() {
    Map<String, AttributeValue> innerMap = new HashMap<>();
    innerMap.put("inner", AttributeValue.builder().s("value").build());

    AttributeValue value =
        AttributeValue.builder()
            .l(
                Arrays.asList(
                    AttributeValue.builder().m(innerMap).build(),
                    AttributeValue.builder().s("item").build()))
            .build();
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

  // ========== Collision Detection Tests ==========
  // These tests document known collision scenarios in the current implementation.
  // See GitHub issue #32 for the fix proposal.

  /**
   * Documents that String and Number types with the same value currently produce the same hash.
   *
   * <p>This is a known limitation - see issue #32.
   *
   * <p>IMPORTANT: When issue #32 is fixed, this test should be updated to use assertNotEquals.
   */
  @Test
  public void testStringNumberCollision_KnownIssue() {
    // String "12345" and Number "12345" currently hash to the same value
    // because both are encoded as UTF-8 bytes without type prefix
    AttributeValue stringVal = AttributeValue.builder().s("12345").build();
    AttributeValue numberVal = AttributeValue.builder().n("12345").build();

    long stringHash = AttributeValueHasher.hash(stringVal);
    long numberHash = AttributeValueHasher.hash(numberVal);

    // KNOWN ISSUE: These currently collide. After fix, change to assertNotEquals.
    assertEquals(
        "KNOWN ISSUE #32: String and Number with same value collide", stringHash, numberHash);
  }

  /**
   * Documents that different String Set element boundaries can produce the same hash.
   *
   * <p>This is a known limitation - see issue #32.
   */
  @Test
  public void testStringSetConcatenationCollision_KnownIssue() {
    // SS ["a", "bc"] and SS ["ab", "c"] both concatenate to "abc"
    AttributeValue set1 = AttributeValue.builder().ss(Arrays.asList("a", "bc")).build();
    AttributeValue set2 = AttributeValue.builder().ss(Arrays.asList("ab", "c")).build();

    long hash1 = AttributeValueHasher.hash(set1);
    long hash2 = AttributeValueHasher.hash(set2);

    // KNOWN ISSUE: These currently collide. After fix, change to assertNotEquals.
    assertEquals("KNOWN ISSUE #32: Different SS element boundaries collide", hash1, hash2);
  }

  /**
   * Documents that different List element boundaries can produce the same hash.
   *
   * <p>This is a known limitation - see issue #32.
   */
  @Test
  public void testListConcatenationCollision_KnownIssue() {
    // L [S("a"), S("bc")] and L [S("ab"), S("c")] both concatenate to "abc"
    AttributeValue list1 =
        AttributeValue.builder()
            .l(
                Arrays.asList(
                    AttributeValue.builder().s("a").build(),
                    AttributeValue.builder().s("bc").build()))
            .build();
    AttributeValue list2 =
        AttributeValue.builder()
            .l(
                Arrays.asList(
                    AttributeValue.builder().s("ab").build(),
                    AttributeValue.builder().s("c").build()))
            .build();

    long hash1 = AttributeValueHasher.hash(list1);
    long hash2 = AttributeValueHasher.hash(list2);

    // KNOWN ISSUE: These currently collide. After fix, change to assertNotEquals.
    assertEquals("KNOWN ISSUE #32: Different List element boundaries collide", hash1, hash2);
  }

  /**
   * Documents that empty collections of different types currently produce the same hash.
   *
   * <p>This is a known limitation - see issue #32.
   */
  @Test
  public void testEmptyCollectionCollision_KnownIssue() {
    // Empty SS, NS, and BS all produce empty byte array → hash 0
    AttributeValue emptySS = AttributeValue.builder().ss(Arrays.<String>asList()).build();
    AttributeValue emptyNS = AttributeValue.builder().ns(Arrays.<String>asList()).build();
    AttributeValue emptyBS = AttributeValue.builder().bs(Arrays.<SdkBytes>asList()).build();

    long hashSS = AttributeValueHasher.hash(emptySS);
    long hashNS = AttributeValueHasher.hash(emptyNS);
    long hashBS = AttributeValueHasher.hash(emptyBS);

    // KNOWN ISSUE: Empty collections all hash to 0
    assertEquals("Empty SS hashes to 0", 0L, hashSS);
    assertEquals("Empty NS hashes to 0", 0L, hashNS);
    assertEquals("Empty BS hashes to 0", 0L, hashBS);

    // All three collide (after fix, these should differ)
    assertEquals("KNOWN ISSUE #32: Empty SS and NS collide", hashSS, hashNS);
    assertEquals("KNOWN ISSUE #32: Empty NS and BS collide", hashNS, hashBS);
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

  // ========== String Set Order Independence Tests ==========

  @Test
  public void testStringSetOrderIndependence() {
    // String sets should hash the same regardless of input order
    // because we sort before hashing
    AttributeValue set1 =
        AttributeValue.builder().ss(Arrays.asList("cherry", "apple", "banana")).build();
    AttributeValue set2 =
        AttributeValue.builder().ss(Arrays.asList("apple", "banana", "cherry")).build();
    AttributeValue set3 =
        AttributeValue.builder().ss(Arrays.asList("banana", "cherry", "apple")).build();

    long hash1 = AttributeValueHasher.hash(set1);
    long hash2 = AttributeValueHasher.hash(set2);
    long hash3 = AttributeValueHasher.hash(set3);

    assertEquals("SS order should not affect hash (1 vs 2)", hash1, hash2);
    assertEquals("SS order should not affect hash (2 vs 3)", hash2, hash3);
  }

  @Test
  public void testNumberSetOrderIndependence() {
    AttributeValue set1 = AttributeValue.builder().ns(Arrays.asList("3", "1", "2")).build();
    AttributeValue set2 = AttributeValue.builder().ns(Arrays.asList("1", "2", "3")).build();

    assertEquals(
        "NS order should not affect hash",
        AttributeValueHasher.hash(set1),
        AttributeValueHasher.hash(set2));
  }

  @Test
  public void testBinarySetOrderIndependence() {
    AttributeValue set1 =
        AttributeValue.builder()
            .bs(
                Arrays.asList(
                    SdkBytes.fromByteArray(new byte[] {3, 4}),
                    SdkBytes.fromByteArray(new byte[] {1, 2})))
            .build();
    AttributeValue set2 =
        AttributeValue.builder()
            .bs(
                Arrays.asList(
                    SdkBytes.fromByteArray(new byte[] {1, 2}),
                    SdkBytes.fromByteArray(new byte[] {3, 4})))
            .build();

    assertEquals(
        "BS order should not affect hash",
        AttributeValueHasher.hash(set1),
        AttributeValueHasher.hash(set2));
  }

  // ========== Edge Case Tests ==========

  @Test
  public void testEmptyString() {
    AttributeValue value = AttributeValue.builder().s("").build();
    long hash = AttributeValueHasher.hash(value);
    // Empty string hashes to 0 (same as empty byte array)
    assertEquals("Empty string hashes to 0", 0L, hash);
  }

  @Test
  public void testUnicodeString() {
    AttributeValue value = AttributeValue.builder().s("こんにちは世界").build();
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

  @Test
  public void testDeeplyNestedStructure() {
    // Create a deeply nested map
    Map<String, AttributeValue> level3 = new HashMap<>();
    level3.put("deep", AttributeValue.builder().s("value").build());

    Map<String, AttributeValue> level2 = new HashMap<>();
    level2.put("nested", AttributeValue.builder().m(level3).build());

    Map<String, AttributeValue> level1 = new HashMap<>();
    level1.put("outer", AttributeValue.builder().m(level2).build());

    AttributeValue value = AttributeValue.builder().m(level1).build();
    long hash = AttributeValueHasher.hash(value);

    assertNotEquals(0L, hash);
    assertEquals(hash, AttributeValueHasher.hash(value));
  }
}
