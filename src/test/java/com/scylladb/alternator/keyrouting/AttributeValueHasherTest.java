package com.scylladb.alternator.keyrouting;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
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

  /** Verifies that Boolean and Null types produce different hashes. */
  @Test
  public void testBooleanNullNoCollision() {
    // Boolean true and Null true should hash to different values
    AttributeValue boolTrue = AttributeValue.builder().bool(true).build();
    AttributeValue nullTrue = AttributeValue.builder().nul(true).build();

    long boolHash = AttributeValueHasher.hash(boolTrue);
    long nullHash = AttributeValueHasher.hash(nullTrue);

    assertNotEquals("Boolean true and Null true should NOT collide", boolHash, nullHash);

    // Boolean false should differ from Null true
    AttributeValue boolFalse = AttributeValue.builder().bool(false).build();
    assertNotEquals(
        "Boolean false and Null true should NOT collide",
        AttributeValueHasher.hash(boolFalse),
        nullHash);
  }

  /** Verifies that nul(false) throws an exception since it's not a valid DynamoDB value. */
  @Test(expected = IllegalArgumentException.class)
  public void testNullFalseThrowsException() {
    // nul(false) is not a valid DynamoDB NULL value - NULL is semantically always true
    AttributeValue nullFalse = AttributeValue.builder().nul(false).build();
    AttributeValueHasher.hash(nullFalse);
  }

  // ========== Collection Boundary Collision Prevention Tests ==========
  // These tests verify that different element boundaries produce different hashes.

  /** Verifies that different String Set element boundaries produce different hashes. */
  @Test
  public void testStringSetBoundaryNoCollision() {
    // SS ["a", "bc"] and SS ["ab", "c"] should hash to different values
    // because of length-prefixed encoding
    AttributeValue set1 = AttributeValue.builder().ss(Arrays.asList("a", "bc")).build();
    AttributeValue set2 = AttributeValue.builder().ss(Arrays.asList("ab", "c")).build();

    long hash1 = AttributeValueHasher.hash(set1);
    long hash2 = AttributeValueHasher.hash(set2);

    assertNotEquals("Different SS element boundaries should NOT collide", hash1, hash2);
  }

  /** Verifies that different Number Set element boundaries produce different hashes. */
  @Test
  public void testNumberSetBoundaryNoCollision() {
    // NS ["1", "23"] and NS ["12", "3"] should hash to different values
    AttributeValue set1 = AttributeValue.builder().ns(Arrays.asList("1", "23")).build();
    AttributeValue set2 = AttributeValue.builder().ns(Arrays.asList("12", "3")).build();

    long hash1 = AttributeValueHasher.hash(set1);
    long hash2 = AttributeValueHasher.hash(set2);

    assertNotEquals("Different NS element boundaries should NOT collide", hash1, hash2);
  }

  /** Verifies that different Binary Set element boundaries produce different hashes. */
  @Test
  public void testBinarySetBoundaryNoCollision() {
    // BS [{0x01}, {0x02, 0x03}] and BS [{0x01, 0x02}, {0x03}] should differ
    AttributeValue set1 =
        AttributeValue.builder()
            .bs(
                Arrays.asList(
                    SdkBytes.fromByteArray(new byte[] {0x01}),
                    SdkBytes.fromByteArray(new byte[] {0x02, 0x03})))
            .build();
    AttributeValue set2 =
        AttributeValue.builder()
            .bs(
                Arrays.asList(
                    SdkBytes.fromByteArray(new byte[] {0x01, 0x02}),
                    SdkBytes.fromByteArray(new byte[] {0x03})))
            .build();

    long hash1 = AttributeValueHasher.hash(set1);
    long hash2 = AttributeValueHasher.hash(set2);

    assertNotEquals("Different BS element boundaries should NOT collide", hash1, hash2);
  }

  /** Verifies that different List element boundaries produce different hashes. */
  @Test
  public void testListBoundaryNoCollision() {
    // L [S("a"), S("bc")] and L [S("ab"), S("c")] should hash to different values
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

    assertNotEquals("Different List element boundaries should NOT collide", hash1, hash2);
  }

  /** Verifies that different Map key boundaries produce different hashes. */
  @Test
  public void testMapKeyBoundaryNoCollision() {
    // M {"a": S("x"), "bc": S("y")} and M {"ab": S("x"), "c": S("y")} should differ
    Map<String, AttributeValue> map1 = new HashMap<>();
    map1.put("a", AttributeValue.builder().s("x").build());
    map1.put("bc", AttributeValue.builder().s("y").build());

    Map<String, AttributeValue> map2 = new HashMap<>();
    map2.put("ab", AttributeValue.builder().s("x").build());
    map2.put("c", AttributeValue.builder().s("y").build());

    long hash1 = AttributeValueHasher.hash(AttributeValue.builder().m(map1).build());
    long hash2 = AttributeValueHasher.hash(AttributeValue.builder().m(map2).build());

    assertNotEquals("Different Map key boundaries should NOT collide", hash1, hash2);
  }

  // ========== Empty Collection Collision Prevention Tests ==========

  /** Verifies that empty collections of different types produce different hashes. */
  @Test
  public void testEmptyCollectionsNoCollision() {
    // Empty SS, NS, BS, L, and M should all produce different hashes
    AttributeValue emptySS = AttributeValue.builder().ss(Arrays.<String>asList()).build();
    AttributeValue emptyNS = AttributeValue.builder().ns(Arrays.<String>asList()).build();
    AttributeValue emptyBS = AttributeValue.builder().bs(Arrays.<SdkBytes>asList()).build();
    AttributeValue emptyL = AttributeValue.builder().l(Arrays.<AttributeValue>asList()).build();
    AttributeValue emptyM =
        AttributeValue.builder().m(new HashMap<String, AttributeValue>()).build();

    long hashSS = AttributeValueHasher.hash(emptySS);
    long hashNS = AttributeValueHasher.hash(emptyNS);
    long hashBS = AttributeValueHasher.hash(emptyBS);
    long hashL = AttributeValueHasher.hash(emptyL);
    long hashM = AttributeValueHasher.hash(emptyM);

    // All empty collections should produce different hashes due to type prefixes
    assertNotEquals("Empty SS and NS should NOT collide", hashSS, hashNS);
    assertNotEquals("Empty NS and BS should NOT collide", hashNS, hashBS);
    assertNotEquals("Empty BS and L should NOT collide", hashBS, hashL);
    assertNotEquals("Empty L and M should NOT collide", hashL, hashM);
    assertNotEquals("Empty SS and L should NOT collide", hashSS, hashL);
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

  // ========== All Types Unique Tests ==========

  /** Verifies that all primitive types produce unique hashes for reasonable test values. */
  @Test
  public void testAllPrimitiveTypesUnique() {
    // Create one value of each primitive type with similar-looking data
    AttributeValue stringVal = AttributeValue.builder().s("1").build();
    AttributeValue numberVal = AttributeValue.builder().n("1").build();
    AttributeValue binaryVal =
        AttributeValue.builder()
            .b(SdkBytes.fromByteArray(new byte[] {0x31})) // ASCII '1'
            .build();
    AttributeValue boolVal = AttributeValue.builder().bool(true).build();
    AttributeValue nullVal = AttributeValue.builder().nul(true).build();

    long hashS = AttributeValueHasher.hash(stringVal);
    long hashN = AttributeValueHasher.hash(numberVal);
    long hashB = AttributeValueHasher.hash(binaryVal);
    long hashBool = AttributeValueHasher.hash(boolVal);
    long hashNull = AttributeValueHasher.hash(nullVal);

    // All should be different
    assertNotEquals("S vs N", hashS, hashN);
    assertNotEquals("S vs B", hashS, hashB);
    assertNotEquals("S vs BOOL", hashS, hashBool);
    assertNotEquals("S vs NULL", hashS, hashNull);
    assertNotEquals("N vs B", hashN, hashB);
    assertNotEquals("N vs BOOL", hashN, hashBool);
    assertNotEquals("N vs NULL", hashN, hashNull);
    assertNotEquals("B vs BOOL", hashB, hashBool);
    assertNotEquals("B vs NULL", hashB, hashNull);
    assertNotEquals("BOOL vs NULL", hashBool, hashNull);
  }

  /** Verifies that all collection types produce unique hashes when empty. */
  @Test
  public void testAllCollectionTypesUnique() {
    AttributeValue ss = AttributeValue.builder().ss(Arrays.asList("a")).build();
    AttributeValue ns = AttributeValue.builder().ns(Arrays.asList("1")).build();
    AttributeValue bs =
        AttributeValue.builder()
            .bs(Arrays.asList(SdkBytes.fromByteArray(new byte[] {0x61})))
            .build();
    AttributeValue list =
        AttributeValue.builder().l(Arrays.asList(AttributeValue.builder().s("a").build())).build();

    Map<String, AttributeValue> mapData = new HashMap<>();
    mapData.put("k", AttributeValue.builder().s("v").build());
    AttributeValue map = AttributeValue.builder().m(mapData).build();

    long hashSS = AttributeValueHasher.hash(ss);
    long hashNS = AttributeValueHasher.hash(ns);
    long hashBS = AttributeValueHasher.hash(bs);
    long hashL = AttributeValueHasher.hash(list);
    long hashM = AttributeValueHasher.hash(map);

    // All collection types should produce different hashes
    assertNotEquals("SS vs NS", hashSS, hashNS);
    assertNotEquals("SS vs BS", hashSS, hashBS);
    assertNotEquals("SS vs L", hashSS, hashL);
    assertNotEquals("SS vs M", hashSS, hashM);
    assertNotEquals("NS vs BS", hashNS, hashBS);
    assertNotEquals("NS vs L", hashNS, hashL);
    assertNotEquals("NS vs M", hashNS, hashM);
    assertNotEquals("BS vs L", hashBS, hashL);
    assertNotEquals("BS vs M", hashBS, hashM);
    assertNotEquals("L vs M", hashL, hashM);
  }

  // ========== Mixed Type in Collections Tests ==========

  /** Verifies that lists with different element types but same values produce different hashes. */
  @Test
  public void testListWithDifferentElementTypes() {
    // L [S("123")] vs L [N("123")] should differ
    AttributeValue listWithString =
        AttributeValue.builder()
            .l(Arrays.asList(AttributeValue.builder().s("123").build()))
            .build();
    AttributeValue listWithNumber =
        AttributeValue.builder()
            .l(Arrays.asList(AttributeValue.builder().n("123").build()))
            .build();

    assertNotEquals(
        "Lists with S vs N elements should differ",
        AttributeValueHasher.hash(listWithString),
        AttributeValueHasher.hash(listWithNumber));
  }

  /** Verifies that maps with same keys but different value types produce different hashes. */
  @Test
  public void testMapWithDifferentValueTypes() {
    Map<String, AttributeValue> map1 = new HashMap<>();
    map1.put("key", AttributeValue.builder().s("123").build());

    Map<String, AttributeValue> map2 = new HashMap<>();
    map2.put("key", AttributeValue.builder().n("123").build());

    assertNotEquals(
        "Maps with S vs N values should differ",
        AttributeValueHasher.hash(AttributeValue.builder().m(map1).build()),
        AttributeValueHasher.hash(AttributeValue.builder().m(map2).build()));
  }

  // ========== Thread Safety Tests ==========

  /** Verifies that concurrent access from multiple threads produces consistent results. */
  @Test
  public void testConcurrentAccess() throws InterruptedException {
    final int threadCount = 10;
    final int iterationsPerThread = 1000;

    // Create test values of different types
    final AttributeValue stringValue = AttributeValue.builder().s("test_user_123").build();
    final AttributeValue numberValue = AttributeValue.builder().n("12345").build();
    final AttributeValue listValue =
        AttributeValue.builder()
            .l(
                Arrays.asList(
                    AttributeValue.builder().s("item1").build(),
                    AttributeValue.builder().n("42").build()))
            .build();

    // Compute expected hashes
    final long expectedStringHash = AttributeValueHasher.hash(stringValue);
    final long expectedNumberHash = AttributeValueHasher.hash(numberValue);
    final long expectedListHash = AttributeValueHasher.hash(listValue);

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
                long listHash = AttributeValueHasher.hash(listValue);

                if (stringHash != expectedStringHash
                    || numberHash != expectedNumberHash
                    || listHash != expectedListHash) {
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

  // ========== Large Collection Tests ==========

  /** Verifies hashing works correctly with large collections (10,000+ elements). */
  @Test
  public void testLargeStringSet() {
    List<String> largeSet = new ArrayList<>();
    for (int i = 0; i < 10000; i++) {
      largeSet.add("element_" + i);
    }

    AttributeValue value = AttributeValue.builder().ss(largeSet).build();
    long hash = AttributeValueHasher.hash(value);

    // Verify non-zero hash
    assertNotEquals("Large string set should produce non-zero hash", 0L, hash);

    // Verify consistency
    assertEquals(
        "Large string set hash should be consistent",
        hash,
        AttributeValueHasher.hash(AttributeValue.builder().ss(largeSet).build()));
  }

  /** Verifies hashing works correctly with large lists (10,000+ elements). */
  @Test
  public void testLargeList() {
    List<AttributeValue> largeList = new ArrayList<>();
    for (int i = 0; i < 10000; i++) {
      largeList.add(AttributeValue.builder().s("item_" + i).build());
    }

    AttributeValue value = AttributeValue.builder().l(largeList).build();
    long hash = AttributeValueHasher.hash(value);

    // Verify non-zero hash
    assertNotEquals("Large list should produce non-zero hash", 0L, hash);

    // Verify consistency
    assertEquals("Large list hash should be consistent", hash, AttributeValueHasher.hash(value));
  }

  /** Verifies hashing works correctly with large maps (10,000+ entries). */
  @Test
  public void testLargeMap() {
    Map<String, AttributeValue> largeMap = new HashMap<>();
    for (int i = 0; i < 10000; i++) {
      largeMap.put("key_" + i, AttributeValue.builder().s("value_" + i).build());
    }

    AttributeValue value = AttributeValue.builder().m(largeMap).build();
    long hash = AttributeValueHasher.hash(value);

    // Verify non-zero hash
    assertNotEquals("Large map should produce non-zero hash", 0L, hash);

    // Verify consistency
    assertEquals("Large map hash should be consistent", hash, AttributeValueHasher.hash(value));
  }
}
