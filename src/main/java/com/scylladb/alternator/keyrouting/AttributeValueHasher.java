package com.scylladb.alternator.keyrouting;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * Hashes DynamoDB AttributeValue objects using MurmurHash3.
 *
 * <p>Supports all DynamoDB attribute types with type-prefixed encoding to prevent collisions
 * between different types:
 *
 * <ul>
 *   <li>S (String) - Type prefix 0x01 + UTF-8 bytes
 *   <li>N (Number) - Type prefix 0x02 + UTF-8 bytes of string representation
 *   <li>B (Binary) - Type prefix 0x03 + raw bytes
 *   <li>BOOL (Boolean) - Type prefix 0x04 + 1 byte (0x01 for true, 0x00 for false)
 *   <li>NULL - Type prefix 0x05 + 1 byte (0x01 for true, 0x00 for false)
 *   <li>SS (String Set) - Type prefix 0x06 + length-prefixed elements (sorted)
 *   <li>NS (Number Set) - Type prefix 0x07 + length-prefixed elements (sorted)
 *   <li>BS (Binary Set) - Type prefix 0x08 + length-prefixed elements (sorted)
 *   <li>L (List) - Type prefix 0x09 + length-prefixed recursive elements
 *   <li>M (Map) - Type prefix 0x0A + length-prefixed key-value pairs (keys sorted)
 * </ul>
 *
 * <p>Sets are sorted before hashing to ensure deterministic results, since DynamoDB sets are
 * unordered collections and iteration order is not guaranteed.
 *
 * <p>Collection elements use 4-byte big-endian length prefixes to prevent boundary collisions (e.g.
 * ["a", "bc"] vs ["ab", "c"]).
 *
 * <h3>Composite Partition Keys</h3>
 *
 * <p>This hasher operates on individual {@link AttributeValue} objects. For tables with composite
 * keys (partition key + sort key), only the partition key should be hashed for routing purposes,
 * since DynamoDB partitions data by partition key only. The sort key determines ordering within a
 * partition but does not affect which node stores the data.
 *
 * <p>Example for a table with composite key (user_id, timestamp):
 *
 * <pre>{@code
 * // Only hash the partition key (user_id)
 * AttributeValue partitionKey = item.get("user_id");
 * long hash = AttributeValueHasher.hash(partitionKey);
 * }</pre>
 *
 * <h3>Number Representation</h3>
 *
 * <p>Number values (N type) are hashed using their exact string representation as stored in
 * DynamoDB. This means that numerically equivalent values with different representations will
 * produce different hashes:
 *
 * <ul>
 *   <li>{@code "42"} and {@code "42.0"} produce different hashes
 *   <li>{@code "1e2"} and {@code "100"} produce different hashes
 *   <li>{@code "1.0"} and {@code "1.00"} produce different hashes
 * </ul>
 *
 * <p>This behavior preserves the exact representation stored in DynamoDB and matches how DynamoDB
 * itself handles number comparisons in certain contexts.
 *
 * <h3>Cross-Language Compatibility</h3>
 *
 * <p>This hashing implementation is designed to be compatible with other Alternator client
 * libraries (e.g., the Go client). For clients to produce identical hashes for the same partition
 * key values, all implementations must follow the same encoding format:
 *
 * <ul>
 *   <li>Type prefixes must use the exact byte values (0x01-0x0A) specified above
 *   <li>Strings must be encoded as UTF-8 bytes
 *   <li>Length prefixes must be 4-byte big-endian integers
 *   <li>Sets must be sorted lexicographically (strings by UTF-8 bytes, binaries by unsigned byte
 *       comparison)
 *   <li>Map keys must be sorted lexicographically by their UTF-8 byte representation
 *   <li>The MurmurHash3 implementation must use the x86_128 variant with seed 0, returning the
 *       first 64 bits
 * </ul>
 *
 * <p>If you are implementing a compatible hasher in another language, ensure your implementation
 * passes the same test vectors as this Java implementation.
 *
 * <h3>Performance Characteristics</h3>
 *
 * <p>Time complexity varies by attribute type:
 *
 * <ul>
 *   <li><b>O(n)</b> for primitive types (S, N, B, BOOL, NULL) where n is the byte length
 *   <li><b>O(n)</b> for Lists (L) where n is the total size of all elements
 *   <li><b>O(n log n)</b> for Sets (SS, NS, BS) due to sorting, where n is the number of elements
 *   <li><b>O(n log n)</b> for Maps (M) due to key sorting, where n is the number of entries
 * </ul>
 *
 * <p>Space complexity is O(n) for all types, as the entire value is converted to bytes before
 * hashing.
 *
 * @author dmitry.kropachev
 * @since 1.0.7
 */
public final class AttributeValueHasher {

  // Type prefix constants
  private static final byte TYPE_STRING = 0x01;
  private static final byte TYPE_NUMBER = 0x02;
  private static final byte TYPE_BINARY = 0x03;
  private static final byte TYPE_BOOLEAN = 0x04;
  private static final byte TYPE_NULL = 0x05;
  private static final byte TYPE_STRING_SET = 0x06;
  private static final byte TYPE_NUMBER_SET = 0x07;
  private static final byte TYPE_BINARY_SET = 0x08;
  private static final byte TYPE_LIST = 0x09;
  private static final byte TYPE_MAP = 0x0A;

  private AttributeValueHasher() {}

  /**
   * Computes a hash for a DynamoDB AttributeValue.
   *
   * @param value the attribute value to hash
   * @return the hash value
   * @throws IllegalArgumentException if the attribute value type is not supported
   */
  public static long hash(AttributeValue value) {
    if (value == null) {
      return 0L;
    }

    byte[] data = toBytes(value);
    return MurmurHash3.hash(data);
  }

  /**
   * Converts an AttributeValue to bytes for hashing.
   *
   * @param value the attribute value
   * @return the byte representation with type prefix
   */
  private static byte[] toBytes(AttributeValue value) {
    // String
    if (value.s() != null) {
      byte[] strBytes = value.s().getBytes(StandardCharsets.UTF_8);
      return prependTypePrefix(TYPE_STRING, strBytes);
    }

    // Number (stored as string)
    if (value.n() != null) {
      byte[] numBytes = value.n().getBytes(StandardCharsets.UTF_8);
      return prependTypePrefix(TYPE_NUMBER, numBytes);
    }

    // Binary
    if (value.b() != null) {
      byte[] binBytes = value.b().asByteArray();
      return prependTypePrefix(TYPE_BINARY, binBytes);
    }

    // Boolean
    if (value.bool() != null) {
      return new byte[] {TYPE_BOOLEAN, (byte) (value.bool() ? 1 : 0)};
    }

    // Null - DynamoDB NULL type is semantically always true.
    // nul(false) is an invalid state that should not occur in practice.
    if (value.nul() != null) {
      if (!value.nul()) {
        throw new IllegalArgumentException(
            "Invalid NULL attribute: nul(false) is not a valid DynamoDB NULL value");
      }
      return new byte[] {TYPE_NULL, (byte) 1};
    }

    // String Set
    if (value.hasSs() && value.ss() != null) {
      return hashStringSet(value.ss());
    }

    // Number Set
    if (value.hasNs() && value.ns() != null) {
      return hashNumberSet(value.ns());
    }

    // Binary Set
    if (value.hasBs() && value.bs() != null) {
      return hashBinarySet(value.bs());
    }

    // List
    if (value.hasL() && value.l() != null) {
      return hashList(value.l());
    }

    // Map
    if (value.hasM() && value.m() != null) {
      return hashMap(value.m());
    }

    throw new IllegalArgumentException("Unsupported AttributeValue type");
  }

  /**
   * Prepends a type prefix byte to the given data.
   *
   * @param typePrefix the type prefix byte
   * @param data the data bytes
   * @return new array with type prefix followed by data
   */
  private static byte[] prependTypePrefix(byte typePrefix, byte[] data) {
    byte[] result = new byte[1 + data.length];
    result[0] = typePrefix;
    System.arraycopy(data, 0, result, 1, data.length);
    return result;
  }

  /**
   * Encodes an integer as 4-byte big-endian.
   *
   * <p>Uses direct byte manipulation instead of ByteBuffer for better performance, as this method
   * is called frequently when encoding collection elements.
   *
   * @param value the integer value
   * @return 4-byte big-endian representation
   */
  private static byte[] intToBytes(int value) {
    return new byte[] {
      (byte) (value >> 24), (byte) (value >> 16), (byte) (value >> 8), (byte) value
    };
  }

  /**
   * Builds a byte array with type prefix and length-prefixed elements.
   *
   * <p>This is a shared helper method used by all set hashing methods to avoid code duplication.
   * The elements must be pre-sorted by the caller to ensure deterministic hashing.
   *
   * @param typePrefix the type prefix byte for this set type
   * @param sortedElements the pre-sorted list of byte arrays to encode
   * @return encoded bytes with type prefix and length-prefixed elements
   */
  private static byte[] buildLengthPrefixedBytes(byte typePrefix, List<byte[]> sortedElements) {
    // Calculate total length: 1 (type) + sum of (4 + len) for each element
    int totalLength = 1;
    for (byte[] bytes : sortedElements) {
      totalLength += 4 + bytes.length;
    }

    // Build result with type prefix and length-prefixed elements
    byte[] result = new byte[totalLength];
    result[0] = typePrefix;
    int offset = 1;
    for (byte[] bytes : sortedElements) {
      // Write 4-byte length prefix
      byte[] lenBytes = intToBytes(bytes.length);
      System.arraycopy(lenBytes, 0, result, offset, 4);
      offset += 4;
      // Write element data
      System.arraycopy(bytes, 0, result, offset, bytes.length);
      offset += bytes.length;
    }
    return result;
  }

  /**
   * Hashes a String Set with type prefix and length-prefixed elements.
   *
   * @param values the string set values; must not be null (caller is responsible for null check)
   * @return encoded bytes
   */
  private static byte[] hashStringSet(List<String> values) {
    // Sort strings lexicographically for deterministic hashing
    List<String> sorted = new ArrayList<>(values);
    Collections.sort(sorted);

    // Convert to byte arrays
    List<byte[]> bytesList = new ArrayList<>();
    for (String s : sorted) {
      bytesList.add(s.getBytes(StandardCharsets.UTF_8));
    }

    return buildLengthPrefixedBytes(TYPE_STRING_SET, bytesList);
  }

  /**
   * Hashes a Number Set with type prefix and length-prefixed elements.
   *
   * @param values the number set values (as strings); must not be null (caller is responsible for
   *     null check)
   * @return encoded bytes
   */
  private static byte[] hashNumberSet(List<String> values) {
    // Sort strings lexicographically for deterministic hashing
    List<String> sorted = new ArrayList<>(values);
    Collections.sort(sorted);

    // Convert to byte arrays
    List<byte[]> bytesList = new ArrayList<>();
    for (String s : sorted) {
      bytesList.add(s.getBytes(StandardCharsets.UTF_8));
    }

    return buildLengthPrefixedBytes(TYPE_NUMBER_SET, bytesList);
  }

  /**
   * Hashes a Binary Set with type prefix and length-prefixed elements.
   *
   * @param values the binary set values; must not be null (caller is responsible for null check)
   * @return encoded bytes
   */
  private static byte[] hashBinarySet(List<SdkBytes> values) {
    // Convert to byte arrays and sort for deterministic hashing
    List<byte[]> byteArrays = new ArrayList<>();
    for (SdkBytes b : values) {
      byteArrays.add(b.asByteArray());
    }
    Collections.sort(byteArrays, BYTE_ARRAY_COMPARATOR);

    return buildLengthPrefixedBytes(TYPE_BINARY_SET, byteArrays);
  }

  /** Comparator for sorting byte arrays lexicographically (unsigned byte comparison). */
  private static final Comparator<byte[]> BYTE_ARRAY_COMPARATOR =
      (a, b) -> {
        int minLength = Math.min(a.length, b.length);
        for (int i = 0; i < minLength; i++) {
          int cmp = (a[i] & 0xff) - (b[i] & 0xff);
          if (cmp != 0) {
            return cmp;
          }
        }
        return a.length - b.length;
      };

  /**
   * Hashes a List with type prefix and length-prefixed elements.
   *
   * @param values the list values; must not be null (caller is responsible for null check)
   * @return encoded bytes
   */
  private static byte[] hashList(List<AttributeValue> values) {
    // Recursively encode each element (order preserved, no sorting)
    List<byte[]> bytesList = new ArrayList<>();
    for (AttributeValue v : values) {
      bytesList.add(toBytes(v));
    }

    return buildLengthPrefixedBytes(TYPE_LIST, bytesList);
  }

  /**
   * Hashes a Map with type prefix and length-prefixed key-value pairs.
   *
   * @param map the map values; must not be null (caller is responsible for null check)
   * @return encoded bytes
   */
  private static byte[] hashMap(Map<String, AttributeValue> map) {
    // Sort keys lexicographically, then encode key-value pairs with length prefixes
    List<String> keys = new ArrayList<>(map.keySet());
    Collections.sort(keys);

    // Calculate total length and collect encoded pairs
    int totalLength = 1; // type prefix
    List<byte[]> keyBytesList = new ArrayList<>();
    List<byte[]> valueBytesList = new ArrayList<>();
    for (String key : keys) {
      byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
      byte[] valueBytes = toBytes(map.get(key));
      keyBytesList.add(keyBytes);
      valueBytesList.add(valueBytes);
      totalLength += 4 + keyBytes.length + 4 + valueBytes.length;
    }

    // Build result with type prefix and length-prefixed key-value pairs
    byte[] result = new byte[totalLength];
    result[0] = TYPE_MAP;
    int offset = 1;
    for (int i = 0; i < keys.size(); i++) {
      byte[] keyBytes = keyBytesList.get(i);
      byte[] valueBytes = valueBytesList.get(i);

      // Write key with length prefix
      byte[] keyLenBytes = intToBytes(keyBytes.length);
      System.arraycopy(keyLenBytes, 0, result, offset, 4);
      offset += 4;
      System.arraycopy(keyBytes, 0, result, offset, keyBytes.length);
      offset += keyBytes.length;

      // Write value with length prefix
      byte[] valueLenBytes = intToBytes(valueBytes.length);
      System.arraycopy(valueLenBytes, 0, result, offset, 4);
      offset += 4;
      System.arraycopy(valueBytes, 0, result, offset, valueBytes.length);
      offset += valueBytes.length;
    }
    return result;
  }
}
