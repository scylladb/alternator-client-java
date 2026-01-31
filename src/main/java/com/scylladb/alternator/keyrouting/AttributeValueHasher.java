package com.scylladb.alternator.keyrouting;

import java.nio.ByteBuffer;
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

    // Null
    if (value.nul() != null) {
      return new byte[] {TYPE_NULL, (byte) (value.nul() ? 1 : 0)};
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
   * @param value the integer value
   * @return 4-byte big-endian representation
   */
  private static byte[] intToBytes(int value) {
    return ByteBuffer.allocate(4).putInt(value).array();
  }

  /**
   * Hashes a String Set with type prefix and length-prefixed elements.
   *
   * @param values the string set values
   * @return encoded bytes
   */
  private static byte[] hashStringSet(List<String> values) {
    // Sort strings lexicographically for deterministic hashing
    List<String> sorted = new ArrayList<>(values);
    Collections.sort(sorted);

    // Calculate total length: 1 (type) + sum of (4 + len) for each element
    int totalLength = 1;
    List<byte[]> bytesList = new ArrayList<>();
    for (String s : sorted) {
      byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
      bytesList.add(bytes);
      totalLength += 4 + bytes.length;
    }

    // Build result with type prefix and length-prefixed elements
    byte[] result = new byte[totalLength];
    result[0] = TYPE_STRING_SET;
    int offset = 1;
    for (byte[] bytes : bytesList) {
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
   * Hashes a Number Set with type prefix and length-prefixed elements.
   *
   * @param values the number set values (as strings)
   * @return encoded bytes
   */
  private static byte[] hashNumberSet(List<String> values) {
    // Sort strings lexicographically for deterministic hashing
    List<String> sorted = new ArrayList<>(values);
    Collections.sort(sorted);

    // Calculate total length: 1 (type) + sum of (4 + len) for each element
    int totalLength = 1;
    List<byte[]> bytesList = new ArrayList<>();
    for (String s : sorted) {
      byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
      bytesList.add(bytes);
      totalLength += 4 + bytes.length;
    }

    // Build result with type prefix and length-prefixed elements
    byte[] result = new byte[totalLength];
    result[0] = TYPE_NUMBER_SET;
    int offset = 1;
    for (byte[] bytes : bytesList) {
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
   * Hashes a Binary Set with type prefix and length-prefixed elements.
   *
   * @param values the binary set values
   * @return encoded bytes
   */
  private static byte[] hashBinarySet(List<SdkBytes> values) {
    // Sort binary values for deterministic hashing
    List<byte[]> byteArrays = new ArrayList<>();
    for (SdkBytes b : values) {
      byteArrays.add(b.asByteArray());
    }
    Collections.sort(byteArrays, BYTE_ARRAY_COMPARATOR);

    // Calculate total length: 1 (type) + sum of (4 + len) for each element
    int totalLength = 1;
    for (byte[] bytes : byteArrays) {
      totalLength += 4 + bytes.length;
    }

    // Build result with type prefix and length-prefixed elements
    byte[] result = new byte[totalLength];
    result[0] = TYPE_BINARY_SET;
    int offset = 1;
    for (byte[] bytes : byteArrays) {
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
   * @param values the list values
   * @return encoded bytes
   */
  private static byte[] hashList(List<AttributeValue> values) {
    // Recursively encode each element with length prefix
    List<byte[]> bytesList = new ArrayList<>();
    int totalLength = 1; // type prefix
    for (AttributeValue v : values) {
      byte[] bytes = toBytes(v);
      bytesList.add(bytes);
      totalLength += 4 + bytes.length;
    }

    // Build result with type prefix and length-prefixed elements
    byte[] result = new byte[totalLength];
    result[0] = TYPE_LIST;
    int offset = 1;
    for (byte[] bytes : bytesList) {
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
   * Hashes a Map with type prefix and length-prefixed key-value pairs.
   *
   * @param map the map values
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
