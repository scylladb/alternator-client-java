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
 * <p>Supports all DynamoDB attribute types:
 *
 * <ul>
 *   <li>S (String) - UTF-8 bytes hashed directly
 *   <li>N (Number) - String representation hashed
 *   <li>B (Binary) - Raw bytes hashed
 *   <li>BOOL (Boolean) - 1 or 0 byte
 *   <li>NULL - 1 or 0 byte
 *   <li>SS (String Set) - Strings sorted lexicographically, then concatenated and hashed
 *   <li>NS (Number Set) - Number strings sorted lexicographically, then concatenated and hashed
 *   <li>BS (Binary Set) - Binary values sorted by unsigned byte comparison, then concatenated and
 *       hashed
 *   <li>L (List) - Each element hashed recursively in order
 *   <li>M (Map) - Keys sorted lexicographically, then key-value pairs hashed recursively
 * </ul>
 *
 * <p>Sets are sorted before hashing to ensure deterministic results, since DynamoDB sets are
 * unordered collections and iteration order is not guaranteed.
 *
 * @author dmitry.kropachev
 * @since 1.0.7
 */
public final class AttributeValueHasher {

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
   * @return the byte representation
   */
  private static byte[] toBytes(AttributeValue value) {
    // String
    if (value.s() != null) {
      return value.s().getBytes(StandardCharsets.UTF_8);
    }

    // Number (stored as string)
    if (value.n() != null) {
      return value.n().getBytes(StandardCharsets.UTF_8);
    }

    // Binary
    if (value.b() != null) {
      return value.b().asByteArray();
    }

    // Boolean
    if (value.bool() != null) {
      return new byte[] {(byte) (value.bool() ? 1 : 0)};
    }

    // Null
    if (value.nul() != null) {
      return new byte[] {(byte) (value.nul() ? 1 : 0)};
    }

    // String Set
    if (value.hasSs() && value.ss() != null) {
      return hashStringCollection(value.ss());
    }

    // Number Set
    if (value.hasNs() && value.ns() != null) {
      return hashStringCollection(value.ns());
    }

    // Binary Set
    if (value.hasBs() && value.bs() != null) {
      return hashBinaryCollection(value.bs());
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

  private static byte[] hashStringCollection(List<String> values) {
    // Sort strings lexicographically for deterministic hashing
    // (DynamoDB sets are unordered, so iteration order is not guaranteed)
    List<String> sorted = new ArrayList<>(values);
    Collections.sort(sorted);

    // Concatenate all string bytes
    int totalLength = 0;
    List<byte[]> bytesList = new ArrayList<>();
    for (String s : sorted) {
      byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
      bytesList.add(bytes);
      totalLength += bytes.length;
    }

    byte[] result = new byte[totalLength];
    int offset = 0;
    for (byte[] bytes : bytesList) {
      System.arraycopy(bytes, 0, result, offset, bytes.length);
      offset += bytes.length;
    }
    return result;
  }

  private static byte[] hashBinaryCollection(List<SdkBytes> values) {
    // Sort binary values for deterministic hashing
    // (DynamoDB sets are unordered, so iteration order is not guaranteed)
    List<byte[]> byteArrays = new ArrayList<>();
    for (SdkBytes b : values) {
      byteArrays.add(b.asByteArray());
    }
    Collections.sort(byteArrays, BYTE_ARRAY_COMPARATOR);

    int totalLength = 0;
    for (byte[] bytes : byteArrays) {
      totalLength += bytes.length;
    }

    byte[] result = new byte[totalLength];
    int offset = 0;
    for (byte[] bytes : byteArrays) {
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

  private static byte[] hashList(List<AttributeValue> values) {
    // Recursively hash each element and concatenate
    int totalLength = 0;
    List<byte[]> bytesList = new ArrayList<>();
    for (AttributeValue v : values) {
      byte[] bytes = toBytes(v);
      bytesList.add(bytes);
      totalLength += bytes.length;
    }

    byte[] result = new byte[totalLength];
    int offset = 0;
    for (byte[] bytes : bytesList) {
      System.arraycopy(bytes, 0, result, offset, bytes.length);
      offset += bytes.length;
    }
    return result;
  }

  private static byte[] hashMap(Map<String, AttributeValue> map) {
    // Sort keys lexicographically, then hash key-value pairs
    List<String> keys = new ArrayList<>(map.keySet());
    Collections.sort(keys);

    int totalLength = 0;
    List<byte[]> bytesList = new ArrayList<>();
    for (String key : keys) {
      byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
      byte[] valueBytes = toBytes(map.get(key));
      bytesList.add(keyBytes);
      bytesList.add(valueBytes);
      totalLength += keyBytes.length + valueBytes.length;
    }

    byte[] result = new byte[totalLength];
    int offset = 0;
    for (byte[] bytes : bytesList) {
      System.arraycopy(bytes, 0, result, offset, bytes.length);
      offset += bytes.length;
    }
    return result;
  }
}
