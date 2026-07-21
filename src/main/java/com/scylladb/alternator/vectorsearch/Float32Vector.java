// Copyright 2026-present ScyllaDB
//
// SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

package com.scylladb.alternator.vectorsearch;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * Utility class for the Alternator {@code FLOAT32VECTOR} attribute type.
 *
 * <p>Alternator stores vector attributes in a compact binary format on disk ({@code FLOAT32VECTOR})
 * rather than the standard DynamoDB list-of-numbers encoding ({@code L}). The standard AWS SDK for
 * Java has no knowledge of this type, so this class provides a marker-based encoding that the
 * {@link VectorSearchInterceptor} recognises and converts automatically:
 *
 * <ul>
 *   <li><b>Writes</b> — Call {@link #toAttributeValue(float[])} to create a {@code Binary} ({@code
 *       B}) {@link AttributeValue} that embeds a magic prefix followed by the raw IEEE-754
 *       big-endian float bytes. The interceptor detects the magic prefix in the serialised JSON and
 *       replaces the attribute with {@code {"FLOAT32VECTOR": [...]}} before transmission. This
 *       works transparently for {@code PutItem}, {@code UpdateItem} (in {@code
 *       ExpressionAttributeValues}), {@code BatchWriteItem}, and any other operation that carries
 *       {@link AttributeValue}s in its request body.
 *   <li><b>Reads</b> — When Alternator returns {@code {"FLOAT32VECTOR": [...]}} in a response, the
 *       interceptor converts it transparently to a standard DynamoDB list-of-numbers ({@code L})
 *       {@link AttributeValue}. The float values are accessible via {@link AttributeValue#l()} —
 *       each element is an {@code N}-typed {@link AttributeValue}.
 * </ul>
 *
 * <h2>Example — writing a vector item</h2>
 *
 * <pre>{@code
 * Map<String, AttributeValue> item = new HashMap<>();
 * item.put("pk", AttributeValue.fromS("item-1"));
 * item.put("embedding", Float32Vector.toAttributeValue(new float[]{0.1f, 0.2f, 0.3f}));
 * client.putItem(PutItemRequest.builder().tableName("t").item(item).build());
 * }</pre>
 *
 * <h2>Example — reading a vector back</h2>
 *
 * <pre>{@code
 * GetItemResponse resp = client.getItem(...);
 * AttributeValue embedding = resp.item().get("embedding");
 * List<AttributeValue> numbers = embedding.l(); // each element has type N
 * float[] values = new float[numbers.size()];
 * for (int i = 0; i < values.length; i++) {
 *     values[i] = Float.parseFloat(numbers.get(i).n());
 * }
 * }</pre>
 *
 * <h2>Requirement</h2>
 *
 * <p>{@link VectorSearchInterceptor#INSTANCE} must be registered on the DynamoDB client for the
 * automatic conversion to take effect. Without it, the magic-{@code B} attribute is sent as plain
 * binary data, which Alternator will not recognise as a vector.
 */
public final class Float32Vector {

  /**
   * 8-byte magic prefix that marks a Binary {@link AttributeValue} as a Float32Vector placeholder.
   *
   * <p>Chosen to be highly unlikely to appear at the start of legitimate binary data (probability
   * of random collision ≈ 1/2^64).
   */
  static final byte[] MAGIC = {
    (byte) 0xF2, (byte) 0xF3, (byte) 0x2F, (byte) 0xEC,
    (byte) 0x4A, (byte) 0x7B, (byte) 0x19, (byte) 0xD3
  };

  /**
   * The guaranteed base64 prefix of any Float32Vector-encoded {@link AttributeValue}'s {@code B}
   * field in the serialised DynamoDB JSON, derived from the first 6 bytes of {@link #MAGIC} (two
   * complete 3-byte base64 groups → 8 base64 characters).
   *
   * <p>Used internally by {@link VectorSearchInterceptor} for a fast substring scan of serialised
   * request bodies before committing to a full JSON parse.
   */
  static final String BASE64_PREFIX = "8vMv7Ep7";

  private Float32Vector() {}

  /**
   * Creates a DynamoDB Binary ({@code B}) {@link AttributeValue} that encodes {@code values} in the
   * Alternator {@code FLOAT32VECTOR} wire format.
   *
   * <p>When used in a write request on a client that has {@link VectorSearchInterceptor} registered
   * (via {@code .overrideConfiguration(c ->
   * c.addExecutionInterceptor(VectorSearchInterceptor.INSTANCE))}), this attribute value is
   * automatically converted to {@code {"FLOAT32VECTOR": [...]}} in the JSON body, enabling compact
   * on-disk storage.
   *
   * @param values the float array to encode; must not be {@code null}
   * @return a {@code B}-typed {@link AttributeValue} that the interceptor converts to {@code
   *     FLOAT32VECTOR}
   */
  public static AttributeValue toAttributeValue(float... values) {
    ByteBuffer buf =
        ByteBuffer.allocate(MAGIC.length + values.length * Float.BYTES).order(ByteOrder.BIG_ENDIAN);
    buf.put(MAGIC);
    for (float f : values) {
      buf.putFloat(f);
    }
    buf.flip();
    return AttributeValue.fromB(SdkBytes.fromByteBuffer(buf));
  }

  /**
   * Creates a DynamoDB Binary ({@code B}) {@link AttributeValue} that encodes the numbers in {@code
   * values} in the Alternator {@code FLOAT32VECTOR} wire format.
   *
   * <p>This overload is convenient for re-encoding a vector that was read back from Alternator as
   * an {@code L}-typed {@link AttributeValue} (each element is an {@code N}-typed {@link
   * AttributeValue}):
   *
   * <pre>{@code
   * AttributeValue read = resp.item().get("embedding"); // L type after round-trip
   * AttributeValue reEncoded = Float32Vector.toAttributeValue(read.l());
   * }</pre>
   *
   * @param values a list of {@code N}-typed {@link AttributeValue}s; must not be {@code null}
   * @return a {@code B}-typed {@link AttributeValue} that the interceptor converts to {@code
   *     FLOAT32VECTOR}
   * @throws NumberFormatException if any element's {@code n()} string is not a valid float
   */
  public static AttributeValue toAttributeValue(List<AttributeValue> values) {
    ByteBuffer buf =
        ByteBuffer.allocate(MAGIC.length + values.size() * Float.BYTES).order(ByteOrder.BIG_ENDIAN);
    buf.put(MAGIC);
    for (AttributeValue av : values) {
      buf.putFloat(Float.parseFloat(av.n()));
    }
    buf.flip();
    return AttributeValue.fromB(SdkBytes.fromByteBuffer(buf));
  }

  /**
   * Returns {@code true} if {@code av} is a Float32Vector-encoded {@link AttributeValue} — i.e., a
   * Binary ({@code B}) attribute whose bytes start with the Float32Vector magic prefix.
   *
   * <p>This identifies values created by {@link #toAttributeValue(float...)} or {@link
   * #toAttributeValue(List)} on the <em>write path</em>, before the interceptor has converted them
   * to {@code FLOAT32VECTOR} JSON. It does <em>not</em> apply to values read back from Alternator —
   * those arrive as standard {@code L}-typed {@link AttributeValue}s.
   *
   * @param av the {@link AttributeValue} to test; must not be {@code null}
   * @return {@code true} if {@code av} encodes a {@code FLOAT32VECTOR}
   */
  public static boolean isFloat32Vector(AttributeValue av) {
    return av.b() != null && hasFloat32VectorMagic(av.b().asByteArray());
  }

  /**
   * Extracts the float array from a Float32Vector-encoded {@link AttributeValue}.
   *
   * @param av an {@link AttributeValue} satisfying {@link #isFloat32Vector(AttributeValue)}
   * @return the decoded float array
   * @throws IllegalArgumentException if {@code av} is not a Float32Vector
   */
  public static float[] toFloats(AttributeValue av) {
    if (!isFloat32Vector(av)) {
      throw new IllegalArgumentException(
          "AttributeValue is not a Float32Vector "
              + "(expected a B attribute with the Float32Vector magic prefix)");
    }
    return bytesToFloats(av.b().asByteArray());
  }

  // -------------------------------------------------------------------------
  // Package-private helpers used by VectorSearchInterceptor
  // -------------------------------------------------------------------------

  /** Returns {@code true} if {@code bytes} starts with the Float32Vector magic prefix. */
  static boolean hasFloat32VectorMagic(byte[] bytes) {
    if (bytes.length < MAGIC.length) {
      return false;
    }
    for (int i = 0; i < MAGIC.length; i++) {
      if (bytes[i] != MAGIC[i]) {
        return false;
      }
    }
    return true;
  }

  /**
   * Decodes a float array from magic-prefixed bytes. The caller must have already verified the
   * magic prefix.
   */
  static float[] bytesToFloats(byte[] bytes) {
    int payload = bytes.length - MAGIC.length;
    if (payload < 0 || payload % Float.BYTES != 0) {
      throw new IllegalArgumentException(
          "Invalid Float32Vector payload length: "
              + bytes.length
              + " bytes (expected MAGIC.length + N * "
              + Float.BYTES
              + ")");
    }
    int floatCount = payload / Float.BYTES;
    float[] result = new float[floatCount];
    ByteBuffer buf =
        ByteBuffer.wrap(bytes, MAGIC.length, floatCount * Float.BYTES).order(ByteOrder.BIG_ENDIAN);
    for (int i = 0; i < floatCount; i++) {
      result[i] = buf.getFloat();
    }
    return result;
  }
}
