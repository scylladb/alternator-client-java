/*
 * Copyright ScyllaDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
 * {@link VectorSearchInterceptor} recognises and converts automatically. The marker deliberately
 * uses two DynamoDB union members ({@code B} and {@code NULL=false}), so it cannot be confused with
 * any valid DynamoDB attribute:
 *
 * <ul>
 *   <li><b>Writes</b> — Call {@link #toAttributeValue(float[])} to create a marker {@link
 *       AttributeValue} whose {@code B} member embeds a magic prefix followed by the raw IEEE-754
 *       big-endian float bytes. The interceptor detects the complete marker in the serialised JSON
 *       and replaces the attribute with {@code {"FLOAT32VECTOR": [...]}} before transmission. This
 *       works transparently for {@code PutItem}, {@code UpdateItem} (in {@code
 *       ExpressionAttributeValues}), {@code BatchWriteItem}, and any other operation that carries
 *       {@link AttributeValue}s in its request body.
 *   <li><b>Reads</b> — When Alternator returns {@code {"FLOAT32VECTOR": [...]}} in a response, the
 *       interceptor converts it transparently to the same marker. Use {@link
 *       #isFloat32Vector(AttributeValue)} to identify it and {@link #toFloats(AttributeValue)} to
 *       access its values. Passing a returned marker back to a write preserves the compact {@code
 *       FLOAT32VECTOR} storage type.
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
 * if (Float32Vector.isFloat32Vector(embedding)) {
 *     float[] values = Float32Vector.toFloats(embedding);
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
   * Creates an {@link AttributeValue} marker that encodes {@code values} for conversion to the
   * Alternator {@code FLOAT32VECTOR} wire format.
   *
   * <p>When used in a write request on a client that has {@link VectorSearchInterceptor} registered
   * (via {@code .overrideConfiguration(c ->
   * c.addExecutionInterceptor(VectorSearchInterceptor.INSTANCE))}), this attribute value is
   * automatically converted to {@code {"FLOAT32VECTOR": [...]}} in the JSON body, enabling compact
   * on-disk storage.
   *
   * @param values the float array to encode; must not be {@code null}
   * @return a marker {@link AttributeValue} that the interceptor converts to {@code FLOAT32VECTOR}
   */
  public static AttributeValue toAttributeValue(float... values) {
    ByteBuffer buf =
        ByteBuffer.allocate(MAGIC.length + values.length * Float.BYTES).order(ByteOrder.BIG_ENDIAN);
    buf.put(MAGIC);
    for (float f : values) {
      buf.putFloat(f);
    }
    buf.flip();
    return marker(SdkBytes.fromByteBuffer(buf));
  }

  /**
   * Creates a DynamoDB Binary ({@code B}) {@link AttributeValue} that encodes the numbers in {@code
   * values} in the Alternator {@code FLOAT32VECTOR} wire format.
   *
   * <p>This overload is convenient for converting an ordinary DynamoDB {@code L}-typed vector
   * (whose elements are {@code N}-typed {@link AttributeValue}s) to the optimized storage type:
   *
   * <pre>{@code
   * AttributeValue ordinaryList = ...;
   * AttributeValue optimized = Float32Vector.toAttributeValue(ordinaryList.l());
   * }</pre>
   *
   * @param values a list of {@code N}-typed {@link AttributeValue}s; must not be {@code null}
   * @return a marker {@link AttributeValue} that the interceptor converts to {@code FLOAT32VECTOR}
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
    return marker(SdkBytes.fromByteBuffer(buf));
  }

  /**
   * Returns {@code true} if {@code av} is a Float32Vector marker — i.e., it has the deliberately
   * invalid DynamoDB union combination {@code B} plus {@code NULL=false}, its binary bytes start
   * with the Float32Vector magic prefix, and the remaining payload contains a whole number of
   * 32-bit floats.
   *
   * <p>This identifies values created by {@link #toAttributeValue(float...)} or {@link
   * #toAttributeValue(List)}, as well as optimized vector attributes read back through {@link
   * VectorSearchInterceptor}.
   *
   * @param av the {@link AttributeValue} to test; must not be {@code null}
   * @return {@code true} if {@code av} encodes a {@code FLOAT32VECTOR}
   */
  public static boolean isFloat32Vector(AttributeValue av) {
    return av.b() != null
        && Boolean.FALSE.equals(av.nul())
        && hasFloat32VectorMagic(av.b().asByteArray());
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
          "AttributeValue is not a Float32Vector " + "(expected the Float32Vector B/NULL marker)");
    }
    return bytesToFloats(av.b().asByteArray());
  }

  // -------------------------------------------------------------------------
  // Package-private helpers used by VectorSearchInterceptor
  // -------------------------------------------------------------------------

  private static AttributeValue marker(SdkBytes bytes) {
    // AttributeValue is a union, so B + NULL=false is not a valid user value. The extra member
    // makes the placeholder unambiguous even if ordinary binary data starts with MAGIC.
    return AttributeValue.builder().b(bytes).nul(false).build();
  }

  /**
   * Returns {@code true} if {@code bytes} starts with the Float32Vector magic prefix and has an
   * aligned float payload.
   */
  static boolean hasFloat32VectorMagic(byte[] bytes) {
    int payloadLength = bytes.length - MAGIC.length;
    if (payloadLength < 0 || payloadLength % Float.BYTES != 0) {
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
