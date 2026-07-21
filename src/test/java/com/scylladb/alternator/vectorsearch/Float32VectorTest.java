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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Base64;
import java.util.Optional;
import org.junit.Test;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.InterceptorContext;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

public class Float32VectorTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  public void recognizesEmptyAndAlignedPayloads() {
    AttributeValue empty = Float32Vector.toAttributeValue();
    AttributeValue aligned = Float32Vector.toAttributeValue(1.0f, -2.5f);

    assertTrue(Float32Vector.isFloat32Vector(empty));
    assertTrue(Float32Vector.isFloat32Vector(aligned));
    assertTrue(Float32Vector.hasFloat32VectorMagic(empty.b().asByteArray()));
    assertTrue(Float32Vector.hasFloat32VectorMagic(aligned.b().asByteArray()));
    assertEquals(Boolean.FALSE, empty.nul());
    assertEquals(Boolean.FALSE, aligned.nul());
    assertArrayEquals(new float[0], Float32Vector.toFloats(empty), 0.0f);
    assertArrayEquals(new float[] {1.0f, -2.5f}, Float32Vector.toFloats(aligned), 0.0f);
  }

  @Test
  public void rejectsOrdinaryBinaryEvenWhenItContainsAlignedMarkerBytes() {
    AttributeValue marker = Float32Vector.toAttributeValue(1.0f, -2.5f);
    AttributeValue ordinaryBinary = AttributeValue.fromB(marker.b());

    assertTrue(Float32Vector.hasFloat32VectorMagic(ordinaryBinary.b().asByteArray()));
    assertFalse(Float32Vector.isFloat32Vector(ordinaryBinary));
    assertThrows(IllegalArgumentException.class, () -> Float32Vector.toFloats(ordinaryBinary));
  }

  @Test
  public void rejectsMagicPrefixWithMisalignedPayload() {
    byte[] malformedBytes = malformedMarkerBytes();
    AttributeValue malformed = AttributeValue.fromB(SdkBytes.fromByteArray(malformedBytes));

    assertFalse(Float32Vector.hasFloat32VectorMagic(malformedBytes));
    assertFalse(Float32Vector.isFloat32Vector(malformed));
    assertThrows(IllegalArgumentException.class, () -> Float32Vector.toFloats(malformed));
  }

  @Test
  public void requestRewritingLeavesMisalignedMagicBinaryUntouched() throws Exception {
    byte[] malformedBytes = malformedMarkerBytes();
    String encoded = Base64.getEncoder().encodeToString(malformedBytes);
    assertTrue(encoded.startsWith(Float32Vector.BASE64_PREFIX));

    ObjectNode requestJson = MAPPER.createObjectNode();
    requestJson.put("TableName", "items");
    requestJson.putObject("Item").putObject("embedding").put("B", encoded);
    byte[] originalBody = MAPPER.writeValueAsBytes(requestJson);

    InterceptorContext context =
        InterceptorContext.builder()
            .request(PutItemRequest.builder().tableName("items").build())
            .httpRequest(putItemHttpRequest())
            .requestBody(RequestBody.fromBytes(originalBody))
            .build();

    Optional<RequestBody> processed =
        VectorSearchInterceptor.INSTANCE.modifyHttpContent(context, new ExecutionAttributes());
    assertTrue(processed.isPresent());

    byte[] processedBody;
    try (InputStream in = processed.get().contentStreamProvider().newStream()) {
      processedBody = in.readAllBytes();
    }
    assertArrayEquals(originalBody, processedBody);

    JsonNode embedding = MAPPER.readTree(processedBody).get("Item").get("embedding");
    assertTrue(embedding.has("B"));
    assertFalse(embedding.has("FLOAT32VECTOR"));
  }

  @Test
  public void requestRewritingLeavesAlignedMagicBinaryWithoutSentinelUntouched() throws Exception {
    AttributeValue marker = Float32Vector.toAttributeValue(1.0f, -2.5f);
    String encoded = Base64.getEncoder().encodeToString(marker.b().asByteArray());
    assertTrue(encoded.startsWith(Float32Vector.BASE64_PREFIX));

    ObjectNode requestJson = MAPPER.createObjectNode();
    requestJson.put("TableName", "items");
    requestJson.putObject("Item").putObject("embedding").put("B", encoded);
    byte[] originalBody = MAPPER.writeValueAsBytes(requestJson);

    InterceptorContext context =
        InterceptorContext.builder()
            .request(PutItemRequest.builder().tableName("items").build())
            .httpRequest(putItemHttpRequest())
            .requestBody(RequestBody.fromBytes(originalBody))
            .build();

    Optional<RequestBody> processed =
        VectorSearchInterceptor.INSTANCE.modifyHttpContent(context, new ExecutionAttributes());
    assertTrue(processed.isPresent());

    byte[] processedBody;
    try (InputStream in = processed.get().contentStreamProvider().newStream()) {
      processedBody = in.readAllBytes();
    }
    assertArrayEquals(originalBody, processedBody);

    JsonNode embedding = MAPPER.readTree(processedBody).get("Item").get("embedding");
    assertTrue(embedding.has("B"));
    assertFalse(embedding.has("NULL"));
    assertFalse(embedding.has("FLOAT32VECTOR"));
  }

  private static byte[] malformedMarkerBytes() {
    byte[] bytes = Arrays.copyOf(Float32Vector.MAGIC, Float32Vector.MAGIC.length + 1);
    bytes[bytes.length - 1] = 0x01;
    return bytes;
  }

  private static SdkHttpRequest putItemHttpRequest() {
    return SdkHttpRequest.builder()
        .protocol("http")
        .host("localhost")
        .port(8000)
        .method(SdkHttpMethod.POST)
        .encodedPath("/")
        .putHeader("X-Amz-Target", "DynamoDB_20120810.PutItem")
        .putHeader("Content-Type", "application/x-amz-json-1.0")
        .build();
  }
}
