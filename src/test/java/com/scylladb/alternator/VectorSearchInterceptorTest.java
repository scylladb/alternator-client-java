// Copyright 2026-present ScyllaDB
//
// SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

package com.scylladb.alternator;

import static org.junit.Assert.*;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.scylladb.alternator.vectorsearch.*;
import java.util.*;
import org.junit.Test;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * Unit tests for {@link VectorSearchInterceptor} and {@link VectorSearchSupport}. These tests do
 * not require a running Alternator instance and verify the JSON serialisation logic.
 */
public class VectorSearchInterceptorTest {

  // -------------------------------------------------------------------------
  // VectorAttribute / VectorIndex serialisation
  // -------------------------------------------------------------------------

  @Test
  public void testVectorAttributeBuilder() {
    VectorAttribute va =
        VectorAttribute.builder().attributeName("embedding").dimensions(128).build();
    assertEquals("embedding", va.attributeName());
    assertEquals(128, va.dimensions());
  }

  @Test
  public void testVectorIndexBuilder() {
    VectorAttribute va = VectorAttribute.builder().attributeName("v").dimensions(4).build();
    VectorIndex vi =
        VectorIndex.builder()
            .indexName("idx")
            .vectorAttribute(va)
            .similarityFunction("COSINE")
            .build();

    assertEquals("idx", vi.indexName());
    assertEquals("v", vi.vectorAttribute().attributeName());
    assertEquals(4, vi.vectorAttribute().dimensions());
    assertEquals("COSINE", vi.similarityFunction());
    assertNull(vi.projection());
    assertNull(vi.indexStatus());
    assertNull(vi.backfilling());
  }

  // -------------------------------------------------------------------------
  // VectorSearch builder
  // -------------------------------------------------------------------------

  @Test
  public void testVectorSearchBuilderWithFloatArray() {
    float[] floats = {0.1f, 0.2f, 0.3f};
    VectorSearch vs = VectorSearch.builder().queryVector(floats).returnScores(true).build();

    assertArrayEquals(floats, vs.queryVectorFloats(), 1e-6f);
    assertNull(vs.queryVectorAttributeValue());
    assertTrue(vs.returnScores());
  }

  @Test
  public void testVectorSearchBuilderWithAttributeValue() {
    AttributeValue av = AttributeValue.fromN("42");
    VectorSearch vs = VectorSearch.builder().queryVector(av).build();

    assertNull(vs.queryVectorFloats());
    assertEquals(av, vs.queryVectorAttributeValue());
    assertFalse(vs.returnScores());
  }

  @Test(expected = IllegalStateException.class)
  public void testVectorSearchBuilderRequiresQueryVector() {
    VectorSearch.builder().returnScores(true).build();
  }

  // -------------------------------------------------------------------------
  // attributeValueToJson
  // -------------------------------------------------------------------------

  @Test
  public void testAttributeValueToJsonString() throws Exception {
    AttributeValue av = AttributeValue.fromS("hello");
    ObjectNode node = VectorSearchInterceptor.attributeValueToJson(av);
    assertEquals("hello", node.get("S").asText());
    assertEquals(1, node.size());
  }

  @Test
  public void testAttributeValueToJsonNumber() throws Exception {
    AttributeValue av = AttributeValue.fromN("42.5");
    ObjectNode node = VectorSearchInterceptor.attributeValueToJson(av);
    assertEquals("42.5", node.get("N").asText());
    assertEquals(1, node.size());
  }

  @Test
  public void testAttributeValueToJsonList() throws Exception {
    AttributeValue av =
        AttributeValue.fromL(
            Arrays.asList(
                AttributeValue.fromN("1.0"),
                AttributeValue.fromN("2.0"),
                AttributeValue.fromN("3.0")));
    ObjectNode node = VectorSearchInterceptor.attributeValueToJson(av);
    assertNotNull(node.get("L"));
    assertEquals(3, node.get("L").size());
    assertEquals("1.0", node.get("L").get(0).get("N").asText());
    assertEquals("2.0", node.get("L").get(1).get("N").asText());
    assertEquals("3.0", node.get("L").get(2).get("N").asText());
  }

  @Test
  public void testAttributeValueToJsonBool() throws Exception {
    ObjectNode trueNode =
        VectorSearchInterceptor.attributeValueToJson(AttributeValue.fromBool(true));
    assertTrue(trueNode.get("BOOL").asBoolean());

    ObjectNode falseNode =
        VectorSearchInterceptor.attributeValueToJson(AttributeValue.fromBool(false));
    assertFalse(falseNode.get("BOOL").asBoolean());
  }

  @Test
  public void testAttributeValueToJsonNull() throws Exception {
    AttributeValue av = AttributeValue.fromNul(true);
    ObjectNode node = VectorSearchInterceptor.attributeValueToJson(av);
    assertTrue(node.get("NULL").asBoolean());
  }

  // -------------------------------------------------------------------------
  // VectorSearchSupport helpers
  // -------------------------------------------------------------------------

  @Test
  public void testWithVectorIndexesSetsOverrideConfiguration() {
    VectorIndex vi =
        VectorIndex.builder()
            .indexName("idx")
            .vectorAttribute(VectorAttribute.builder().attributeName("v").dimensions(2).build())
            .build();

    software.amazon.awssdk.services.dynamodb.model.CreateTableRequest base =
        software.amazon.awssdk.services.dynamodb.model.CreateTableRequest.builder()
            .tableName("test")
            .keySchema(
                software.amazon.awssdk.services.dynamodb.model.KeySchemaElement.builder()
                    .attributeName("pk")
                    .keyType(software.amazon.awssdk.services.dynamodb.model.KeyType.HASH)
                    .build())
            .attributeDefinitions(
                software.amazon.awssdk.services.dynamodb.model.AttributeDefinition.builder()
                    .attributeName("pk")
                    .attributeType(
                        software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType.S)
                    .build())
            .billingMode(software.amazon.awssdk.services.dynamodb.model.BillingMode.PAY_PER_REQUEST)
            .build();

    software.amazon.awssdk.services.dynamodb.model.CreateTableRequest enriched =
        VectorSearchSupport.withVectorIndexes(base, Collections.singletonList(vi));

    assertNotNull(enriched);
    assertTrue(enriched.overrideConfiguration().isPresent());
    // The VECTOR_INDEXES attribute must be set
    software.amazon.awssdk.core.interceptor.ExecutionAttributes ea =
        enriched.overrideConfiguration().get().executionAttributes();
    List<VectorIndex> vis = ea.getAttribute(VectorSearchInterceptor.VECTOR_INDEXES);
    assertNotNull(vis);
    assertEquals(1, vis.size());
    assertEquals("idx", vis.get(0).indexName());
  }

  @Test
  public void testVectorIndexUpdateBuilder() {
    CreateVectorIndexAction createAction =
        CreateVectorIndexAction.builder()
            .indexName("new-idx")
            .vectorAttribute(VectorAttribute.builder().attributeName("v").dimensions(3).build())
            .similarityFunction("DOT_PRODUCT")
            .build();
    VectorIndexUpdate update = VectorIndexUpdate.builder().create(createAction).build();

    assertNotNull(update.create());
    assertNull(update.delete());
    assertEquals("new-idx", update.create().indexName());
    assertEquals("DOT_PRODUCT", update.create().similarityFunction());

    DeleteVectorIndexAction deleteAction =
        DeleteVectorIndexAction.builder().indexName("old-idx").build();
    VectorIndexUpdate deleteUpdate = VectorIndexUpdate.builder().delete(deleteAction).build();

    assertNull(deleteUpdate.create());
    assertNotNull(deleteUpdate.delete());
    assertEquals("old-idx", deleteUpdate.delete().indexName());
  }

  @Test
  public void testVectorQueryResultEmptyScores() {
    software.amazon.awssdk.services.dynamodb.model.QueryResponse response =
        software.amazon.awssdk.services.dynamodb.model.QueryResponse.builder().build();
    VectorQueryResult result = new VectorQueryResult(response, null);
    assertNotNull(result.scores());
    assertTrue(result.scores().isEmpty());
  }

  @Test
  public void testVectorQueryResultWithScores() {
    software.amazon.awssdk.services.dynamodb.model.QueryResponse response =
        software.amazon.awssdk.services.dynamodb.model.QueryResponse.builder().build();
    List<Double> scores = Arrays.asList(0.9, 0.8, 0.7);
    VectorQueryResult result = new VectorQueryResult(response, scores);
    assertEquals(3, result.scores().size());
    assertEquals(0.9, result.scores().get(0), 1e-9);
  }

  // -------------------------------------------------------------------------
  // Float32Vector — encoding / decoding
  // -------------------------------------------------------------------------

  @Test
  public void testFloat32VectorRoundTrip() {
    float[] original = {1.0f, -2.5f, 0.0f, Float.MAX_VALUE, Float.MIN_VALUE};
    AttributeValue av = Float32Vector.toAttributeValue(original);

    assertTrue("toAttributeValue must produce a B attribute", av.b() != null);
    assertTrue("isFloat32Vector must return true", Float32Vector.isFloat32Vector(av));

    float[] decoded = Float32Vector.toFloats(av);
    assertArrayEquals("round-trip must be lossless", original, decoded, 0.0f);
  }

  @Test
  public void testFloat32VectorEmptyArray() {
    float[] empty = {};
    AttributeValue av = Float32Vector.toAttributeValue(empty);
    assertTrue(Float32Vector.isFloat32Vector(av));
    assertArrayEquals(empty, Float32Vector.toFloats(av), 0.0f);
  }

  @Test
  public void testFloat32VectorIsNotStringAttribute() {
    assertFalse(Float32Vector.isFloat32Vector(AttributeValue.fromS("hello")));
  }

  @Test
  public void testFloat32VectorIsNotNumberAttribute() {
    assertFalse(Float32Vector.isFloat32Vector(AttributeValue.fromN("42")));
  }

  @Test
  public void testFloat32VectorIsNotArbitraryBinaryAttribute() {
    // A B attribute without the magic prefix must not be considered a Float32Vector.
    assertFalse(
        Float32Vector.isFloat32Vector(
            AttributeValue.fromB(SdkBytes.fromByteArray(new byte[] {1, 2, 3, 4, 5, 6, 7, 8}))));
  }

  @Test
  public void testFloat32VectorBinaryTooShortForMagic() {
    assertFalse(
        Float32Vector.isFloat32Vector(
            AttributeValue.fromB(SdkBytes.fromByteArray(new byte[] {(byte) 0xF2}))));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testFloat32VectorToFloatsRequiresMagic() {
    Float32Vector.toFloats(AttributeValue.fromS("not a vector"));
  }

  @Test
  public void testFloat32VectorBase64PrefixIsCorrect() {
    // Build a Float32Vector and verify the B field in its JSON representation
    // starts with a known fixed prefix (derived from the 8-byte magic).
    float[] values = {1.0f, 2.0f};
    AttributeValue av = Float32Vector.toAttributeValue(values);
    String b64 = java.util.Base64.getEncoder().encodeToString(av.b().asByteArray());
    // The first 8 base64 chars are fully determined by the first 6 magic bytes (two groups of 3).
    // We verify stability of the encoding rather than hard-coding the string here.
    AttributeValue av2 = Float32Vector.toAttributeValue(new float[] {9.0f, 8.0f});
    String b64_2 = java.util.Base64.getEncoder().encodeToString(av2.b().asByteArray());
    assertEquals(
        "All Float32Vectors must share the same 8-char base64 prefix",
        b64.substring(0, 8),
        b64_2.substring(0, 8));
  }

  // -------------------------------------------------------------------------
  // Float32Vector — JSON-level request replacement (B → FLOAT32VECTOR)
  // -------------------------------------------------------------------------

  @Test
  public void testAttributeValueToJsonFloat32VectorUsesCompactFormat() throws Exception {
    // When Float32Vector.toAttributeValue() is used, attributeValueToJson should emit
    // {"FLOAT32VECTOR": [...]} rather than the generic {"B": "..."}.
    float[] values = {0.5f, -1.5f};
    AttributeValue av = Float32Vector.toAttributeValue(values);
    ObjectNode json = VectorSearchInterceptor.attributeValueToJson(av);

    assertNull("B key must be absent for Float32Vector", json.get("B"));
    assertNotNull("FLOAT32VECTOR key must be present", json.get("FLOAT32VECTOR"));
    assertEquals(2, json.get("FLOAT32VECTOR").size());
    assertEquals(0.5f, (float) json.get("FLOAT32VECTOR").get(0).asDouble(), 1e-6f);
    assertEquals(-1.5f, (float) json.get("FLOAT32VECTOR").get(1).asDouble(), 1e-6f);
  }

  @Test
  public void testAttributeValueToJsonOrdinaryBinaryIsBase64() throws Exception {
    // A regular B attribute (no magic) must be emitted as base64 string.
    byte[] rawBytes = {1, 2, 3, 4};
    AttributeValue av = AttributeValue.fromB(SdkBytes.fromByteArray(rawBytes));
    ObjectNode json = VectorSearchInterceptor.attributeValueToJson(av);

    assertNotNull("B key must be present for plain binary", json.get("B"));
    assertNull("FLOAT32VECTOR key must be absent for plain binary", json.get("FLOAT32VECTOR"));
  }

  // -------------------------------------------------------------------------
  // Float32Vector — JSON-level response replacement (FLOAT32VECTOR → L)
  // -------------------------------------------------------------------------

  @Test
  public void testFloat32VectorWritePathProducesCompactFormat() throws Exception {
    // Verify that encoding float[] → toAttributeValue → attributeValueToJson produces
    // {"FLOAT32VECTOR": [...]} (compact wire format for writes).
    float[] original = {1.0f, 2.0f, 3.0f};

    AttributeValue av = Float32Vector.toAttributeValue(original);
    ObjectNode asJson = VectorSearchInterceptor.attributeValueToJson(av);
    assertNotNull(
        "FLOAT32VECTOR key must be present in compact wire format", asJson.get("FLOAT32VECTOR"));
    assertNull("B key must be absent for Float32Vector", asJson.get("B"));
    assertEquals(3, asJson.get("FLOAT32VECTOR").size());
  }

  @Test
  public void testFloat32VectorBytesRoundTrip() {
    // Round-trip through toAttributeValue → isFloat32Vector → toFloats
    float[] values = {-1.0f, 0.0f, 3.14159f};
    AttributeValue av = Float32Vector.toAttributeValue(values);
    assertTrue(Float32Vector.isFloat32Vector(av));
    float[] back = Float32Vector.toFloats(av);
    assertArrayEquals(values, back, 1e-6f);
  }

  @Test
  public void testFloat32VectorFromListRoundTrip() {
    // Simulate the read-back path: interceptor returns an L-typed AttributeValue;
    // re-encoding it via toAttributeValue(List) must produce the same result as
    // toAttributeValue(float...) with the original values.
    float[] original = {1.5f, -2.5f, 0.0f};
    AttributeValue fromFloats = Float32Vector.toAttributeValue(original);

    // Build an L-typed AttributeValue as the interceptor would return
    List<AttributeValue> numbers =
        java.util.Arrays.asList(
            AttributeValue.fromN("1.5"), AttributeValue.fromN("-2.5"), AttributeValue.fromN("0.0"));
    AttributeValue fromList = Float32Vector.toAttributeValue(numbers);

    assertArrayEquals(fromFloats.b().asByteArray(), fromList.b().asByteArray());
  }
}
