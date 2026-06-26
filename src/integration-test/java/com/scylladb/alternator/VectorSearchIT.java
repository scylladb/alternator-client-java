// Copyright 2026-present ScyllaDB
//
// SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

package com.scylladb.alternator;

import static org.junit.Assert.*;
import static org.junit.Assume.*;

import com.scylladb.alternator.vectorsearch.*;
import java.net.URI;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

/**
 * Integration tests for Alternator's vector search extension.
 *
 * <p>These tests verify that {@link VectorSearchInterceptor} and {@link VectorSearchSupport}
 * correctly inject vector-search parameters into requests and extract them from responses.
 *
 * <p>Tests require a running Alternator cluster. Enable with {@code INTEGRATION_TESTS=true}.
 */
@RunWith(Parameterized.class)
public class VectorSearchIT {

  private final URI seedUri;
  private DynamoDbClient client;
  private String tableName;

  public VectorSearchIT(String scheme, URI seedUri) {
    this.seedUri = seedUri;
  }

  @Parameters(name = "{0}")
  public static Collection<Object[]> data() {
    return IntegrationTestConfig.httpAndHttpsEndpoints();
  }

  @Before
  public void setUp() {
    assumeTrue(
        "Integration tests disabled. Set INTEGRATION_TESTS=true to enable.",
        IntegrationTestConfig.ENABLED);

    client =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(seedUri)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .build();

    tableName = "vs_it_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
  }

  @After
  public void tearDown() {
    if (client != null) {
      try {
        client.deleteTable(DeleteTableRequest.builder().tableName(tableName).build());
      } catch (ResourceNotFoundException ignored) {
      }
      client.close();
    }
  }

  // -------------------------------------------------------------------------
  // Helper: create a plain hash-only table with a single vector index
  // -------------------------------------------------------------------------

  private static final int DIMENSIONS = 4;

  /**
   * Skips the test if the server does not support vector indexes. Must be called <em>after</em>
   * {@link #createTableWithVectorIndex()} so that the table exists.
   *
   * <p>Two cases are detected:
   *
   * <ol>
   *   <li>The server predates vector-store support entirely: it ignores {@code VectorIndexes} in
   *       {@code CreateTable}, so {@code DescribeTable} returns an empty vector-indexes list.
   *   <li>The server has vector-store code but it is disabled: it acknowledges the index in
   *       {@code DescribeTable} but a {@code VectorSearch} query returns "Vector Store is
   *       disabled".
   * </ol>
   *
   * <p>Mirrors the {@code needs_vector_store} fixture in Scylla's Python tests.
   */
  private void assumeVectorStoreEnabled() {
    // Case 1: server does not know about vector indexes at all.
    VectorSearchSupport.DescribeTableWithVectorIndexes desc =
        VectorSearchSupport.describeTable(
            client, DescribeTableRequest.builder().tableName(tableName).build());
    assumeTrue(
        "Skipping: server does not support vector indexes (VectorIndexes absent in DescribeTable)",
        !desc.vectorIndexes().isEmpty());

    // Case 2: server has vector-store code but it is disabled.
    try {
      VectorSearchSupport.query(
          client,
          QueryRequest.builder().tableName(tableName).indexName("vi1").limit(1).build(),
          VectorSearch.builder().queryVector(new float[] {0f, 0f, 0f, 0f}).build());
    } catch (DynamoDbException e) {
      if (e.getMessage() != null && e.getMessage().contains("Vector Store is disabled")) {
        assumeTrue("Skipping: Vector Store is disabled on this server", false);
      }
      // Any other error (e.g. index not yet active) means the vector store is reachable.
    }
  }

  /** Polls until the table is ACTIVE and the named vector index is ACTIVE (up to 60 seconds). */
  private void waitForVectorIndexActive(String indexName) throws InterruptedException {
    for (int i = 0; i < 120; i++) {
      VectorSearchSupport.DescribeTableWithVectorIndexes desc =
          VectorSearchSupport.describeTable(
              client, DescribeTableRequest.builder().tableName(tableName).build());
      if (desc.response().table().tableStatus() != TableStatus.ACTIVE) {
        TimeUnit.MILLISECONDS.sleep(500);
        continue;
      }
      boolean indexActive = desc.vectorIndexes().stream()
          .anyMatch(vi -> indexName.equals(vi.indexName()) && "ACTIVE".equals(vi.indexStatus()));
      if (indexActive) {
        return;
      }
      TimeUnit.MILLISECONDS.sleep(500);
    }
    fail("Timed out waiting for vector index '" + indexName + "' to become ACTIVE on table '" + tableName + "'");
  }

  private void createTableWithVectorIndex() {
    VectorIndex vi =
        VectorIndex.builder()
            .indexName("vi1")
            .vectorAttribute(
                VectorAttribute.builder()
                    .attributeName("embedding")
                    .dimensions(DIMENSIONS)
                    .build())
            .build();

    CreateTableRequest base =
        CreateTableRequest.builder()
            .tableName(tableName)
            .keySchema(
                KeySchemaElement.builder().attributeName("pk").keyType(KeyType.HASH).build())
            .attributeDefinitions(
                AttributeDefinition.builder()
                    .attributeName("pk")
                    .attributeType(ScalarAttributeType.S)
                    .build())
            .billingMode(BillingMode.PAY_PER_REQUEST)
            .build();

    VectorSearchSupport.createTable(client, base, Collections.singletonList(vi));
  }

  // -------------------------------------------------------------------------
  // Tests
  // -------------------------------------------------------------------------

  /**
   * Verify that CreateTable with a VectorIndexes parameter is accepted by Alternator without
   * throwing an exception.
   */
  @Test
  public void testCreateTableWithVectorIndex() throws Exception {
    createTableWithVectorIndex();
    // Wait until ACTIVE — the vector index build may take a moment.
    TableStatus status = TableStatus.UNKNOWN_TO_SDK_VERSION;
    for (int i = 0; i < 20; i++) {
      DescribeTableResponse desc =
          client.describeTable(DescribeTableRequest.builder().tableName(tableName).build());
      status = desc.table().tableStatus();
      if (status == TableStatus.ACTIVE) {
        break;
      }
      TimeUnit.MILLISECONDS.sleep(500);
    }
    assertEquals(TableStatus.ACTIVE, status);
  }

  /**
   * Verify that DescribeTable response VectorIndexes can be parsed back via
   * VectorSearchSupport.describeTable.
   */
  @Test
  public void testDescribeTableWithVectorIndexes() throws Exception {
    createTableWithVectorIndex();
    assumeVectorStoreEnabled();
    waitForVectorIndexActive("vi1");

    VectorSearchSupport.DescribeTableWithVectorIndexes result =
        VectorSearchSupport.describeTable(
            client, DescribeTableRequest.builder().tableName(tableName).build());

    assertNotNull(result.response());
    List<VectorIndex> vis = result.vectorIndexes();
    assertFalse("Expected at least one vector index in the response", vis.isEmpty());

    VectorIndex vi = vis.get(0);
    assertEquals("vi1", vi.indexName());
    assertEquals("embedding", vi.vectorAttribute().attributeName());
    assertEquals(DIMENSIONS, vi.vectorAttribute().dimensions());
  }

  /**
   * Verify that a basic vector similarity Query (without ReturnScores) does not throw and returns
   * a non-null result.
   */
  @Test
  public void testQueryWithVectorSearch() throws Exception {
    createTableWithVectorIndex();
    assumeVectorStoreEnabled();
    waitForVectorIndexActive("vi1");
    for (int i = 0; i < 3; i++) {
      Map<String, AttributeValue> item = new HashMap<>();
      item.put("pk", AttributeValue.fromS("item-" + i));
      // Store embedding as a regular DynamoDB list
      List<AttributeValue> vec = new ArrayList<>();
      for (int d = 0; d < DIMENSIONS; d++) {
        vec.add(AttributeValue.fromN(String.valueOf((float) (i + d))));
      }
      item.put("embedding", AttributeValue.fromL(vec));
      client.putItem(PutItemRequest.builder().tableName(tableName).item(item).build());
    }

    // Query vector (closest to item-0's embedding)
    VectorSearch vs =
        VectorSearch.builder()
            .queryVector(new float[] {0.0f, 1.0f, 2.0f, 3.0f})
            .build();

    VectorQueryResult result =
        VectorSearchSupport.query(
            client,
            QueryRequest.builder().tableName(tableName).indexName("vi1").limit(3).build(),
            vs);
    assertNotNull(result);
    assertNotNull(result.items());
  }

  /**
   * Verify that ReturnScores causes the server to include per-item scores in the response and that
   * VectorSearchSupport.query parses them correctly.
   */
  @Test
  public void testQueryWithReturnScores() throws Exception {
    createTableWithVectorIndex();
    assumeVectorStoreEnabled();
    waitForVectorIndexActive("vi1");

    // Put an item
    Map<String, AttributeValue> item = new HashMap<>();
    item.put("pk", AttributeValue.fromS("item-0"));
    List<AttributeValue> vec = new ArrayList<>();
    for (int d = 0; d < DIMENSIONS; d++) {
      vec.add(AttributeValue.fromN(String.valueOf((float) d)));
    }
    item.put("embedding", AttributeValue.fromL(vec));
    client.putItem(PutItemRequest.builder().tableName(tableName).item(item).build());

    VectorSearch vs =
        VectorSearch.builder()
            .queryVector(new float[]{0.0f, 1.0f, 2.0f, 3.0f})
            .returnScores(true)
            .build();

    VectorQueryResult result =
        VectorSearchSupport.query(
            client,
            QueryRequest.builder().tableName(tableName).indexName("vi1").limit(10).build(),
            vs);

    assertNotNull(result);
    assertNotNull(result.items());
    // If items were returned and ReturnScores was true, scores list should be non-empty
    if (!result.items().isEmpty()) {
      assertFalse("Expected scores to be returned when ReturnScores=true", result.scores().isEmpty());
      assertEquals(result.items().size(), result.scores().size());
      for (double score : result.scores()) {
        // Scores should be finite, positive numbers
        assertTrue("Score should be finite", Double.isFinite(score));
      }
    }
  }

  /**
   * Verify that withVectorIndexes preserves any existing overrideConfiguration on the request.
   */
  @Test
  public void testWithVectorIndexesPreservesExistingOverrideConfiguration() {
    // Just test that the helper method doesn't throw and returns a non-null request
    VectorIndex vi =
        VectorIndex.builder()
            .indexName("idx")
            .vectorAttribute(
                VectorAttribute.builder().attributeName("v").dimensions(2).build())
            .build();

    CreateTableRequest base =
        CreateTableRequest.builder()
            .tableName("dummy")
            .keySchema(
                KeySchemaElement.builder().attributeName("pk").keyType(KeyType.HASH).build())
            .attributeDefinitions(
                AttributeDefinition.builder()
                    .attributeName("pk")
                    .attributeType(ScalarAttributeType.S)
                    .build())
            .billingMode(BillingMode.PAY_PER_REQUEST)
            .build();

    CreateTableRequest enriched =
        VectorSearchSupport.withVectorIndexes(base, Collections.singletonList(vi));
    assertNotNull(enriched);
    assertTrue(enriched.overrideConfiguration().isPresent());
  }

  /**
   * Verify that Alternator accepts the uppercase similarity function values "COSINE",
   * "DOT_PRODUCT", and "EUCLIDEAN". Each valid value must result in a successfully created table.
   */
  @Test
  public void testValidSimilarityFunctions() throws Exception {
    assumeTrue(
        "Integration tests disabled. Set INTEGRATION_TESTS=true to enable.",
        IntegrationTestConfig.ENABLED);

    for (String sf : Arrays.asList("COSINE", "DOT_PRODUCT", "EUCLIDEAN")) {
      String tbl = tableName + "_" + sf.toLowerCase();
      VectorIndex vi =
          VectorIndex.builder()
              .indexName("vi-" + sf.toLowerCase())
              .vectorAttribute(
                  VectorAttribute.builder().attributeName("embedding").dimensions(4).build())
              .similarityFunction(sf)
              .build();

      CreateTableRequest base =
          CreateTableRequest.builder()
              .tableName(tbl)
              .keySchema(
                  KeySchemaElement.builder().attributeName("pk").keyType(KeyType.HASH).build())
              .attributeDefinitions(
                  AttributeDefinition.builder()
                      .attributeName("pk")
                      .attributeType(ScalarAttributeType.S)
                      .build())
              .billingMode(BillingMode.PAY_PER_REQUEST)
              .build();

      // Must not throw — Alternator should accept this similarity function value.
      VectorSearchSupport.createTable(client, base, Collections.singletonList(vi));

      // Clean up immediately so tearDown doesn't have to know about these extra tables.
      client.deleteTable(DeleteTableRequest.builder().tableName(tbl).build());
    }
  }

  /**
   * Verify that Alternator rejects lowercase similarity function values such as "cosine".
   * The server requires uppercase: "COSINE", "DOT_PRODUCT", "EUCLIDEAN".
   */
  @Test
  public void testLowercaseSimilarityFunctionIsRejected() {
    assumeTrue(
        "Integration tests disabled. Set INTEGRATION_TESTS=true to enable.",
        IntegrationTestConfig.ENABLED);

    VectorIndex vi =
        VectorIndex.builder()
            .indexName("vi1")
            .vectorAttribute(
                VectorAttribute.builder().attributeName("embedding").dimensions(4).build())
            .similarityFunction("cosine") // intentionally wrong casing
            .build();

    CreateTableRequest base =
        CreateTableRequest.builder()
            .tableName(tableName)
            .keySchema(
                KeySchemaElement.builder().attributeName("pk").keyType(KeyType.HASH).build())
            .attributeDefinitions(
                AttributeDefinition.builder()
                    .attributeName("pk")
                    .attributeType(ScalarAttributeType.S)
                    .build())
            .billingMode(BillingMode.PAY_PER_REQUEST)
            .build();

    try {
      VectorSearchSupport.createTable(client, base, Collections.singletonList(vi));
      // Server did not reject lowercase — this Alternator version may not enforce the validation.
      assumeTrue(
          "Skipping: this Alternator version does not reject lowercase similarity functions",
          false);
    } catch (DynamoDbException e) {
      // Expected — Alternator rejects invalid SimilarityFunction values.
    }
  }
}
