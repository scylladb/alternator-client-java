// Copyright 2026-present ScyllaDB
//
// SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

package com.scylladb.alternator.vectorsearch;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.CreateTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.UpdateTableRequest;

public class VectorSearchSupportTest {

  @Test
  public void testCreateTableAttachesResultHolderAndReturnsVectorIndexes() {
    DynamoDbClient client = mock(DynamoDbClient.class);
    CreateTableResponse response = CreateTableResponse.builder().build();
    VectorIndex index = vectorIndex("embedding-index");

    when(client.createTable(any(CreateTableRequest.class)))
        .thenAnswer(
            invocation -> {
              CreateTableRequest enriched = invocation.getArgument(0);
              assertEquals("items", enriched.tableName());
              assertNotNull(enriched.overrideConfiguration().orElse(null));
              assertEquals(
                  Collections.singletonList(index),
                  enriched
                      .overrideConfiguration()
                      .get()
                      .executionAttributes()
                      .getAttribute(VectorSearchInterceptor.VECTOR_INDEXES));
              VectorSearchResultHolder holder =
                  enriched
                      .overrideConfiguration()
                      .get()
                      .executionAttributes()
                      .getAttribute(VectorSearchInterceptor.RESULT_HOLDER);
              assertNotNull(holder);
              holder.setVectorIndexes(Collections.singletonList(index));
              return response;
            });

    VectorSearchSupport.CreateTableWithVectorIndexes result =
        VectorSearchSupport.createTable(
            client,
            CreateTableRequest.builder().tableName("items").build(),
            Collections.singletonList(index));

    assertSame(response, result.response());
    assertEquals(1, result.vectorIndexes().size());
    assertSame(index, result.vectorIndexes().get(0));
  }

  @Test
  public void testCreateTableResultSnapshotsVectorIndexes() {
    VectorIndex original = vectorIndex("original-index");
    List<VectorIndex> indexes = new ArrayList<>();
    indexes.add(original);
    VectorSearchSupport.CreateTableWithVectorIndexes result =
        new VectorSearchSupport.CreateTableWithVectorIndexes(
            CreateTableResponse.builder().build(), indexes);

    indexes.clear();
    indexes.add(vectorIndex("replacement-index"));

    assertEquals(1, result.vectorIndexes().size());
    assertSame(original, result.vectorIndexes().get(0));
  }

  @Test
  public void testDescribeTableAsyncAttachesResultHolderAndReturnsVectorIndexes() throws Exception {
    DynamoDbAsyncClient client = mock(DynamoDbAsyncClient.class);
    DescribeTableResponse response = DescribeTableResponse.builder().build();
    VectorIndex index =
        VectorIndex.builder()
            .indexName("embedding-index")
            .vectorAttribute(
                VectorAttribute.builder().attributeName("embedding").dimensions(2).build())
            .indexStatus("ACTIVE")
            .build();

    when(client.describeTable(any(DescribeTableRequest.class)))
        .thenAnswer(
            invocation -> {
              DescribeTableRequest enriched = invocation.getArgument(0);
              assertEquals("items", enriched.tableName());
              assertNotNull(enriched.overrideConfiguration().orElse(null));
              VectorSearchResultHolder holder =
                  enriched
                      .overrideConfiguration()
                      .get()
                      .executionAttributes()
                      .getAttribute(VectorSearchInterceptor.RESULT_HOLDER);
              assertNotNull(holder);
              holder.setVectorIndexes(Collections.singletonList(index));
              return CompletableFuture.completedFuture(response);
            });

    VectorSearchSupport.DescribeTableWithVectorIndexes result =
        VectorSearchSupport.describeTableAsync(
                client, DescribeTableRequest.builder().tableName("items").build())
            .get(5, TimeUnit.SECONDS);

    assertSame(response, result.response());
    assertEquals(1, result.vectorIndexes().size());
    assertSame(index, result.vectorIndexes().get(0));
  }

  @Test
  public void testQueryAsyncCancellationPropagatesToSdkFuture() {
    DynamoDbAsyncClient client = mock(DynamoDbAsyncClient.class);
    CompletableFuture<QueryResponse> sdkFuture = new CompletableFuture<>();
    when(client.query(any(QueryRequest.class))).thenReturn(sdkFuture);

    CompletableFuture<VectorQueryResult> result =
        VectorSearchSupport.queryAsync(
            client,
            QueryRequest.builder().tableName("items").indexName("embedding-index").limit(1).build(),
            VectorSearch.builder().queryVector(1.0f, 2.0f).build());

    assertTrue(result.cancel(true));
    assertTrue(sdkFuture.isCancelled());
  }

  @Test
  public void testCreateTableAsyncCancellationPropagatesToSdkFuture() {
    DynamoDbAsyncClient client = mock(DynamoDbAsyncClient.class);
    CompletableFuture<CreateTableResponse> sdkFuture = new CompletableFuture<>();
    when(client.createTable(any(CreateTableRequest.class))).thenReturn(sdkFuture);

    CompletableFuture<VectorSearchSupport.CreateTableWithVectorIndexes> result =
        VectorSearchSupport.createTableAsync(
            client,
            CreateTableRequest.builder().tableName("items").build(),
            Collections.singletonList(vectorIndex("embedding-index")));

    assertTrue(result.cancel(true));
    assertTrue(sdkFuture.isCancelled());
  }

  @Test
  public void testDescribeTableAsyncCancellationPropagatesToSdkFuture() {
    DynamoDbAsyncClient client = mock(DynamoDbAsyncClient.class);
    CompletableFuture<DescribeTableResponse> sdkFuture = new CompletableFuture<>();
    when(client.describeTable(any(DescribeTableRequest.class))).thenReturn(sdkFuture);

    CompletableFuture<VectorSearchSupport.DescribeTableWithVectorIndexes> result =
        VectorSearchSupport.describeTableAsync(
            client, DescribeTableRequest.builder().tableName("items").build());

    assertTrue(result.cancel(true));
    assertTrue(sdkFuture.isCancelled());
  }

  @Test
  public void testDescribeTableResultSnapshotsVectorIndexes() {
    VectorIndex original = vectorIndex("original-index");
    List<VectorIndex> indexes = new ArrayList<>();
    indexes.add(original);
    VectorSearchSupport.DescribeTableWithVectorIndexes result =
        new VectorSearchSupport.DescribeTableWithVectorIndexes(
            DescribeTableResponse.builder().build(), indexes);

    indexes.clear();
    indexes.add(vectorIndex("replacement-index"));

    assertEquals(1, result.vectorIndexes().size());
    assertSame(original, result.vectorIndexes().get(0));
  }

  @Test
  public void testWithVectorIndexesSnapshotsMutableInput() {
    VectorIndex original = vectorIndex("original-index");
    List<VectorIndex> indexes = new ArrayList<>();
    indexes.add(original);

    CreateTableRequest enriched =
        VectorSearchSupport.withVectorIndexes(
            CreateTableRequest.builder().tableName("items").build(), indexes);
    indexes.clear();
    indexes.add(vectorIndex("replacement-index"));

    List<VectorIndex> attached =
        enriched
            .overrideConfiguration()
            .get()
            .executionAttributes()
            .getAttribute(VectorSearchInterceptor.VECTOR_INDEXES);
    assertEquals(1, attached.size());
    assertSame(original, attached.get(0));
  }

  @Test(expected = NullPointerException.class)
  public void testWithVectorIndexesRejectsNullList() {
    VectorSearchSupport.withVectorIndexes(
        CreateTableRequest.builder().tableName("items").build(), null);
  }

  @Test(expected = NullPointerException.class)
  public void testWithVectorIndexesRejectsNullElement() {
    VectorSearchSupport.withVectorIndexes(
        CreateTableRequest.builder().tableName("items").build(),
        Collections.<VectorIndex>singletonList(null));
  }

  @Test
  public void testWithVectorIndexUpdatesSnapshotsAfterValidation() {
    VectorIndexUpdate original = deleteUpdate("original-index");
    List<VectorIndexUpdate> updates = new ArrayList<>();
    updates.add(original);

    UpdateTableRequest enriched =
        VectorSearchSupport.withVectorIndexUpdates(
            UpdateTableRequest.builder().tableName("items").build(), updates);
    updates.clear();
    updates.add(deleteUpdate("replacement-index-1"));
    updates.add(deleteUpdate("replacement-index-2"));

    List<VectorIndexUpdate> attached =
        enriched
            .overrideConfiguration()
            .get()
            .executionAttributes()
            .getAttribute(VectorSearchInterceptor.VECTOR_INDEX_UPDATES);
    assertEquals(1, attached.size());
    assertSame(original, attached.get(0));
  }

  @Test(expected = NullPointerException.class)
  public void testWithVectorIndexUpdatesRejectsNullElement() {
    VectorSearchSupport.withVectorIndexUpdates(
        UpdateTableRequest.builder().tableName("items").build(),
        Collections.<VectorIndexUpdate>singletonList(null));
  }

  @Test
  public void testVectorSearchSnapshotsAndDoesNotExposeFloatArray() {
    float[] input = {1.0f, 2.0f};
    VectorSearch vectorSearch = VectorSearch.builder().queryVector(input).build();

    input[0] = 99.0f;
    assertArrayEquals(new float[] {1.0f, 2.0f}, vectorSearch.queryVectorFloats(), 0.0f);

    float[] returned = vectorSearch.queryVectorFloats();
    returned[1] = 99.0f;
    assertArrayEquals(new float[] {1.0f, 2.0f}, vectorSearch.queryVectorFloats(), 0.0f);
  }

  private static VectorIndex vectorIndex(String name) {
    return VectorIndex.builder()
        .indexName(name)
        .vectorAttribute(VectorAttribute.builder().attributeName("embedding").dimensions(2).build())
        .build();
  }

  private static VectorIndexUpdate deleteUpdate(String name) {
    return VectorIndexUpdate.builder()
        .delete(DeleteVectorIndexAction.builder().indexName(name).build())
        .build();
  }
}
