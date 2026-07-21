// Copyright 2026-present ScyllaDB
//
// SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

package com.scylladb.alternator.vectorsearch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.core.interceptor.ExecutionAttribute;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.CreateTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.UpdateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateTableResponse;

/**
 * Utility facade for Alternator's vector search extension.
 *
 * <p>Alternator extends the DynamoDB API with vector indexes and vector similarity search. Because
 * the standard AWS SDK for Java does not know about these extensions, this class provides
 * convenience methods that attach the extra parameters to standard SDK requests via {@link
 * VectorSearchInterceptor}.
 *
 * <h2>Setup</h2>
 *
 * <p>Register {@link VectorSearchInterceptor#INSTANCE} when building the client <em>once</em>:
 *
 * <pre>{@code
 * DynamoDbClient client = DynamoDbClient.builder()
 *     .overrideConfiguration(c ->
 *         c.addExecutionInterceptor(VectorSearchInterceptor.INSTANCE))
 *     .endpointOverride(URI.create("http://localhost:8000"))
 *     .credentialsProvider(...)
 *     .build();
 * }</pre>
 *
 * <h2>CreateTable with a vector index</h2>
 *
 * <pre>{@code
 * VectorIndex vi = VectorIndex.builder()
 *     .indexName("embedding-index")
 *     .vectorAttribute(VectorAttribute.builder()
 *         .attributeName("embedding")
 *         .dimensions(128)
 *         .build())
 *     .similarityFunction("COSINE")
 *     .build();
 *
 * CreateTableRequest base = CreateTableRequest.builder()
 *     .tableName("items")
 *     .keySchema(KeySchemaElement.builder().attributeName("id").keyType(KeyType.HASH).build())
 *     .attributeDefinitions(
 *         AttributeDefinition.builder().attributeName("id").attributeType(ScalarAttributeType.S).build())
 *     .billingMode(BillingMode.PAY_PER_REQUEST)
 *     .build();
 *
 * VectorSearchSupport.CreateTableWithVectorIndexes result =
 *     VectorSearchSupport.createTable(client, base, List.of(vi));
 * result.vectorIndexes().forEach(index -> System.out.println(index.indexStatus()));
 * }</pre>
 *
 * <h2>Query with vector similarity search</h2>
 *
 * <pre>{@code
 * VectorSearch vs = VectorSearch.builder()
 *     .queryVector(new float[]{0.1f, 0.2f, 0.3f, ...})
 *     .returnScores(true)
 *     .build();
 *
 * QueryRequest qr = QueryRequest.builder()
 *     .tableName("items")
 *     .indexName("embedding-index")
 *     .limit(10)
 *     .build();
 *
 * VectorQueryResult result = VectorSearchSupport.query(client, qr, vs);
 * result.items().forEach(item -> System.out.println(item));
 * result.scores().forEach(score -> System.out.println("score: " + score));
 * }</pre>
 */
public final class VectorSearchSupport {

  private VectorSearchSupport() {}

  // -------------------------------------------------------------------------
  // Request enrichment helpers (for use with standard client.operation())
  // -------------------------------------------------------------------------

  /**
   * Returns a copy of {@code request} with the given vector indexes attached so that {@link
   * VectorSearchInterceptor} will inject them as the {@code VectorIndexes} field in the {@code
   * CreateTable} JSON body.
   *
   * <p>Any existing {@code overrideConfiguration} on the request is preserved.
   *
   * <p>This is a request-only helper. The standard SDK response cannot represent vector indexes;
   * use {@link #createTable(DynamoDbClient, CreateTableRequest, List)} or {@link
   * #createTableAsync(DynamoDbAsyncClient, CreateTableRequest, List)} when the response metadata is
   * needed.
   *
   * @throws NullPointerException if {@code vectorIndexes} or any of its elements is {@code null}
   */
  public static CreateTableRequest withVectorIndexes(
      CreateTableRequest request, List<VectorIndex> vectorIndexes) {
    List<VectorIndex> snapshot =
        List.copyOf(Objects.requireNonNull(vectorIndexes, "vectorIndexes"));
    return request.toBuilder()
        .overrideConfiguration(
            mergeExecutionAttribute(
                request.overrideConfiguration().orElse(null),
                VectorSearchInterceptor.VECTOR_INDEXES,
                snapshot))
        .build();
  }

  /**
   * Returns a copy of {@code request} with the given vector index updates attached so that {@link
   * VectorSearchInterceptor} will inject them as the {@code VectorIndexUpdates} field in the {@code
   * UpdateTable} JSON body.
   *
   * <p>Alternator accepts exactly one vector index update per {@code UpdateTable} request.
   *
   * <p>Any existing {@code overrideConfiguration} on the request is preserved.
   *
   * @throws IllegalArgumentException if {@code vectorIndexUpdates} does not contain exactly one
   *     update
   * @throws NullPointerException if the sole update is {@code null}
   */
  public static UpdateTableRequest withVectorIndexUpdates(
      UpdateTableRequest request, List<VectorIndexUpdate> vectorIndexUpdates) {
    if (vectorIndexUpdates == null || vectorIndexUpdates.size() != 1) {
      throw new IllegalArgumentException(
          "exactly one vector index update must be provided per UpdateTable request");
    }
    List<VectorIndexUpdate> snapshot = List.copyOf(vectorIndexUpdates);
    return request.toBuilder()
        .overrideConfiguration(
            mergeExecutionAttribute(
                request.overrideConfiguration().orElse(null),
                VectorSearchInterceptor.VECTOR_INDEX_UPDATES,
                snapshot))
        .build();
  }

  /**
   * Returns a copy of {@code request} with the given vector search parameters attached so that
   * {@link VectorSearchInterceptor} will inject the {@code VectorSearch} field in the {@code Query}
   * JSON body and capture the {@code Scores} field in the response.
   *
   * <p>Use this when you need the raw {@link QueryResponse} and will retrieve scores separately via
   * a {@link VectorSearchResultHolder}. Prefer {@link #query(DynamoDbClient, QueryRequest,
   * VectorSearch)} for a more convenient API that bundles the response and scores.
   */
  static QueryRequest withVectorSearch(
      QueryRequest request, VectorSearch vectorSearch, VectorSearchResultHolder resultHolder) {
    AwsRequestOverrideConfiguration base = request.overrideConfiguration().orElse(null);
    AwsRequestOverrideConfiguration config =
        mergeExecutionAttributes(
            base,
            VectorSearchInterceptor.VECTOR_SEARCH,
            vectorSearch,
            VectorSearchInterceptor.RESULT_HOLDER,
            resultHolder);
    return request.toBuilder().overrideConfiguration(config).build();
  }

  // -------------------------------------------------------------------------
  // Convenience methods that bundle request + response
  // -------------------------------------------------------------------------

  /**
   * Executes a vector similarity {@code Query} and returns the items together with any per-item
   * similarity scores.
   *
   * <p>The client must have {@link VectorSearchInterceptor#INSTANCE} registered (see class
   * javadoc).
   *
   * @param client the DynamoDB client
   * @param request the base {@code QueryRequest}; the {@code VectorSearch} parameter will be
   *     injected automatically
   * @param vectorSearch the vector search parameters
   * @return a {@link VectorQueryResult} wrapping the response and scores
   */
  public static VectorQueryResult query(
      DynamoDbClient client, QueryRequest request, VectorSearch vectorSearch) {
    VectorSearchResultHolder holder = new VectorSearchResultHolder();
    QueryRequest enriched = withVectorSearch(request, vectorSearch, holder);
    QueryResponse response = client.query(enriched);
    return new VectorQueryResult(response, holder.getScores());
  }

  /**
   * Asynchronously executes a vector similarity {@code Query} and returns a future that resolves to
   * the items and per-item similarity scores.
   *
   * <p>The client must have {@link VectorSearchInterceptor#INSTANCE} registered (see class
   * javadoc).
   *
   * @param client the async DynamoDB client
   * @param request the base {@code QueryRequest}
   * @param vectorSearch the vector search parameters
   * @return a future resolving to a {@link VectorQueryResult}
   */
  public static CompletableFuture<VectorQueryResult> queryAsync(
      DynamoDbAsyncClient client, QueryRequest request, VectorSearch vectorSearch) {
    VectorSearchResultHolder holder = new VectorSearchResultHolder();
    QueryRequest enriched = withVectorSearch(request, vectorSearch, holder);
    CompletableFuture<QueryResponse> source = client.query(enriched);
    CompletableFuture<VectorQueryResult> result =
        source.thenApply(resp -> new VectorQueryResult(resp, holder.getScores()));
    forwardCancellation(result, source);
    return result;
  }

  /**
   * Executes a {@code CreateTable} request with the given vector indexes and returns the standard
   * response alongside the vector indexes reported by the server.
   *
   * <p>The {@link VectorSearchInterceptor} must be registered on the client.
   *
   * @param client the DynamoDB client
   * @param request the base {@code CreateTableRequest}
   * @param vectorIndexes the vector indexes to create together with the table
   * @return the standard response and the vector indexes returned in its table description
   */
  public static CreateTableWithVectorIndexes createTable(
      DynamoDbClient client, CreateTableRequest request, List<VectorIndex> vectorIndexes) {
    VectorSearchResultHolder holder = new VectorSearchResultHolder();
    CreateTableRequest enriched =
        withResultHolder(withVectorIndexes(request, vectorIndexes), holder);
    CreateTableResponse response = client.createTable(enriched);
    List<VectorIndex> indexes = holder.getVectorIndexes();
    return new CreateTableWithVectorIndexes(
        response, indexes != null ? indexes : Collections.emptyList());
  }

  /**
   * Asynchronously executes a {@code CreateTable} request with the given vector indexes and returns
   * the standard response alongside the vector indexes reported by the server.
   *
   * <p>The {@link VectorSearchInterceptor} must be registered on the client.
   *
   * @param client the async DynamoDB client
   * @param request the base {@code CreateTableRequest}
   * @param vectorIndexes the vector indexes to create together with the table
   * @return a future resolving to the standard response and the vector indexes returned in its
   *     table description
   */
  public static CompletableFuture<CreateTableWithVectorIndexes> createTableAsync(
      DynamoDbAsyncClient client, CreateTableRequest request, List<VectorIndex> vectorIndexes) {
    VectorSearchResultHolder holder = new VectorSearchResultHolder();
    CreateTableRequest enriched =
        withResultHolder(withVectorIndexes(request, vectorIndexes), holder);
    CompletableFuture<CreateTableResponse> source = client.createTable(enriched);
    CompletableFuture<CreateTableWithVectorIndexes> result =
        source.thenApply(
            response -> {
              List<VectorIndex> indexes = holder.getVectorIndexes();
              return new CreateTableWithVectorIndexes(
                  response, indexes != null ? indexes : Collections.emptyList());
            });
    forwardCancellation(result, source);
    return result;
  }

  /**
   * Executes an {@code UpdateTable} request that adds or removes one vector index.
   *
   * @param client the DynamoDB client
   * @param request the base {@code UpdateTableRequest}
   * @param vectorIndexUpdates exactly one vector index change to apply
   * @return the {@code UpdateTableResponse}
   */
  public static UpdateTableResponse updateTable(
      DynamoDbClient client,
      UpdateTableRequest request,
      List<VectorIndexUpdate> vectorIndexUpdates) {
    return client.updateTable(withVectorIndexUpdates(request, vectorIndexUpdates));
  }

  /**
   * Executes a {@code DescribeTable} request and returns the standard response alongside any vector
   * indexes defined on the table.
   *
   * <p>The {@link VectorSearchInterceptor} must be registered on the client.
   *
   * @param client the DynamoDB client
   * @param request the {@code DescribeTableRequest}
   * @return a pair of the standard response and the list of vector indexes (may be empty)
   */
  public static DescribeTableWithVectorIndexes describeTable(
      DynamoDbClient client, DescribeTableRequest request) {
    VectorSearchResultHolder holder = new VectorSearchResultHolder();
    DescribeTableRequest enriched = withResultHolder(request, holder);
    DescribeTableResponse response = client.describeTable(enriched);
    List<VectorIndex> indexes = holder.getVectorIndexes();
    return new DescribeTableWithVectorIndexes(
        response, indexes != null ? indexes : Collections.emptyList());
  }

  /**
   * Asynchronously executes a {@code DescribeTable} request and returns the standard response
   * alongside any vector indexes defined on the table.
   *
   * <p>The {@link VectorSearchInterceptor} must be registered on the client.
   *
   * @param client the async DynamoDB client
   * @param request the {@code DescribeTableRequest}
   * @return a future resolving to the standard response and the list of vector indexes (which may
   *     be empty)
   */
  public static CompletableFuture<DescribeTableWithVectorIndexes> describeTableAsync(
      DynamoDbAsyncClient client, DescribeTableRequest request) {
    VectorSearchResultHolder holder = new VectorSearchResultHolder();
    DescribeTableRequest enriched = withResultHolder(request, holder);
    CompletableFuture<DescribeTableResponse> source = client.describeTable(enriched);
    CompletableFuture<DescribeTableWithVectorIndexes> result =
        source.thenApply(
            response -> {
              List<VectorIndex> indexes = holder.getVectorIndexes();
              return new DescribeTableWithVectorIndexes(
                  response, indexes != null ? indexes : Collections.emptyList());
            });
    forwardCancellation(result, source);
    return result;
  }

  // -------------------------------------------------------------------------
  // Private helpers for building overrideConfiguration
  // -------------------------------------------------------------------------

  private static void forwardCancellation(
      CompletableFuture<?> result, CompletableFuture<?> source) {
    result.whenComplete(
        (ignored, failure) -> {
          if (result.isCancelled()) {
            source.cancel(true);
          }
        });
  }

  private static CreateTableRequest withResultHolder(
      CreateTableRequest request, VectorSearchResultHolder holder) {
    return request.toBuilder()
        .overrideConfiguration(
            mergeExecutionAttribute(
                request.overrideConfiguration().orElse(null),
                VectorSearchInterceptor.RESULT_HOLDER,
                holder))
        .build();
  }

  private static DescribeTableRequest withResultHolder(
      DescribeTableRequest request, VectorSearchResultHolder holder) {
    return request.toBuilder()
        .overrideConfiguration(
            mergeExecutionAttribute(
                request.overrideConfiguration().orElse(null),
                VectorSearchInterceptor.RESULT_HOLDER,
                holder))
        .build();
  }

  private static <T> AwsRequestOverrideConfiguration mergeExecutionAttribute(
      AwsRequestOverrideConfiguration existing, ExecutionAttribute<T> key, T value) {
    AwsRequestOverrideConfiguration.Builder builder =
        existing != null ? existing.toBuilder() : AwsRequestOverrideConfiguration.builder();
    builder.putExecutionAttribute(key, value);
    return builder.build();
  }

  @SuppressWarnings("unchecked")
  private static <A, B> AwsRequestOverrideConfiguration mergeExecutionAttributes(
      AwsRequestOverrideConfiguration existing,
      ExecutionAttribute<A> keyA,
      A valueA,
      ExecutionAttribute<B> keyB,
      B valueB) {
    AwsRequestOverrideConfiguration.Builder builder =
        existing != null ? existing.toBuilder() : AwsRequestOverrideConfiguration.builder();
    builder.putExecutionAttribute(keyA, valueA);
    builder.putExecutionAttribute(keyB, valueB);
    return builder.build();
  }

  // -------------------------------------------------------------------------
  // Result wrappers for table operations
  // -------------------------------------------------------------------------

  /**
   * Holds the result of {@link #createTable(DynamoDbClient, CreateTableRequest, List)} or {@link
   * #createTableAsync(DynamoDbAsyncClient, CreateTableRequest, List)}: the standard SDK response
   * together with the vector indexes parsed from its table description.
   */
  public static final class CreateTableWithVectorIndexes {
    private final CreateTableResponse response;
    private final List<VectorIndex> vectorIndexes;

    CreateTableWithVectorIndexes(CreateTableResponse response, List<VectorIndex> vectorIndexes) {
      this.response = response;
      this.vectorIndexes = Collections.unmodifiableList(new ArrayList<>(vectorIndexes));
    }

    /** Returns the standard {@link CreateTableResponse}. */
    public CreateTableResponse response() {
      return response;
    }

    /**
     * Returns the vector indexes reported in the created table description, or an empty list if
     * none were returned or the interceptor was not registered.
     */
    public List<VectorIndex> vectorIndexes() {
      return vectorIndexes;
    }
  }

  /**
   * Holds the result of {@link #describeTable(DynamoDbClient, DescribeTableRequest)} or {@link
   * #describeTableAsync(DynamoDbAsyncClient, DescribeTableRequest)}: the standard SDK response
   * together with the vector indexes parsed from the raw JSON response.
   */
  public static final class DescribeTableWithVectorIndexes {
    private final DescribeTableResponse response;
    private final List<VectorIndex> vectorIndexes;

    DescribeTableWithVectorIndexes(
        DescribeTableResponse response, List<VectorIndex> vectorIndexes) {
      this.response = response;
      this.vectorIndexes = Collections.unmodifiableList(new ArrayList<>(vectorIndexes));
    }

    /** Returns the standard {@link DescribeTableResponse}. */
    public DescribeTableResponse response() {
      return response;
    }

    /**
     * Returns the vector indexes defined on the table, or an empty list if none are defined or the
     * interceptor was not registered.
     */
    public List<VectorIndex> vectorIndexes() {
      return vectorIndexes;
    }
  }
}
