// Copyright 2026-present ScyllaDB
//
// SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

package com.scylladb.alternator.vectorsearch;

import java.util.Collections;
import java.util.List;
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
 * CreateTableResponse resp = client.createTable(
 *     VectorSearchSupport.withVectorIndexes(base, List.of(vi)));
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
   */
  public static CreateTableRequest withVectorIndexes(
      CreateTableRequest request, List<VectorIndex> vectorIndexes) {
    return request.toBuilder()
        .overrideConfiguration(
            mergeExecutionAttribute(
                request.overrideConfiguration().orElse(null),
                VectorSearchInterceptor.VECTOR_INDEXES,
                vectorIndexes))
        .build();
  }

  /**
   * Returns a copy of {@code request} with the given vector index updates attached so that {@link
   * VectorSearchInterceptor} will inject them as the {@code VectorIndexUpdates} field in the {@code
   * UpdateTable} JSON body.
   *
   * <p>Any existing {@code overrideConfiguration} on the request is preserved.
   */
  public static UpdateTableRequest withVectorIndexUpdates(
      UpdateTableRequest request, List<VectorIndexUpdate> vectorIndexUpdates) {
    return request.toBuilder()
        .overrideConfiguration(
            mergeExecutionAttribute(
                request.overrideConfiguration().orElse(null),
                VectorSearchInterceptor.VECTOR_INDEX_UPDATES,
                vectorIndexUpdates))
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
    return client
        .query(enriched)
        .thenApply(resp -> new VectorQueryResult(resp, holder.getScores()));
  }

  /**
   * Executes a {@code CreateTable} request with the given vector indexes and returns the response.
   *
   * <p>The returned vector indexes (as reported by the server, with status fields populated) are
   * available in the table description via {@link DescribeTableResponse} or via a subsequent {@link
   * #describeTable(DynamoDbClient, DescribeTableRequest)}.
   *
   * @param client the DynamoDB client
   * @param request the base {@code CreateTableRequest}
   * @param vectorIndexes the vector indexes to create together with the table
   * @return the {@code CreateTableResponse}
   */
  public static CreateTableResponse createTable(
      DynamoDbClient client, CreateTableRequest request, List<VectorIndex> vectorIndexes) {
    return client.createTable(withVectorIndexes(request, vectorIndexes));
  }

  /**
   * Executes an {@code UpdateTable} request that adds or removes vector indexes.
   *
   * @param client the DynamoDB client
   * @param request the base {@code UpdateTableRequest}
   * @param vectorIndexUpdates the vector index changes to apply
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
    DescribeTableRequest enriched =
        request.toBuilder()
            .overrideConfiguration(
                mergeExecutionAttribute(
                    request.overrideConfiguration().orElse(null),
                    VectorSearchInterceptor.RESULT_HOLDER,
                    holder))
            .build();
    DescribeTableResponse response = client.describeTable(enriched);
    List<VectorIndex> indexes = holder.getVectorIndexes();
    return new DescribeTableWithVectorIndexes(
        response, indexes != null ? indexes : Collections.emptyList());
  }

  // -------------------------------------------------------------------------
  // Private helpers for building overrideConfiguration
  // -------------------------------------------------------------------------

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
  // Inner result class for DescribeTable
  // -------------------------------------------------------------------------

  /**
   * Holds the result of {@link #describeTable(DynamoDbClient, DescribeTableRequest)}: the standard
   * SDK response together with the vector indexes parsed from the raw JSON response.
   */
  public static final class DescribeTableWithVectorIndexes {
    private final DescribeTableResponse response;
    private final List<VectorIndex> vectorIndexes;

    DescribeTableWithVectorIndexes(
        DescribeTableResponse response, List<VectorIndex> vectorIndexes) {
      this.response = response;
      this.vectorIndexes = Collections.unmodifiableList(vectorIndexes);
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
