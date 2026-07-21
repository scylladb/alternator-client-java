// Copyright 2026-present ScyllaDB
//
// SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

package com.scylladb.alternator.vectorsearch;

import static org.junit.Assert.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.scylladb.alternator.GzipRequestInterceptor;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32;
import java.util.zip.CRC32C;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.checksums.DefaultChecksumAlgorithm;
import software.amazon.awssdk.checksums.SdkChecksum;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.exception.Crc32MismatchException;
import software.amazon.awssdk.core.exception.RetryableException;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptorChain;
import software.amazon.awssdk.core.interceptor.InterceptorContext;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.http.ContentStreamProvider;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpFullResponse;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.http.async.AsyncExecuteRequest;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ListTablesRequest;
import software.amazon.awssdk.services.dynamodb.model.ListTablesResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

public class VectorSearchHttpInterceptorTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  public void testAsyncHttpResponseContentExtractsScoresAndConvertsFloat32Vector()
      throws Exception {
    byte[] responseBody =
        bytes(
            "{\"Items\":[{\"embedding\":{\"FLOAT32VECTOR\":[1.0,2.5]}}],"
                + "\"Scores\":[0.7,0.6]}");
    VectorSearchResultHolder holder = new VectorSearchResultHolder();
    ExecutionAttributes attrs = new ExecutionAttributes();
    attrs.putAttribute(VectorSearchInterceptor.RESULT_HOLDER, holder);

    Context.ModifyHttpResponse context =
        responseContext(queryHttpRequest(), singleChunkPublisher(responseBody));

    Optional<Publisher<ByteBuffer>> modified =
        VectorSearchInterceptor.INSTANCE.modifyAsyncHttpResponseContent(context, attrs);

    assertTrue(modified.isPresent());
    byte[] out = collect(modified.get()).get(5, TimeUnit.SECONDS);
    JsonNode json = MAPPER.readTree(out);

    JsonNode embedding = json.get("Items").get(0).get("embedding");
    assertFloat32Vector(embedding, 1.0f, 2.5f);
    assertEquals(Arrays.asList(0.7, 0.6), holder.getScores());
  }

  @Test
  public void testGzipHttpResponseContentExtractsScoresAndConvertsFloat32Vector() throws Exception {
    byte[] responseBody =
        bytes(
            "{\"Items\":[{\"embedding\":{\"FLOAT32VECTOR\":[1.0,2.5]}}],"
                + "\"Scores\":[0.7,0.6]}");
    byte[] compressedBody = gzipCompress(responseBody);
    VectorSearchResultHolder holder = new VectorSearchResultHolder();
    ExecutionAttributes attrs = new ExecutionAttributes();
    attrs.putAttribute(VectorSearchInterceptor.RESULT_HOLDER, holder);
    SdkHttpResponse compressedResponse =
        SdkHttpResponse.builder()
            .statusCode(200)
            .putHeader("Content-Encoding", "gzip")
            .putHeader("Content-Length", String.valueOf(compressedBody.length))
            .build();

    Context.ModifyHttpResponse headerContext =
        responseContext(queryHttpRequest(), compressedResponse, compressedBody);
    SdkHttpResponse modifiedResponse =
        VectorSearchInterceptor.INSTANCE.modifyHttpResponse(headerContext, attrs);
    Context.ModifyHttpResponse bodyContext =
        responseContext(queryHttpRequest(), modifiedResponse, compressedBody);

    assertFalse(modifiedResponse.firstMatchingHeader("Content-Encoding").isPresent());
    assertFalse(modifiedResponse.firstMatchingHeader("Content-Length").isPresent());

    Optional<java.io.InputStream> modified =
        VectorSearchInterceptor.INSTANCE.modifyHttpResponseContent(bodyContext, attrs);

    assertTrue(modified.isPresent());
    JsonNode json = MAPPER.readTree(readAllBytes(modified.get()));

    JsonNode embedding = json.get("Items").get(0).get("embedding");
    assertFloat32Vector(embedding, 1.0f, 2.5f);
    assertEquals(Arrays.asList(0.7, 0.6), holder.getScores());
  }

  @Test
  public void testUncompressedHttpResponseStripsChecksumAndLengthHeadersBeforeVectorConversion()
      throws Exception {
    byte[] responseBody = bytes("{\"Item\":{\"embedding\":{\"FLOAT32VECTOR\":[3.0,4.5]}}}");
    ExecutionAttributes attrs = new ExecutionAttributes();
    SdkHttpResponse response =
        SdkHttpResponse.builder()
            .statusCode(200)
            .putHeader("Content-Length", String.valueOf(responseBody.length))
            .putHeader("x-amz-crc32", crc32(responseBody))
            .putHeader("X-Amz-Crc32c", crc32c(responseBody))
            .putHeader("x-amz-checksum-sha256", sha256(responseBody))
            .putHeader("X-Test", "kept")
            .build();

    Context.ModifyHttpResponse headerContext =
        responseContext(queryHttpRequest(), response, responseBody);
    SdkHttpResponse modifiedResponse =
        VectorSearchInterceptor.INSTANCE.modifyHttpResponse(headerContext, attrs);

    assertFalse(modifiedResponse.firstMatchingHeader("Content-Length").isPresent());
    assertFalse(modifiedResponse.firstMatchingHeader("x-amz-crc32").isPresent());
    assertFalse(modifiedResponse.firstMatchingHeader("x-amz-crc32c").isPresent());
    assertFalse(modifiedResponse.firstMatchingHeader("x-amz-checksum-sha256").isPresent());
    assertEquals("kept", modifiedResponse.firstMatchingHeader("X-Test").get());

    Context.ModifyHttpResponse bodyContext =
        responseContext(queryHttpRequest(), modifiedResponse, responseBody);
    Optional<java.io.InputStream> modified =
        VectorSearchInterceptor.INSTANCE.modifyHttpResponseContent(bodyContext, attrs);

    assertTrue(modified.isPresent());
    JsonNode json = MAPPER.readTree(readAllBytes(modified.get()));
    JsonNode embedding = json.get("Item").get("embedding");
    assertFloat32Vector(embedding, 3.0f, 4.5f);
  }

  @Test
  public void testUnchangedUncompressedHttpResponsePreservesChecksumAndLengthHeaders()
      throws Exception {
    byte[] responseBody = bytes("{\"TableNames\":[\"items\"]}");
    ExecutionAttributes attrs = new ExecutionAttributes();
    SdkHttpResponse response =
        SdkHttpResponse.builder()
            .statusCode(200)
            .putHeader("Content-Length", String.valueOf(responseBody.length))
            .putHeader("x-amz-crc32", crc32(responseBody))
            .putHeader("X-Amz-Crc32c", crc32c(responseBody))
            .putHeader("x-amz-checksum-sha256", sha256(responseBody))
            .putHeader("X-Test", "kept")
            .build();

    Context.ModifyHttpResponse headerContext =
        responseContext(listTablesHttpRequest(), response, responseBody);
    SdkHttpResponse modifiedResponse =
        VectorSearchInterceptor.INSTANCE.modifyHttpResponse(headerContext, attrs);

    assertEquals(
        String.valueOf(responseBody.length),
        modifiedResponse.firstMatchingHeader("Content-Length").get());
    assertEquals(crc32(responseBody), modifiedResponse.firstMatchingHeader("x-amz-crc32").get());
    assertEquals(crc32c(responseBody), modifiedResponse.firstMatchingHeader("x-amz-crc32c").get());
    assertEquals(
        sha256(responseBody), modifiedResponse.firstMatchingHeader("x-amz-checksum-sha256").get());
    assertEquals("kept", modifiedResponse.firstMatchingHeader("X-Test").get());

    Optional<java.io.InputStream> modified =
        VectorSearchInterceptor.INSTANCE.modifyHttpResponseContent(headerContext, attrs);

    assertTrue(modified.isPresent());
    assertArrayEquals(responseBody, readAllBytes(modified.get()));
  }

  @Test
  public void testUnchangedUncompressedAsyncHttpResponsePreservesChecksumAndLengthHeaders()
      throws Exception {
    byte[] responseBody = bytes("{\"TableNames\":[\"items\"]}");
    ExecutionAttributes attrs = new ExecutionAttributes();
    SdkHttpResponse response =
        SdkHttpResponse.builder()
            .statusCode(200)
            .putHeader("Content-Length", String.valueOf(responseBody.length))
            .putHeader("x-amz-crc32", crc32(responseBody))
            .putHeader("X-Amz-Crc32c", crc32c(responseBody))
            .putHeader("x-amz-checksum-sha256", sha256(responseBody))
            .putHeader("X-Test", "kept")
            .build();

    Context.ModifyHttpResponse headerContext =
        responseContext(listTablesHttpRequest(), response, singleChunkPublisher(responseBody));
    SdkHttpResponse modifiedResponse =
        VectorSearchInterceptor.INSTANCE.modifyHttpResponse(headerContext, attrs);

    assertEquals(
        String.valueOf(responseBody.length),
        modifiedResponse.firstMatchingHeader("Content-Length").get());
    assertEquals(crc32(responseBody), modifiedResponse.firstMatchingHeader("x-amz-crc32").get());
    assertEquals(crc32c(responseBody), modifiedResponse.firstMatchingHeader("x-amz-crc32c").get());
    assertEquals(
        sha256(responseBody), modifiedResponse.firstMatchingHeader("x-amz-checksum-sha256").get());
    assertEquals("kept", modifiedResponse.firstMatchingHeader("X-Test").get());

    Optional<Publisher<ByteBuffer>> modified =
        VectorSearchInterceptor.INSTANCE.modifyAsyncHttpResponseContent(headerContext, attrs);

    assertTrue(modified.isPresent());
    assertArrayEquals(responseBody, collect(modified.get()).get(5, TimeUnit.SECONDS));
  }

  @Test
  public void testUncompressedAsyncGetItemFloat32VectorResponseStripsChecksumAndLengthHeaders()
      throws Exception {
    byte[] responseBody = bytes("{\"Item\":{\"embedding\":{\"FLOAT32VECTOR\":[3.0,4.5]}}}");
    ExecutionAttributes attrs = new ExecutionAttributes();
    SdkHttpResponse response =
        SdkHttpResponse.builder()
            .statusCode(200)
            .putHeader("Content-Length", String.valueOf(responseBody.length))
            .putHeader("x-amz-crc32", crc32(responseBody))
            .putHeader("X-Amz-Crc32c", crc32c(responseBody))
            .putHeader("x-amz-checksum-sha256", sha256(responseBody))
            .putHeader("X-Test", "kept")
            .build();

    Context.ModifyHttpResponse headerContext = responseContext(getItemHttpRequest(), response);
    SdkHttpResponse modifiedResponse =
        VectorSearchInterceptor.INSTANCE.modifyHttpResponse(headerContext, attrs);

    assertFalse(modifiedResponse.firstMatchingHeader("Content-Length").isPresent());
    assertFalse(modifiedResponse.firstMatchingHeader("x-amz-crc32").isPresent());
    assertFalse(modifiedResponse.firstMatchingHeader("x-amz-crc32c").isPresent());
    assertFalse(modifiedResponse.firstMatchingHeader("x-amz-checksum-sha256").isPresent());
    assertEquals("kept", modifiedResponse.firstMatchingHeader("X-Test").get());

    Context.ModifyHttpResponse bodyContext =
        responseContext(getItemHttpRequest(), modifiedResponse, singleChunkPublisher(responseBody));
    Optional<Publisher<ByteBuffer>> modified =
        VectorSearchInterceptor.INSTANCE.modifyAsyncHttpResponseContent(bodyContext, attrs);

    assertTrue(modified.isPresent());
    JsonNode json = MAPPER.readTree(collect(modified.get()).get(5, TimeUnit.SECONDS));
    JsonNode embedding = json.get("Item").get("embedding");
    assertFloat32Vector(embedding, 3.0f, 4.5f);
  }

  @Test
  public void testUncompressedAsyncGetItemWithoutVectorValidatesChecksumBeforeDroppingHeader()
      throws Exception {
    byte[] responseBody = bytes("{\"Item\":{\"id\":{\"S\":\"item-1\"}}}");
    ExecutionAttributes attrs = new ExecutionAttributes();
    SdkHttpResponse response =
        SdkHttpResponse.builder()
            .statusCode(200)
            .putHeader("Content-Length", String.valueOf(responseBody.length))
            .putHeader("x-amz-crc32", "0")
            .build();

    Context.ModifyHttpResponse headerContext = responseContext(getItemHttpRequest(), response);
    SdkHttpResponse modifiedResponse =
        VectorSearchInterceptor.INSTANCE.modifyHttpResponse(headerContext, attrs);

    assertFalse(modifiedResponse.firstMatchingHeader("Content-Length").isPresent());
    assertFalse(modifiedResponse.firstMatchingHeader("x-amz-crc32").isPresent());

    Context.ModifyHttpResponse bodyContext =
        responseContext(getItemHttpRequest(), modifiedResponse, singleChunkPublisher(responseBody));
    Publisher<ByteBuffer> modified =
        VectorSearchInterceptor.INSTANCE.modifyAsyncHttpResponseContent(bodyContext, attrs).get();

    try {
      collect(modified).get(5, TimeUnit.SECONDS);
      fail("Expected the corrupt raw response checksum to be rejected");
    } catch (ExecutionException e) {
      assertTrue(e.getCause() instanceof Crc32MismatchException);
      assertTrue(((Crc32MismatchException) e.getCause()).retryable());
      assertTrue(e.getCause().getMessage().contains("different checksum"));
    }
  }

  @Test
  public void
      testUncompressedAsyncBatchWriteItemUnprocessedVectorResponseStripsChecksumAndLengthHeaders()
          throws Exception {
    byte[] responseBody =
        bytes(
            "{\"UnprocessedItems\":{\"items\":[{\"PutRequest\":{\"Item\":"
                + "{\"embedding\":{\"FLOAT32VECTOR\":[3.0,4.5]}}}}]}}}");
    ExecutionAttributes attrs = new ExecutionAttributes();
    SdkHttpResponse response =
        SdkHttpResponse.builder()
            .statusCode(200)
            .putHeader("Content-Length", String.valueOf(responseBody.length))
            .putHeader("x-amz-crc32", crc32(responseBody))
            .putHeader("X-Amz-Crc32c", crc32c(responseBody))
            .putHeader("x-amz-checksum-sha256", sha256(responseBody))
            .putHeader("X-Test", "kept")
            .build();

    Context.ModifyHttpResponse headerContext =
        responseContext(batchWriteItemHttpRequest(), response);
    SdkHttpResponse modifiedResponse =
        VectorSearchInterceptor.INSTANCE.modifyHttpResponse(headerContext, attrs);

    assertFalse(modifiedResponse.firstMatchingHeader("Content-Length").isPresent());
    assertFalse(modifiedResponse.firstMatchingHeader("x-amz-crc32").isPresent());
    assertFalse(modifiedResponse.firstMatchingHeader("x-amz-crc32c").isPresent());
    assertFalse(modifiedResponse.firstMatchingHeader("x-amz-checksum-sha256").isPresent());
    assertEquals("kept", modifiedResponse.firstMatchingHeader("X-Test").get());

    Context.ModifyHttpResponse bodyContext =
        responseContext(
            batchWriteItemHttpRequest(), modifiedResponse, singleChunkPublisher(responseBody));
    Optional<Publisher<ByteBuffer>> modified =
        VectorSearchInterceptor.INSTANCE.modifyAsyncHttpResponseContent(bodyContext, attrs);

    assertTrue(modified.isPresent());
    JsonNode json = MAPPER.readTree(collect(modified.get()).get(5, TimeUnit.SECONDS));
    JsonNode embedding =
        json.get("UnprocessedItems")
            .get("items")
            .get(0)
            .get("PutRequest")
            .get("Item")
            .get("embedding");
    assertFloat32Vector(embedding, 3.0f, 4.5f);
  }

  @Test
  public void testDeflateAsyncHttpResponseContentConvertsFloat32VectorWithoutResultHolder()
      throws Exception {
    byte[] responseBody = bytes("{\"Item\":{\"embedding\":{\"FLOAT32VECTOR\":[3.0,4.5]}}}");
    byte[] compressedBody = deflateCompress(responseBody);
    ExecutionAttributes attrs = new ExecutionAttributes();
    SdkHttpResponse compressedResponse =
        SdkHttpResponse.builder().statusCode(200).putHeader("Content-Encoding", "deflate").build();

    Context.ModifyHttpResponse headerContext =
        responseContext(
            queryHttpRequest(), compressedResponse, singleChunkPublisher(compressedBody));
    SdkHttpResponse modifiedResponse =
        VectorSearchInterceptor.INSTANCE.modifyHttpResponse(headerContext, attrs);
    Context.ModifyHttpResponse bodyContext =
        responseContext(queryHttpRequest(), modifiedResponse, singleChunkPublisher(compressedBody));

    assertFalse(modifiedResponse.firstMatchingHeader("Content-Encoding").isPresent());

    Optional<Publisher<ByteBuffer>> modified =
        VectorSearchInterceptor.INSTANCE.modifyAsyncHttpResponseContent(bodyContext, attrs);

    assertTrue(modified.isPresent());
    JsonNode json = MAPPER.readTree(collect(modified.get()).get(5, TimeUnit.SECONDS));

    JsonNode embedding = json.get("Item").get("embedding");
    assertFloat32Vector(embedding, 3.0f, 4.5f);
  }

  @Test
  public void testModifyHttpResponseReadFailureIsRetryable() {
    IOException readFailure = new IOException("response reset");
    Context.ModifyHttpResponse context =
        responseContext(
            listTablesHttpRequest(),
            SdkHttpResponse.builder().statusCode(200).build(),
            failingInputStream(readFailure));

    RetryableException failure =
        expectRetryable(
            () ->
                VectorSearchInterceptor.INSTANCE.modifyHttpResponse(
                    context, new ExecutionAttributes()));

    assertSame(readFailure, failure.getCause());
  }

  @Test
  public void testModifyHttpResponseContentReadFailureIsRetryable() {
    IOException readFailure = new IOException("truncated response");
    Context.ModifyHttpResponse context =
        responseContext(
            listTablesHttpRequest(),
            SdkHttpResponse.builder().statusCode(200).build(),
            failingInputStream(readFailure));

    RetryableException failure =
        expectRetryable(
            () ->
                VectorSearchInterceptor.INSTANCE.modifyHttpResponseContent(
                    context, new ExecutionAttributes()));

    assertSame(readFailure, failure.getCause());
  }

  @Test
  public void testEmptyGzipAsyncResponseFailureIsRetryable() throws Exception {
    ExecutionAttributes attrs = new ExecutionAttributes();
    SdkHttpResponse response =
        SdkHttpResponse.builder()
            .statusCode(200)
            .putHeader("Content-Encoding", "gzip")
            .putHeader("Content-Length", "0")
            .build();

    SdkHttpResponse modifiedResponse =
        VectorSearchInterceptor.INSTANCE.modifyHttpResponse(
            responseContext(queryHttpRequest(), response), attrs);
    Publisher<ByteBuffer> modifiedBody =
        VectorSearchInterceptor.INSTANCE
            .modifyAsyncHttpResponseContent(
                responseContext(
                    queryHttpRequest(), modifiedResponse, singleChunkPublisher(new byte[0])),
                attrs)
            .get();

    try {
      collect(modifiedBody).get(5, TimeUnit.SECONDS);
      fail("Expected empty gzip response to fail");
    } catch (ExecutionException e) {
      assertTrue(e.getCause() instanceof RetryableException);
      assertTrue(((RetryableException) e.getCause()).retryable());
      assertTrue(e.getCause().getCause() instanceof IOException);
    }
  }

  @Test
  public void testSdkSyncClientRetriesInterceptorResponseReadFailure() {
    ReadFailureThenSuccessHttpClient httpClient = new ReadFailureThenSuccessHttpClient();
    DynamoDbClient client =
        DynamoDbClient.builder()
            .endpointOverride(URI.create("http://localhost:8000"))
            .credentialsProvider(AnonymousCredentialsProvider.create())
            .region(Region.US_EAST_1)
            .httpClient(httpClient)
            .overrideConfiguration(c -> c.addExecutionInterceptor(VectorSearchInterceptor.INSTANCE))
            .build();

    try {
      ListTablesResponse response = client.listTables(ListTablesRequest.builder().build());

      assertEquals(Arrays.asList("items"), response.tableNames());
      assertEquals(2, httpClient.attempts());
    } finally {
      client.close();
    }
  }

  @Test
  public void testSdkAsyncClientRetriesEmptyEncodedResponse() throws Exception {
    EmptyGzipThenSuccessAsyncHttpClient httpClient = new EmptyGzipThenSuccessAsyncHttpClient();
    DynamoDbAsyncClient client =
        DynamoDbAsyncClient.builder()
            .endpointOverride(URI.create("http://localhost:8000"))
            .credentialsProvider(AnonymousCredentialsProvider.create())
            .region(Region.US_EAST_1)
            .httpClient(httpClient)
            .overrideConfiguration(c -> c.addExecutionInterceptor(VectorSearchInterceptor.INSTANCE))
            .build();

    try {
      ListTablesResponse response =
          client.listTables(ListTablesRequest.builder().build()).get(5, TimeUnit.SECONDS);

      assertEquals(Arrays.asList("items"), response.tableNames());
      assertEquals(2, httpClient.attempts());
    } finally {
      client.close();
    }
  }

  @Test
  public void testCrc64NvmeIsValidatedWhileChecksumMetadataIsPreserved() throws Exception {
    byte[] responseBody = bytes("{\"Item\":{\"embedding\":{\"FLOAT32VECTOR\":[3.0,4.5]}}}");
    ExecutionAttributes attrs = new ExecutionAttributes();
    SdkHttpResponse response =
        SdkHttpResponse.builder()
            .statusCode(200)
            .putHeader("Content-Length", String.valueOf(responseBody.length))
            .putHeader("x-amz-checksum-crc64nvme", crc64Nvme(responseBody))
            .putHeader("x-amz-checksum-type", "FULL_OBJECT")
            .build();

    Context.ModifyHttpResponse headerContext =
        responseContext(getItemHttpRequest(), response, responseBody);
    SdkHttpResponse modifiedResponse =
        VectorSearchInterceptor.INSTANCE.modifyHttpResponse(headerContext, attrs);

    assertFalse(modifiedResponse.firstMatchingHeader("Content-Length").isPresent());
    assertFalse(modifiedResponse.firstMatchingHeader("x-amz-checksum-crc64nvme").isPresent());
    assertEquals("FULL_OBJECT", modifiedResponse.firstMatchingHeader("x-amz-checksum-type").get());

    Optional<java.io.InputStream> modified =
        VectorSearchInterceptor.INSTANCE.modifyHttpResponseContent(
            responseContext(getItemHttpRequest(), modifiedResponse, responseBody), attrs);
    JsonNode embedding = MAPPER.readTree(readAllBytes(modified.get())).get("Item").get("embedding");
    assertFloat32Vector(embedding, 3.0f, 4.5f);
  }

  @Test
  public void testResultHolderIsClearedBeforeEachResponseAttempt() {
    VectorSearchResultHolder holder = new VectorSearchResultHolder();
    ExecutionAttributes attrs = new ExecutionAttributes();
    attrs.putAttribute(VectorSearchInterceptor.RESULT_HOLDER, holder);
    SdkHttpResponse response = SdkHttpResponse.builder().statusCode(200).build();

    VectorSearchInterceptor.INSTANCE.modifyHttpResponse(
        responseContext(queryHttpRequest(), response, bytes("{\"Items\":[],\"Scores\":[0.7]}")),
        attrs);
    assertEquals(Arrays.asList(0.7), holder.getScores());

    VectorSearchInterceptor.INSTANCE.modifyHttpResponse(
        responseContext(queryHttpRequest(), response, bytes("{\"Items\":[]}")), attrs);
    assertNull(holder.getScores());
    assertNull(holder.getVectorIndexes());
  }

  @Test
  public void testDynamoDbAsyncClientInjectsVectorSearchAndReadsScores() throws Exception {
    RecordingAsyncHttpClient httpClient =
        new RecordingAsyncHttpClient(
            bytes(
                "{\"Items\":[{\"id\":{\"S\":\"item-1\"},"
                    + "\"embedding\":{\"FLOAT32VECTOR\":[3.0,4.0]}}],"
                    + "\"Count\":1,\"ScannedCount\":1,\"Scores\":[0.9]}"));

    DynamoDbAsyncClient client =
        DynamoDbAsyncClient.builder()
            .endpointOverride(URI.create("http://localhost:8000"))
            .credentialsProvider(AnonymousCredentialsProvider.create())
            .region(Region.US_EAST_1)
            .httpClient(httpClient)
            .overrideConfiguration(c -> c.addExecutionInterceptor(VectorSearchInterceptor.INSTANCE))
            .build();

    try {
      VectorQueryResult result =
          VectorSearchSupport.queryAsync(
                  client,
                  QueryRequest.builder()
                      .tableName("items")
                      .indexName("embedding-index")
                      .limit(1)
                      .build(),
                  VectorSearch.builder().queryVector(3.0f, 4.0f).returnScores(true).build())
              .get(5, TimeUnit.SECONDS);

      String requestJson = new String(httpClient.requestBody(), StandardCharsets.UTF_8);
      assertTrue(requestJson.contains("\"VectorSearch\""));
      assertTrue(requestJson.contains("\"FLOAT32VECTOR\""));
      assertTrue(requestJson.contains("\"ReturnScores\":\"SIMILARITY\""));

      assertEquals(1, result.items().size());
      assertEquals("item-1", result.items().get(0).get("id").s());
      assertEquals(0.9, result.scores().get(0), 1e-9);
      AttributeValue embedding = result.items().get(0).get("embedding");
      assertTrue(Float32Vector.isFloat32Vector(embedding));
      assertArrayEquals(new float[] {3.0f, 4.0f}, Float32Vector.toFloats(embedding), 0.0f);
    } finally {
      client.close();
    }
  }

  @Test
  public void testDynamoDbAsyncClientReadsGzipVectorSearchResponse() throws Exception {
    RecordingAsyncHttpClient httpClient =
        new RecordingAsyncHttpClient(
            gzipCompress(
                bytes(
                    "{\"Items\":[{\"id\":{\"S\":\"item-1\"},"
                        + "\"embedding\":{\"FLOAT32VECTOR\":[3.0,4.0]}}],"
                        + "\"Count\":1,\"ScannedCount\":1,\"Scores\":[0.9]}")),
            "gzip");

    DynamoDbAsyncClient client =
        DynamoDbAsyncClient.builder()
            .endpointOverride(URI.create("http://localhost:8000"))
            .credentialsProvider(AnonymousCredentialsProvider.create())
            .region(Region.US_EAST_1)
            .httpClient(httpClient)
            .overrideConfiguration(c -> c.addExecutionInterceptor(VectorSearchInterceptor.INSTANCE))
            .build();

    try {
      VectorQueryResult result =
          VectorSearchSupport.queryAsync(
                  client,
                  QueryRequest.builder()
                      .tableName("items")
                      .indexName("embedding-index")
                      .limit(1)
                      .build(),
                  VectorSearch.builder().queryVector(3.0f, 4.0f).returnScores(true).build())
              .get(5, TimeUnit.SECONDS);

      assertEquals(1, result.items().size());
      assertEquals("item-1", result.items().get(0).get("id").s());
      assertEquals(0.9, result.scores().get(0), 1e-9);
      AttributeValue embedding = result.items().get(0).get("embedding");
      assertTrue(Float32Vector.isFloat32Vector(embedding));
      assertArrayEquals(new float[] {3.0f, 4.0f}, Float32Vector.toFloats(embedding), 0.0f);
    } finally {
      client.close();
    }
  }

  @Test
  public void testDynamoDbAsyncClientReadsUncompressedVectorResponseWithCrc32Header()
      throws Exception {
    byte[] responseBody =
        bytes(
            "{\"Items\":[{\"id\":{\"S\":\"item-1\"},"
                + "\"embedding\":{\"FLOAT32VECTOR\":[3.0,4.0]}}],"
                + "\"Count\":1,\"ScannedCount\":1,\"Scores\":[0.9]}");
    RecordingAsyncHttpClient httpClient =
        new RecordingAsyncHttpClient(responseBody, null, crc32(responseBody));

    DynamoDbAsyncClient client =
        DynamoDbAsyncClient.builder()
            .endpointOverride(URI.create("http://localhost:8000"))
            .credentialsProvider(AnonymousCredentialsProvider.create())
            .region(Region.US_EAST_1)
            .httpClient(httpClient)
            .overrideConfiguration(c -> c.addExecutionInterceptor(VectorSearchInterceptor.INSTANCE))
            .build();

    try {
      VectorQueryResult result =
          VectorSearchSupport.queryAsync(
                  client,
                  QueryRequest.builder()
                      .tableName("items")
                      .indexName("embedding-index")
                      .limit(1)
                      .build(),
                  VectorSearch.builder().queryVector(3.0f, 4.0f).returnScores(true).build())
              .get(5, TimeUnit.SECONDS);

      assertEquals(1, result.items().size());
      assertEquals("item-1", result.items().get(0).get("id").s());
      assertEquals(0.9, result.scores().get(0), 1e-9);
      AttributeValue embedding = result.items().get(0).get("embedding");
      assertTrue(Float32Vector.isFloat32Vector(embedding));
      assertArrayEquals(new float[] {3.0f, 4.0f}, Float32Vector.toFloats(embedding), 0.0f);
    } finally {
      client.close();
    }
  }

  @Test
  public void testCreateTableAsyncReturnsVectorIndexesFromRawResponse() throws Exception {
    RecordingAsyncHttpClient httpClient =
        new RecordingAsyncHttpClient(
            bytes(
                "{\"TableDescription\":{\"TableName\":\"items\",\"VectorIndexes\":[{"
                    + "\"IndexName\":\"embedding-index\","
                    + "\"VectorAttribute\":{\"AttributeName\":\"embedding\",\"Dimensions\":2},"
                    + "\"SimilarityFunction\":\"COSINE\",\"IndexStatus\":\"CREATING\","
                    + "\"Backfilling\":true}]}}"));
    DynamoDbAsyncClient client =
        DynamoDbAsyncClient.builder()
            .endpointOverride(URI.create("http://localhost:8000"))
            .credentialsProvider(AnonymousCredentialsProvider.create())
            .region(Region.US_EAST_1)
            .httpClient(httpClient)
            .overrideConfiguration(c -> c.addExecutionInterceptor(VectorSearchInterceptor.INSTANCE))
            .build();
    VectorIndex requestedIndex =
        VectorIndex.builder()
            .indexName("embedding-index")
            .vectorAttribute(
                VectorAttribute.builder().attributeName("embedding").dimensions(2).build())
            .similarityFunction("COSINE")
            .build();
    CreateTableRequest request =
        CreateTableRequest.builder()
            .tableName("items")
            .keySchema(KeySchemaElement.builder().attributeName("id").keyType(KeyType.HASH).build())
            .attributeDefinitions(
                AttributeDefinition.builder()
                    .attributeName("id")
                    .attributeType(ScalarAttributeType.S)
                    .build())
            .billingMode(BillingMode.PAY_PER_REQUEST)
            .build();

    try {
      VectorSearchSupport.CreateTableWithVectorIndexes result =
          VectorSearchSupport.createTableAsync(client, request, Arrays.asList(requestedIndex))
              .get(5, TimeUnit.SECONDS);

      JsonNode requestJson = MAPPER.readTree(httpClient.requestBody());
      assertEquals(
          "embedding-index", requestJson.get("VectorIndexes").get(0).get("IndexName").asText());
      assertEquals("items", result.response().tableDescription().tableName());
      assertEquals(1, result.vectorIndexes().size());
      VectorIndex returnedIndex = result.vectorIndexes().get(0);
      assertEquals("embedding-index", returnedIndex.indexName());
      assertEquals("embedding", returnedIndex.vectorAttribute().attributeName());
      assertEquals(2, returnedIndex.vectorAttribute().dimensions());
      assertEquals("COSINE", returnedIndex.similarityFunction());
      assertEquals("CREATING", returnedIndex.indexStatus());
      assertTrue(returnedIndex.backfilling());
    } finally {
      client.close();
    }
  }

  @Test
  public void testDescribeTableAsyncReturnsVectorIndexesFromRawResponse() throws Exception {
    RecordingAsyncHttpClient httpClient =
        new RecordingAsyncHttpClient(
            bytes(
                "{\"Table\":{\"TableName\":\"items\",\"VectorIndexes\":[{"
                    + "\"IndexName\":\"embedding-index\","
                    + "\"VectorAttribute\":{\"AttributeName\":\"embedding\",\"Dimensions\":2},"
                    + "\"SimilarityFunction\":\"COSINE\",\"IndexStatus\":\"ACTIVE\","
                    + "\"Backfilling\":false}]}}"));
    DynamoDbAsyncClient client =
        DynamoDbAsyncClient.builder()
            .endpointOverride(URI.create("http://localhost:8000"))
            .credentialsProvider(AnonymousCredentialsProvider.create())
            .region(Region.US_EAST_1)
            .httpClient(httpClient)
            .overrideConfiguration(c -> c.addExecutionInterceptor(VectorSearchInterceptor.INSTANCE))
            .build();

    try {
      VectorSearchSupport.DescribeTableWithVectorIndexes result =
          VectorSearchSupport.describeTableAsync(
                  client, DescribeTableRequest.builder().tableName("items").build())
              .get(5, TimeUnit.SECONDS);

      assertEquals("items", result.response().table().tableName());
      assertEquals(1, result.vectorIndexes().size());
      VectorIndex index = result.vectorIndexes().get(0);
      assertEquals("embedding-index", index.indexName());
      assertEquals("embedding", index.vectorAttribute().attributeName());
      assertEquals(2, index.vectorAttribute().dimensions());
      assertEquals("COSINE", index.similarityFunction());
      assertEquals("ACTIVE", index.indexStatus());
      assertFalse(index.backfilling());
    } finally {
      client.close();
    }
  }

  @Test
  public void testAsyncGetThenPutPreservesFloat32VectorIdentity() throws Exception {
    RoundTripAsyncHttpClient httpClient = new RoundTripAsyncHttpClient();
    DynamoDbAsyncClient client =
        DynamoDbAsyncClient.builder()
            .endpointOverride(URI.create("http://localhost:8000"))
            .credentialsProvider(AnonymousCredentialsProvider.create())
            .region(Region.US_EAST_1)
            .httpClient(httpClient)
            .overrideConfiguration(c -> c.addExecutionInterceptor(VectorSearchInterceptor.INSTANCE))
            .build();

    try {
      GetItemResponse getResponse =
          client
              .getItem(
                  GetItemRequest.builder()
                      .tableName("items")
                      .key(java.util.Collections.singletonMap("id", AttributeValue.fromS("item-1")))
                      .build())
              .get(5, TimeUnit.SECONDS);

      AttributeValue embedding = getResponse.item().get("embedding");
      assertTrue(Float32Vector.isFloat32Vector(embedding));
      assertArrayEquals(new float[] {3.0f, 4.0f}, Float32Vector.toFloats(embedding), 0.0f);

      client
          .putItem(PutItemRequest.builder().tableName("items").item(getResponse.item()).build())
          .get(5, TimeUnit.SECONDS);

      JsonNode putRequest = MAPPER.readTree(httpClient.requestBody(1));
      JsonNode writtenEmbedding = putRequest.get("Item").get("embedding");
      assertNotNull(writtenEmbedding.get("FLOAT32VECTOR"));
      assertNull(writtenEmbedding.get("B"));
      assertNull(writtenEmbedding.get("L"));
      assertEquals(3.0, writtenEmbedding.get("FLOAT32VECTOR").get(0).asDouble(), 0.0);
      assertEquals(4.0, writtenEmbedding.get("FLOAT32VECTOR").get(1).asDouble(), 0.0);
    } finally {
      client.close();
    }
  }

  @Test
  public void testVectorSearchBeforeGzipCompressesModifiedRequestBody() throws Exception {
    ExecutionInterceptorChain chain =
        new ExecutionInterceptorChain(
            Arrays.asList(VectorSearchInterceptor.INSTANCE, new GzipRequestInterceptor(1)));
    ExecutionAttributes attrs = new ExecutionAttributes();
    attrs.putAttribute(
        VectorSearchInterceptor.VECTOR_SEARCH,
        VectorSearch.builder().queryVector(1.0f, 2.0f).returnScores(true).build());

    InterceptorContext context =
        InterceptorContext.builder()
            .request(ListTablesRequest.builder().build())
            .httpRequest(queryHttpRequest())
            .requestBody(RequestBody.fromString("{\"TableName\":\"items\"}"))
            .build();

    InterceptorContext result = chain.modifyHttpRequestAndHttpContent(context, attrs);

    assertEquals("gzip", result.httpRequest().firstMatchingHeader("Content-Encoding").get());
    byte[] compressed = readRequestBody(result.requestBody());
    byte[] uncompressed = gzipDecompress(compressed);
    String json = new String(uncompressed, StandardCharsets.UTF_8);
    assertTrue(json.contains("\"VectorSearch\""));
    assertTrue(json.contains("\"FLOAT32VECTOR\""));
    assertEquals(
        String.valueOf(compressed.length),
        result.httpRequest().firstMatchingHeader("Content-Length").get());
  }

  @Test
  public void testVectorSearchBeforeGzipReplaysUnchangedSingleUseRequestBody() throws Exception {
    ExecutionInterceptorChain chain =
        new ExecutionInterceptorChain(
            Arrays.asList(VectorSearchInterceptor.INSTANCE, new GzipRequestInterceptor(1)));
    ExecutionAttributes attrs = new ExecutionAttributes();
    byte[] original = bytes("{\"TableName\":\"items\",\"Item\":{\"id\":{\"S\":\"item-1\"}}}");
    ByteArrayInputStream singleUseStream = new ByteArrayInputStream(original);
    ContentStreamProvider singleUseProvider = () -> singleUseStream;

    InterceptorContext context =
        InterceptorContext.builder()
            .request(ListTablesRequest.builder().build())
            .httpRequest(queryHttpRequest())
            .requestBody(
                RequestBody.fromContentProvider(
                    singleUseProvider, original.length, "application/x-amz-json-1.0"))
            .build();

    InterceptorContext result = chain.modifyHttpRequestAndHttpContent(context, attrs);

    assertEquals("gzip", result.httpRequest().firstMatchingHeader("Content-Encoding").get());
    byte[] compressed = readRequestBody(result.requestBody());
    assertArrayEquals(original, gzipDecompress(compressed));
    assertEquals(
        String.valueOf(compressed.length),
        result.httpRequest().firstMatchingHeader("Content-Length").get());
  }

  @Test
  public void testAsyncResponseCancellationStopsBufferingAndDownstreamSignals() {
    AtomicBoolean upstreamCancelled = new AtomicBoolean();
    AtomicInteger downstreamSignals = new AtomicInteger();
    Publisher<ByteBuffer> source =
        subscriber -> {
          subscriber.onSubscribe(
              new Subscription() {
                @Override
                public void request(long n) {}

                @Override
                public void cancel() {
                  upstreamCancelled.set(true);
                }
              });
          // Simulate signals already in flight when cancellation reaches the upstream publisher.
          subscriber.onNext(ByteBuffer.wrap(bytes("{\"TableNames\":[\"items\"]}")));
          subscriber.onComplete();
        };

    Optional<Publisher<ByteBuffer>> modified =
        VectorSearchInterceptor.INSTANCE.modifyAsyncHttpResponseContent(
            responseContext(listTablesHttpRequest(), source), new ExecutionAttributes());

    assertTrue(modified.isPresent());
    modified
        .get()
        .subscribe(
            new Subscriber<ByteBuffer>() {
              @Override
              public void onSubscribe(Subscription subscription) {
                subscription.cancel();
              }

              @Override
              public void onNext(ByteBuffer byteBuffer) {
                downstreamSignals.incrementAndGet();
              }

              @Override
              public void onError(Throwable throwable) {
                downstreamSignals.incrementAndGet();
              }

              @Override
              public void onComplete() {
                downstreamSignals.incrementAndGet();
              }
            });

    assertTrue(upstreamCancelled.get());
    assertEquals(0, downstreamSignals.get());
  }

  @Test
  public void testAsyncResponseCancellationFromOnNextSuppressesCompletion() {
    AtomicInteger downstreamItems = new AtomicInteger();
    AtomicInteger downstreamCompletions = new AtomicInteger();
    AtomicInteger downstreamErrors = new AtomicInteger();
    Optional<Publisher<ByteBuffer>> modified =
        VectorSearchInterceptor.INSTANCE.modifyAsyncHttpResponseContent(
            responseContext(
                listTablesHttpRequest(),
                singleChunkPublisher(bytes("{\"TableNames\":[\"items\"]}"))),
            new ExecutionAttributes());

    assertTrue(modified.isPresent());
    modified
        .get()
        .subscribe(
            new Subscriber<ByteBuffer>() {
              private Subscription subscription;

              @Override
              public void onSubscribe(Subscription subscription) {
                this.subscription = subscription;
                subscription.request(1);
              }

              @Override
              public void onNext(ByteBuffer byteBuffer) {
                downstreamItems.incrementAndGet();
                subscription.cancel();
              }

              @Override
              public void onError(Throwable throwable) {
                downstreamErrors.incrementAndGet();
              }

              @Override
              public void onComplete() {
                downstreamCompletions.incrementAndGet();
              }
            });

    assertEquals(1, downstreamItems.get());
    assertEquals(0, downstreamCompletions.get());
    assertEquals(0, downstreamErrors.get());
  }

  @Test(expected = IllegalStateException.class)
  public void testDeleteVectorIndexActionRequiresIndexName() {
    DeleteVectorIndexAction.builder().build();
  }

  private static SdkHttpRequest queryHttpRequest() {
    return SdkHttpRequest.builder()
        .protocol("http")
        .host("localhost")
        .port(8000)
        .method(SdkHttpMethod.POST)
        .encodedPath("/")
        .putHeader("X-Amz-Target", "DynamoDB_20120810.Query")
        .putHeader("Content-Type", "application/x-amz-json-1.0")
        .build();
  }

  private static SdkHttpRequest getItemHttpRequest() {
    return SdkHttpRequest.builder()
        .protocol("http")
        .host("localhost")
        .port(8000)
        .method(SdkHttpMethod.POST)
        .encodedPath("/")
        .putHeader("X-Amz-Target", "DynamoDB_20120810.GetItem")
        .putHeader("Content-Type", "application/x-amz-json-1.0")
        .build();
  }

  private static SdkHttpRequest batchWriteItemHttpRequest() {
    return SdkHttpRequest.builder()
        .protocol("http")
        .host("localhost")
        .port(8000)
        .method(SdkHttpMethod.POST)
        .encodedPath("/")
        .putHeader("X-Amz-Target", "DynamoDB_20120810.BatchWriteItem")
        .putHeader("Content-Type", "application/x-amz-json-1.0")
        .build();
  }

  private static SdkHttpRequest listTablesHttpRequest() {
    return SdkHttpRequest.builder()
        .protocol("http")
        .host("localhost")
        .port(8000)
        .method(SdkHttpMethod.POST)
        .encodedPath("/")
        .putHeader("X-Amz-Target", "DynamoDB_20120810.ListTables")
        .putHeader("Content-Type", "application/x-amz-json-1.0")
        .build();
  }

  private static Context.ModifyHttpResponse responseContext(
      SdkHttpRequest httpRequest, Publisher<ByteBuffer> publisher) {
    return responseContext(
        httpRequest, SdkHttpResponse.builder().statusCode(200).build(), publisher);
  }

  private static Context.ModifyHttpResponse responseContext(
      SdkHttpRequest httpRequest, SdkHttpResponse httpResponse, Publisher<ByteBuffer> publisher) {
    return InterceptorContext.builder()
        .request(QueryRequest.builder().tableName("items").build())
        .httpRequest(httpRequest)
        .httpResponse(httpResponse)
        .responsePublisher(publisher)
        .build();
  }

  private static Context.ModifyHttpResponse responseContext(
      SdkHttpRequest httpRequest, SdkHttpResponse httpResponse) {
    return InterceptorContext.builder()
        .request(QueryRequest.builder().tableName("items").build())
        .httpRequest(httpRequest)
        .httpResponse(httpResponse)
        .build();
  }

  private static Context.ModifyHttpResponse responseContext(
      SdkHttpRequest httpRequest, SdkHttpResponse httpResponse, byte[] responseBody) {
    return responseContext(httpRequest, httpResponse, new ByteArrayInputStream(responseBody));
  }

  private static Context.ModifyHttpResponse responseContext(
      SdkHttpRequest httpRequest, SdkHttpResponse httpResponse, InputStream responseBody) {
    return InterceptorContext.builder()
        .request(QueryRequest.builder().tableName("items").build())
        .httpRequest(httpRequest)
        .httpResponse(httpResponse)
        .responseBody(responseBody)
        .build();
  }

  private static byte[] readRequestBody(Optional<RequestBody> body) throws IOException {
    assertTrue(body.isPresent());
    return readAllBytes(body.get().contentStreamProvider().newStream());
  }

  private static byte[] gzipCompress(byte[] uncompressed) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (GZIPOutputStream gzip = new GZIPOutputStream(out)) {
      gzip.write(uncompressed);
    }
    return out.toByteArray();
  }

  private static byte[] gzipDecompress(byte[] compressed) throws IOException {
    return readAllBytes(new GZIPInputStream(new ByteArrayInputStream(compressed)));
  }

  private static String crc32(byte[] bytes) {
    CRC32 crc32 = new CRC32();
    crc32.update(bytes, 0, bytes.length);
    return Long.toString(crc32.getValue());
  }

  private static String crc32c(byte[] bytes) {
    CRC32C crc32c = new CRC32C();
    crc32c.update(bytes, 0, bytes.length);
    return Long.toString(crc32c.getValue());
  }

  private static String sha256(byte[] bytes) {
    try {
      return Base64.getEncoder().encodeToString(MessageDigest.getInstance("SHA-256").digest(bytes));
    } catch (java.security.NoSuchAlgorithmException e) {
      throw new AssertionError(e);
    }
  }

  private static String crc64Nvme(byte[] bytes) {
    SdkChecksum checksum = SdkChecksum.forAlgorithm(DefaultChecksumAlgorithm.CRC64NVME);
    checksum.update(bytes, 0, bytes.length);
    return Base64.getEncoder().encodeToString(checksum.getChecksumBytes());
  }

  private static InputStream failingInputStream(IOException failure) {
    return new InputStream() {
      @Override
      public int read() throws IOException {
        throw failure;
      }
    };
  }

  private static RetryableException expectRetryable(Runnable action) {
    try {
      action.run();
      fail("Expected a retryable response-processing failure");
      throw new AssertionError("unreachable");
    } catch (RetryableException e) {
      assertTrue(e.retryable());
      return e;
    }
  }

  private static void assertFloat32Vector(JsonNode attribute, float... expected) {
    assertNull(attribute.get("FLOAT32VECTOR"));
    assertNull(attribute.get("L"));
    assertNotNull(attribute.get("B"));
    AttributeValue marker =
        AttributeValue.fromB(
            SdkBytes.fromByteArray(Base64.getDecoder().decode(attribute.get("B").asText())));
    assertTrue(Float32Vector.isFloat32Vector(marker));
    assertArrayEquals(expected, Float32Vector.toFloats(marker), 0.0f);
  }

  private static byte[] deflateCompress(byte[] uncompressed) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (DeflaterOutputStream deflate = new DeflaterOutputStream(out)) {
      deflate.write(uncompressed);
    }
    return out.toByteArray();
  }

  private static byte[] readAllBytes(java.io.InputStream in) throws IOException {
    try {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      byte[] buffer = new byte[4096];
      int len;
      while ((len = in.read(buffer)) != -1) {
        out.write(buffer, 0, len);
      }
      return out.toByteArray();
    } finally {
      in.close();
    }
  }

  private static Publisher<ByteBuffer> singleChunkPublisher(byte[] bytes) {
    return subscriber ->
        subscriber.onSubscribe(
            new Subscription() {
              private boolean done;

              @Override
              public void request(long n) {
                if (done) {
                  return;
                }
                if (n <= 0) {
                  done = true;
                  subscriber.onError(
                      new IllegalArgumentException(
                          "Reactive Streams request amount must be positive"));
                  return;
                }
                done = true;
                if (bytes.length > 0) {
                  subscriber.onNext(ByteBuffer.wrap(bytes));
                }
                subscriber.onComplete();
              }

              @Override
              public void cancel() {
                done = true;
              }
            });
  }

  private static CompletableFuture<byte[]> collect(Publisher<ByteBuffer> publisher) {
    CompletableFuture<byte[]> result = new CompletableFuture<>();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    publisher.subscribe(
        new Subscriber<ByteBuffer>() {
          @Override
          public void onSubscribe(Subscription subscription) {
            subscription.request(Long.MAX_VALUE);
          }

          @Override
          public void onNext(ByteBuffer byteBuffer) {
            ByteBuffer copy = byteBuffer.asReadOnlyBuffer();
            byte[] bytes = new byte[copy.remaining()];
            copy.get(bytes);
            out.write(bytes, 0, bytes.length);
          }

          @Override
          public void onError(Throwable throwable) {
            result.completeExceptionally(throwable);
          }

          @Override
          public void onComplete() {
            result.complete(out.toByteArray());
          }
        });
    return result;
  }

  private static byte[] bytes(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }

  private static final class ReadFailureThenSuccessHttpClient implements SdkHttpClient {
    private final AtomicInteger attempts = new AtomicInteger();

    @Override
    public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
      return new ExecutableHttpRequest() {
        @Override
        public HttpExecuteResponse call() {
          int attempt = attempts.incrementAndGet();
          byte[] responseBody = bytes("{\"TableNames\":[\"items\"]}");
          InputStream body =
              attempt == 1
                  ? failingInputStream(new IOException("response reset"))
                  : new ByteArrayInputStream(responseBody);
          return HttpExecuteResponse.builder()
              .response(
                  SdkHttpFullResponse.builder()
                      .statusCode(200)
                      .putHeader("Content-Type", "application/x-amz-json-1.0")
                      .putHeader("Content-Length", String.valueOf(responseBody.length))
                      .build())
              .responseBody(AbortableInputStream.create(body))
              .build();
        }

        @Override
        public void abort() {}
      };
    }

    @Override
    public void close() {}

    private int attempts() {
      return attempts.get();
    }
  }

  private static final class EmptyGzipThenSuccessAsyncHttpClient implements SdkAsyncHttpClient {
    private final AtomicInteger attempts = new AtomicInteger();

    @Override
    public CompletableFuture<Void> execute(AsyncExecuteRequest request) {
      Publisher<ByteBuffer> requestPublisher = request.requestContentPublisher();
      CompletableFuture<byte[]> requestBodyFuture =
          requestPublisher != null
              ? collect(requestPublisher)
              : CompletableFuture.completedFuture(new byte[0]);
      return requestBodyFuture.thenAccept(
          ignored -> {
            int attempt = attempts.incrementAndGet();
            SdkHttpResponse.Builder response =
                SdkHttpResponse.builder()
                    .statusCode(200)
                    .putHeader("Content-Type", "application/x-amz-json-1.0");
            byte[] responseBody;
            if (attempt == 1) {
              response.putHeader("Content-Encoding", "gzip").putHeader("Content-Length", "0");
              responseBody = new byte[0];
            } else {
              responseBody = bytes("{\"TableNames\":[\"items\"]}");
              response.putHeader("Content-Length", String.valueOf(responseBody.length));
            }
            request.responseHandler().onHeaders(response.build());
            request.responseHandler().onStream(singleChunkPublisher(responseBody));
          });
    }

    @Override
    public void close() {}

    private int attempts() {
      return attempts.get();
    }
  }

  private static final class RoundTripAsyncHttpClient implements SdkAsyncHttpClient {
    private final AtomicInteger requestSequence = new AtomicInteger();
    private final List<byte[]> requestBodies = new ArrayList<>();

    @Override
    public CompletableFuture<Void> execute(AsyncExecuteRequest request) {
      Publisher<ByteBuffer> requestPublisher = request.requestContentPublisher();
      CompletableFuture<byte[]> requestBodyFuture =
          requestPublisher != null
              ? collect(requestPublisher)
              : CompletableFuture.completedFuture(new byte[0]);

      CompletableFuture<Void> responseFuture =
          requestBodyFuture.thenAccept(
              requestBody -> {
                int requestIndex = requestSequence.getAndIncrement();
                requestBodies.add(requestBody);
                byte[] responseBody =
                    requestIndex == 0
                        ? bytes(
                            "{\"Item\":{\"id\":{\"S\":\"item-1\"},"
                                + "\"embedding\":{\"FLOAT32VECTOR\":[3.0,4.0]}}}")
                        : bytes("{}");
                request
                    .responseHandler()
                    .onHeaders(
                        SdkHttpResponse.builder()
                            .statusCode(200)
                            .putHeader("Content-Type", "application/x-amz-json-1.0")
                            .build());
                request.responseHandler().onStream(singleChunkPublisher(responseBody));
              });

      responseFuture.whenComplete(
          (ignored, error) -> {
            if (error != null) {
              request.responseHandler().onError(error);
            }
          });
      return responseFuture;
    }

    @Override
    public void close() {}

    private byte[] requestBody(int index) {
      return requestBodies.get(index);
    }
  }

  private static final class RecordingAsyncHttpClient implements SdkAsyncHttpClient {
    private final byte[] responseBody;
    private final String contentEncoding;
    private final String crc32Header;
    private volatile byte[] requestBody;

    private RecordingAsyncHttpClient(byte[] responseBody) {
      this(responseBody, null);
    }

    private RecordingAsyncHttpClient(byte[] responseBody, String contentEncoding) {
      this(responseBody, contentEncoding, null);
    }

    private RecordingAsyncHttpClient(
        byte[] responseBody, String contentEncoding, String crc32Header) {
      this.responseBody = responseBody;
      this.contentEncoding = contentEncoding;
      this.crc32Header = crc32Header;
    }

    @Override
    public CompletableFuture<Void> execute(AsyncExecuteRequest request) {
      Publisher<ByteBuffer> requestPublisher = request.requestContentPublisher();
      CompletableFuture<byte[]> requestBodyFuture =
          requestPublisher != null
              ? collect(requestPublisher)
              : CompletableFuture.completedFuture(new byte[0]);

      CompletableFuture<Void> responseFuture =
          requestBodyFuture.thenAccept(
              bytes -> {
                requestBody = bytes;
                SdkHttpResponse.Builder responseBuilder =
                    SdkHttpResponse.builder()
                        .statusCode(200)
                        .putHeader("Content-Type", "application/x-amz-json-1.0");
                if (contentEncoding != null) {
                  responseBuilder.putHeader("Content-Encoding", contentEncoding);
                }
                if (crc32Header != null) {
                  responseBuilder.putHeader("x-amz-crc32", crc32Header);
                }
                request.responseHandler().onHeaders(responseBuilder.build());
                request.responseHandler().onStream(singleChunkPublisher(responseBody));
              });

      responseFuture.whenComplete(
          (ignored, error) -> {
            if (error != null) {
              request.responseHandler().onError(error);
            }
          });
      return responseFuture;
    }

    @Override
    public void close() {}

    private byte[] requestBody() {
      return requestBody;
    }
  }
}
