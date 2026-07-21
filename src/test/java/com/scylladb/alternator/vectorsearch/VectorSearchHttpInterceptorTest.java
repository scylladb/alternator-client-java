// Copyright 2026-present ScyllaDB
//
// SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

package com.scylladb.alternator.vectorsearch;

import static org.junit.Assert.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPOutputStream;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.InterceptorContext;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.http.async.AsyncExecuteRequest;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;

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
    assertNull(embedding.get("FLOAT32VECTOR"));
    assertEquals("1.0", embedding.get("L").get(0).get("N").asText());
    assertEquals("2.5", embedding.get("L").get(1).get("N").asText());
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
    assertNull(embedding.get("FLOAT32VECTOR"));
    assertEquals("1.0", embedding.get("L").get(0).get("N").asText());
    assertEquals("2.5", embedding.get("L").get(1).get("N").asText());
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
            .putHeader("x-amz-crc32", "123")
            .putHeader("X-Amz-Crc32c", "456")
            .putHeader("x-amz-checksum-sha256", "abc")
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
    assertNull(embedding.get("FLOAT32VECTOR"));
    assertEquals("3.0", embedding.get("L").get(0).get("N").asText());
    assertEquals("4.5", embedding.get("L").get(1).get("N").asText());
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
            .putHeader("x-amz-crc32", "123")
            .putHeader("X-Amz-Crc32c", "456")
            .putHeader("x-amz-checksum-sha256", "abc")
            .putHeader("X-Test", "kept")
            .build();

    Context.ModifyHttpResponse headerContext =
        responseContext(listTablesHttpRequest(), response, responseBody);
    SdkHttpResponse modifiedResponse =
        VectorSearchInterceptor.INSTANCE.modifyHttpResponse(headerContext, attrs);

    assertEquals(
        String.valueOf(responseBody.length),
        modifiedResponse.firstMatchingHeader("Content-Length").get());
    assertEquals("123", modifiedResponse.firstMatchingHeader("x-amz-crc32").get());
    assertEquals("456", modifiedResponse.firstMatchingHeader("x-amz-crc32c").get());
    assertEquals("abc", modifiedResponse.firstMatchingHeader("x-amz-checksum-sha256").get());
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
            .putHeader("x-amz-crc32", "123")
            .putHeader("X-Amz-Crc32c", "456")
            .putHeader("x-amz-checksum-sha256", "abc")
            .putHeader("X-Test", "kept")
            .build();

    Context.ModifyHttpResponse headerContext =
        responseContext(listTablesHttpRequest(), response, singleChunkPublisher(responseBody));
    SdkHttpResponse modifiedResponse =
        VectorSearchInterceptor.INSTANCE.modifyHttpResponse(headerContext, attrs);

    assertEquals(
        String.valueOf(responseBody.length),
        modifiedResponse.firstMatchingHeader("Content-Length").get());
    assertEquals("123", modifiedResponse.firstMatchingHeader("x-amz-crc32").get());
    assertEquals("456", modifiedResponse.firstMatchingHeader("x-amz-crc32c").get());
    assertEquals("abc", modifiedResponse.firstMatchingHeader("x-amz-checksum-sha256").get());
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
            .putHeader("x-amz-crc32", "123")
            .putHeader("X-Amz-Crc32c", "456")
            .putHeader("x-amz-checksum-sha256", "abc")
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
    assertNull(embedding.get("FLOAT32VECTOR"));
    assertEquals("3.0", embedding.get("L").get(0).get("N").asText());
    assertEquals("4.5", embedding.get("L").get(1).get("N").asText());
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
            .putHeader("x-amz-crc32", "123")
            .putHeader("X-Amz-Crc32c", "456")
            .putHeader("x-amz-checksum-sha256", "abc")
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
    assertNull(embedding.get("FLOAT32VECTOR"));
    assertEquals("3.0", embedding.get("L").get(0).get("N").asText());
    assertEquals("4.5", embedding.get("L").get(1).get("N").asText());
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
    assertNull(embedding.get("FLOAT32VECTOR"));
    assertEquals("3.0", embedding.get("L").get(0).get("N").asText());
    assertEquals("4.5", embedding.get("L").get(1).get("N").asText());
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
      assertEquals("3.0", embedding.l().get(0).n());
      assertEquals("4.0", embedding.l().get(1).n());
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
      assertEquals("3.0", embedding.l().get(0).n());
      assertEquals("4.0", embedding.l().get(1).n());
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
      assertEquals("3.0", embedding.l().get(0).n());
      assertEquals("4.0", embedding.l().get(1).n());
    } finally {
      client.close();
    }
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
    return InterceptorContext.builder()
        .request(QueryRequest.builder().tableName("items").build())
        .httpRequest(httpRequest)
        .httpResponse(httpResponse)
        .responseBody(new ByteArrayInputStream(responseBody))
        .build();
  }

  private static byte[] gzipCompress(byte[] uncompressed) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (GZIPOutputStream gzip = new GZIPOutputStream(out)) {
      gzip.write(uncompressed);
    }
    return out.toByteArray();
  }

  private static String crc32(byte[] bytes) {
    CRC32 crc32 = new CRC32();
    crc32.update(bytes, 0, bytes.length);
    return Long.toString(crc32.getValue());
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
