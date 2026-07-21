// Copyright 2026-present ScyllaDB
//
// SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

package com.scylladb.alternator.vectorsearch;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.GZIPInputStream;
import java.util.zip.InflaterInputStream;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttribute;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;

/**
 * An {@link ExecutionInterceptor} that enables Alternator's vector search extension.
 *
 * <p>Alternator extends the DynamoDB API with vector indexes ({@code VectorIndexes} in {@code
 * CreateTable}/{@code UpdateTable}/{@code DescribeTable}) and vector similarity search ({@code
 * VectorSearch} in {@code Query}). Because the standard AWS SDK for Java does not know about these
 * new fields, this interceptor bridges the gap by injecting them into the raw JSON request bodies
 * and extracting them from the raw JSON response bodies at the HTTP layer.
 *
 * <h2>Usage</h2>
 *
 * <p>Register the interceptor once when building the DynamoDB client:
 *
 * <pre>{@code
 * DynamoDbClient client = DynamoDbClient.builder()
 *     .overrideConfiguration(c -> c.addExecutionInterceptor(VectorSearchInterceptor.INSTANCE))
 *     ...
 *     .build();
 * }</pre>
 *
 * <p>Then use {@link VectorSearchSupport} to attach vector parameters to individual requests.
 *
 * <h2>Implementation notes</h2>
 *
 * <ul>
 *   <li>For vector search params (CreateTable/UpdateTable/Query): attached as {@link
 *       ExecutionAttribute}s via {@link VectorSearchSupport}; injected into the JSON body in {@link
 *       #modifyHttpContent(Context.ModifyHttpRequest, ExecutionAttributes)}.
 *   <li>For {@code FLOAT32VECTOR} item attributes on writes: the user creates a marker {@link
 *       AttributeValue} via {@link Float32Vector#toAttributeValue(float[])}; the interceptor
 *       detects the magic binary prefix in the serialised JSON and replaces it with {@code
 *       {"FLOAT32VECTOR": [...]}} before transmission. Works for any operation that carries {@link
 *       AttributeValue}s ({@code PutItem}, {@code UpdateItem}, {@code BatchWriteItem}, etc.).
 *   <li>For {@code FLOAT32VECTOR} item attributes on reads: {@link
 *       #modifyHttpResponseContent(Context.ModifyHttpResponse, ExecutionAttributes)} and {@link
 *       #modifyAsyncHttpResponseContent(Context.ModifyHttpResponse, ExecutionAttributes)} scan all
 *       response bodies for {@code FLOAT32VECTOR} attribute values and convert them transparently
 *       to standard DynamoDB list-of-numbers ({@code L}) {@link AttributeValue}s, accessible via
 *       {@link AttributeValue#l()}.
 * </ul>
 */
public final class VectorSearchInterceptor implements ExecutionInterceptor {

  /** Singleton instance — stateless, safe to share across clients. */
  public static final VectorSearchInterceptor INSTANCE = new VectorSearchInterceptor();

  // -------------------------------------------------------------------------
  // ExecutionAttribute keys
  // -------------------------------------------------------------------------

  /**
   * List of vector indexes to add to a {@code CreateTable} request. Set via {@link
   * VectorSearchSupport#withVectorIndexes}.
   */
  public static final ExecutionAttribute<List<VectorIndex>> VECTOR_INDEXES =
      new ExecutionAttribute<>("AlternatorVectorIndexes");

  /**
   * List of vector index updates to add to an {@code UpdateTable} request. Set via {@link
   * VectorSearchSupport#withVectorIndexUpdates}.
   */
  public static final ExecutionAttribute<List<VectorIndexUpdate>> VECTOR_INDEX_UPDATES =
      new ExecutionAttribute<>("AlternatorVectorIndexUpdates");

  /**
   * Vector search parameters to add to a {@code Query} request. Set via {@link
   * VectorSearchSupport#query} or {@link VectorSearchSupport#withVectorSearch}.
   */
  public static final ExecutionAttribute<VectorSearch> VECTOR_SEARCH =
      new ExecutionAttribute<>("AlternatorVectorSearch");

  /**
   * Per-request holder for extra response fields (scores, returned vector indexes). Set internally
   * by {@link VectorSearchSupport}.
   */
  static final ExecutionAttribute<VectorSearchResultHolder> RESULT_HOLDER =
      new ExecutionAttribute<>("AlternatorVectorSearchResultHolder");

  /**
   * Cache for the modified request body bytes, set by {@link
   * #modifyHttpContent(Context.ModifyHttpRequest, ExecutionAttributes)} and read by {@link
   * #modifyHttpRequest(Context.ModifyHttpRequest, ExecutionAttributes)}.
   *
   * <p>The SDK calls {@code modifyHttpContent} <em>before</em> {@code modifyHttpRequest} for each
   * interceptor (in a single {@code modifyHttpRequestAndHttpContent} pass), and passes the
   * <em>same</em> {@link ExecutionAttributes} instance to both. Caching here eliminates the second
   * full body read and JSON parse+rewrite that would otherwise be needed to compute the {@code
   * Content-Length} in {@code modifyHttpRequest}.
   */
  private static final ExecutionAttribute<byte[]> MODIFIED_BODY_CACHE =
      new ExecutionAttribute<>("AlternatorModifiedBodyCache");

  private static final ExecutionAttribute<List<String>> RESPONSE_CONTENT_ENCODINGS =
      new ExecutionAttribute<>("AlternatorResponseContentEncodings");
  private static final ExecutionAttribute<ProcessedResponseBody> PROCESSED_RESPONSE_BODY_CACHE =
      new ExecutionAttribute<>("AlternatorProcessedResponseBodyCache");

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String CONTENT_ENCODING_HEADER = "Content-Encoding";
  private static final String CONTENT_LENGTH_HEADER = "Content-Length";
  private static final String DYNAMODB_CRC32_HEADER = "x-amz-crc32";
  private static final String DYNAMODB_CRC32C_HEADER = "x-amz-crc32c";
  private static final String AWS_CHECKSUM_HEADER_PREFIX = "x-amz-checksum-";

  // DynamoDB X-Amz-Target suffixes
  private static final String TARGET_CREATE_TABLE = "DynamoDB_20120810.CreateTable";
  private static final String TARGET_UPDATE_TABLE = "DynamoDB_20120810.UpdateTable";
  private static final String TARGET_DESCRIBE_TABLE = "DynamoDB_20120810.DescribeTable";
  private static final String TARGET_BATCH_EXECUTE_STATEMENT =
      "DynamoDB_20120810.BatchExecuteStatement";
  private static final String TARGET_BATCH_GET_ITEM = "DynamoDB_20120810.BatchGetItem";
  private static final String TARGET_BATCH_WRITE_ITEM = "DynamoDB_20120810.BatchWriteItem";
  private static final String TARGET_DELETE_ITEM = "DynamoDB_20120810.DeleteItem";
  private static final String TARGET_EXECUTE_STATEMENT = "DynamoDB_20120810.ExecuteStatement";
  private static final String TARGET_EXECUTE_TRANSACTION = "DynamoDB_20120810.ExecuteTransaction";
  private static final String TARGET_GET_ITEM = "DynamoDB_20120810.GetItem";
  private static final String TARGET_PUT_ITEM = "DynamoDB_20120810.PutItem";
  private static final String TARGET_QUERY = "DynamoDB_20120810.Query";
  private static final String TARGET_SCAN = "DynamoDB_20120810.Scan";
  private static final String TARGET_TRANSACT_GET_ITEMS = "DynamoDB_20120810.TransactGetItems";
  private static final String TARGET_TRANSACT_WRITE_ITEMS = "DynamoDB_20120810.TransactWriteItems";
  private static final String TARGET_UPDATE_ITEM = "DynamoDB_20120810.UpdateItem";

  private VectorSearchInterceptor() {}

  // -------------------------------------------------------------------------
  // Request interception — inject extra JSON fields
  // -------------------------------------------------------------------------

  /**
   * Updates the {@code Content-Length} header to match the body produced by {@link
   * #modifyHttpContent(Context.ModifyHttpRequest, ExecutionAttributes)}, which runs first.
   *
   * <p>The AWS SDK sets {@code Content-Length} from the original (pre-interceptor) body length. If
   * we only change the body in {@code modifyHttpContent}, the server receives a stale {@code
   * Content-Length}. We therefore read the cached modified bytes (written by {@code
   * modifyHttpContent}) here and update the header to match.
   *
   * <p><b>Call order:</b> The SDK calls {@code modifyHttpContent} before {@code modifyHttpRequest}
   * for each interceptor (verified from {@code ExecutionInterceptorChain} bytecode), passing the
   * same {@link ExecutionAttributes} instance to both, so the cache is always populated by the time
   * this method runs.
   */
  @Override
  public SdkHttpRequest modifyHttpRequest(
      Context.ModifyHttpRequest context, ExecutionAttributes executionAttributes) {
    byte[] modifiedBytes = executionAttributes.getAttribute(MODIFIED_BODY_CACHE);
    if (modifiedBytes == null) {
      return context.httpRequest();
    }
    return context.httpRequest().toBuilder()
        .putHeader("Content-Length", String.valueOf(modifiedBytes.length))
        .build();
  }

  /**
   * Computes the modified body (injecting vector search parameters and converting {@code
   * FLOAT32VECTOR} markers), caches the result in {@link ExecutionAttributes} for {@link
   * #modifyHttpRequest}, and returns the new body.
   */
  @Override
  public Optional<RequestBody> modifyHttpContent(
      Context.ModifyHttpRequest context, ExecutionAttributes executionAttributes) {
    try {
      ProcessedRequestBody processedBody =
          computeProcessedRequestBody(context, executionAttributes);
      if (processedBody == null) {
        return context.requestBody();
      }
      if (processedBody.bodyModified()) {
        executionAttributes.putAttribute(MODIFIED_BODY_CACHE, processedBody.bytes());
      }
      return Optional.of(RequestBody.fromBytes(processedBody.bytes()));
    } catch (IOException e) {
      throw new RuntimeException("Failed to process vector search parameters in request body", e);
    }
  }

  /**
   * Computes the request body bytes, including vector rewrites when needed. If the body was read
   * but not changed, the original bytes are still returned so later interceptors can replay the
   * body safely.
   */
  private static ProcessedRequestBody computeProcessedRequestBody(
      Context.ModifyHttpRequest context, ExecutionAttributes executionAttributes)
      throws IOException {
    List<VectorIndex> vectorIndexes = executionAttributes.getAttribute(VECTOR_INDEXES);
    List<VectorIndexUpdate> vectorIndexUpdates =
        executionAttributes.getAttribute(VECTOR_INDEX_UPDATES);
    VectorSearch vectorSearch = executionAttributes.getAttribute(VECTOR_SEARCH);

    byte[] originalBytes = readBytes(context.requestBody());
    if (originalBytes == null) {
      return null;
    }

    boolean hasVectorParams =
        vectorIndexes != null || vectorIndexUpdates != null || vectorSearch != null;
    boolean hasFloat32Vectors = containsAsciiSubstring(originalBytes, Float32Vector.BASE64_PREFIX);

    if (!hasVectorParams && !hasFloat32Vectors) {
      return new ProcessedRequestBody(originalBytes, false);
    }

    ObjectNode json = (ObjectNode) MAPPER.readTree(originalBytes);
    boolean modified = false;

    if (hasVectorParams) {
      String target = getTarget(context);
      if (TARGET_CREATE_TABLE.equals(target) && vectorIndexes != null) {
        json.set("VectorIndexes", vectorIndexesToJson(vectorIndexes));
        modified = true;
      } else if (TARGET_UPDATE_TABLE.equals(target) && vectorIndexUpdates != null) {
        json.set("VectorIndexUpdates", vectorIndexUpdatesToJson(vectorIndexUpdates));
        modified = true;
      } else if (TARGET_QUERY.equals(target) && vectorSearch != null) {
        json.set("VectorSearch", vectorSearchToJson(vectorSearch));
        modified = true;
      }
    }

    if (hasFloat32Vectors) {
      modified |= replaceFloat32VectorInRequest(json);
    }

    if (!modified) {
      return new ProcessedRequestBody(originalBytes, false);
    }

    byte[] outBytes = MAPPER.writeValueAsBytes(json);
    return new ProcessedRequestBody(outBytes, true);
  }

  private static final class ProcessedRequestBody {
    private final byte[] bytes;
    private final boolean bodyModified;

    private ProcessedRequestBody(byte[] bytes, boolean bodyModified) {
      this.bytes = bytes;
      this.bodyModified = bodyModified;
    }

    private byte[] bytes() {
      return bytes;
    }

    private boolean bodyModified() {
      return bodyModified;
    }
  }

  // -------------------------------------------------------------------------
  // Response interception — extract extra JSON fields
  // -------------------------------------------------------------------------

  /**
   * Intercepts raw response bodies to:
   *
   * <ol>
   *   <li>Convert any {@code {"FLOAT32VECTOR": [...]}} attribute values to standard DynamoDB
   *       list-of-numbers ({@code L}) {@link AttributeValue}s so the SDK can parse the item
   *       normally and callers can access the floats via {@link AttributeValue#l()}. A fast
   *       substring scan is used to skip parsing when no {@code FLOAT32VECTOR} field is present.
   *   <li>Extract Alternator-specific response fields ({@code Scores} from Query, {@code
   *       VectorIndexes} from CreateTable/DescribeTable) into the per-request {@link
   *       VectorSearchResultHolder} when one was set by {@link VectorSearchSupport}.
   * </ol>
   */
  @Override
  public SdkHttpResponse modifyHttpResponse(
      Context.ModifyHttpResponse context, ExecutionAttributes executionAttributes) {
    SdkHttpResponse response = context.httpResponse();
    List<String> contentEncodings = getContentEncodings(response);
    executionAttributes.putAttribute(RESPONSE_CONTENT_ENCODINGS, contentEncodings);

    Optional<InputStream> bodyOpt = context.responseBody();
    if (bodyOpt.isPresent()) {
      VectorSearchResultHolder holder = executionAttributes.getAttribute(RESULT_HOLDER);
      try {
        ProcessedResponseBody processedBody =
            processResponseBody(
                readAllBytes(bodyOpt.get()), context.httpRequest(), holder, contentEncodings);
        executionAttributes.putAttribute(PROCESSED_RESPONSE_BODY_CACHE, processedBody);
        return processedBody.bodyModified()
            ? stripStaleResponseBodyHeaders(response, !contentEncodings.isEmpty())
            : response;
      } catch (IOException e) {
        throw new RuntimeException("Failed to process vector search fields in response body", e);
      }
    }

    boolean asyncBodyMayBeModified =
        executionAttributes.getAttribute(VECTOR_SEARCH) != null
            || responseMayContainAttributeValues(context.httpRequest());
    return contentEncodings.isEmpty() && !asyncBodyMayBeModified
        ? response
        : stripStaleResponseBodyHeaders(response, !contentEncodings.isEmpty());
  }

  @Override
  public Optional<InputStream> modifyHttpResponseContent(
      Context.ModifyHttpResponse context, ExecutionAttributes executionAttributes) {

    VectorSearchResultHolder holder = executionAttributes.getAttribute(RESULT_HOLDER);
    List<String> contentEncodings = executionAttributes.getAttribute(RESPONSE_CONTENT_ENCODINGS);

    Optional<InputStream> bodyOpt = context.responseBody();
    if (!bodyOpt.isPresent()) {
      return bodyOpt;
    }

    ProcessedResponseBody cachedBody =
        executionAttributes.getAttribute(PROCESSED_RESPONSE_BODY_CACHE);
    if (cachedBody != null) {
      return Optional.of(new ByteArrayInputStream(cachedBody.bytes()));
    }

    try {
      byte[] bytes = readAllBytes(bodyOpt.get());
      ProcessedResponseBody processedBody =
          processResponseBody(bytes, context.httpRequest(), holder, contentEncodings);
      return Optional.of(new ByteArrayInputStream(processedBody.bytes()));

    } catch (IOException e) {
      throw new RuntimeException("Failed to process vector search fields in response body", e);
    }
  }

  @Override
  public Optional<Publisher<ByteBuffer>> modifyAsyncHttpResponseContent(
      Context.ModifyHttpResponse context, ExecutionAttributes executionAttributes) {
    Optional<Publisher<ByteBuffer>> publisherOpt = context.responsePublisher();
    if (!publisherOpt.isPresent()) {
      return publisherOpt;
    }

    VectorSearchResultHolder holder = executionAttributes.getAttribute(RESULT_HOLDER);
    List<String> contentEncodings = executionAttributes.getAttribute(RESPONSE_CONTENT_ENCODINGS);
    return Optional.of(
        new TransformingResponsePublisher(
            publisherOpt.get(), context.httpRequest(), holder, contentEncodings));
  }

  private static ProcessedResponseBody processResponseBody(
      byte[] bytes,
      SdkHttpRequest httpRequest,
      VectorSearchResultHolder holder,
      List<String> contentEncodings)
      throws IOException {
    boolean modified = false;
    if (contentEncodings != null && !contentEncodings.isEmpty() && bytes.length > 0) {
      bytes = decompressResponseBody(bytes, contentEncodings);
      modified = true;
    }
    if (bytes.length == 0) {
      return new ProcessedResponseBody(bytes, modified);
    }

    // Quick scan: skip JSON parsing entirely when neither FLOAT32VECTOR nor a result holder
    // needing extraction are in play.
    boolean hasFloat32Vector = containsAsciiSubstring(bytes, "FLOAT32VECTOR");
    if (!hasFloat32Vector && holder == null) {
      return new ProcessedResponseBody(bytes, modified);
    }

    JsonNode json = MAPPER.readTree(bytes);
    boolean jsonModified = false;

    // Convert FLOAT32VECTOR -> standard L (list-of-numbers) so the SDK can unmarshal items
    // normally.
    if (hasFloat32Vector) {
      jsonModified = replaceFloat32VectorInResponse(json);
    }

    // Extract Scores / VectorIndexes for callers using VectorSearchSupport.
    if (holder != null) {
      String target = getTarget(httpRequest);
      if (TARGET_QUERY.equals(target)) {
        extractScores(json, holder);
      } else if (TARGET_CREATE_TABLE.equals(target) || TARGET_DESCRIBE_TABLE.equals(target)) {
        extractTableDescriptionVectorIndexes(json, holder);
      }
    }

    if (jsonModified) {
      bytes = MAPPER.writeValueAsBytes(json);
      modified = true;
    }
    return new ProcessedResponseBody(bytes, modified);
  }

  private static final class ProcessedResponseBody {
    private final byte[] bytes;
    private final boolean bodyModified;

    private ProcessedResponseBody(byte[] bytes, boolean bodyModified) {
      this.bytes = bytes;
      this.bodyModified = bodyModified;
    }

    private byte[] bytes() {
      return bytes;
    }

    private boolean bodyModified() {
      return bodyModified;
    }
  }

  private static final class TransformingResponsePublisher implements Publisher<ByteBuffer> {
    private final Publisher<ByteBuffer> delegate;
    private final SdkHttpRequest httpRequest;
    private final VectorSearchResultHolder holder;
    private final List<String> contentEncodings;

    private TransformingResponsePublisher(
        Publisher<ByteBuffer> delegate,
        SdkHttpRequest httpRequest,
        VectorSearchResultHolder holder,
        List<String> contentEncodings) {
      this.delegate = delegate;
      this.httpRequest = httpRequest;
      this.holder = holder;
      this.contentEncodings = contentEncodings;
    }

    @Override
    public void subscribe(Subscriber<? super ByteBuffer> subscriber) {
      delegate.subscribe(
          new BufferingResponseSubscriber(subscriber, httpRequest, holder, contentEncodings));
    }
  }

  private static final class BufferingResponseSubscriber implements Subscriber<ByteBuffer> {
    private final Subscriber<? super ByteBuffer> downstream;
    private final SdkHttpRequest httpRequest;
    private final VectorSearchResultHolder holder;
    private final List<String> contentEncodings;
    private final ByteArrayOutputStream body = new ByteArrayOutputStream();
    private final AtomicBoolean requested = new AtomicBoolean();
    private Subscription upstream;

    private BufferingResponseSubscriber(
        Subscriber<? super ByteBuffer> downstream,
        SdkHttpRequest httpRequest,
        VectorSearchResultHolder holder,
        List<String> contentEncodings) {
      this.downstream = downstream;
      this.httpRequest = httpRequest;
      this.holder = holder;
      this.contentEncodings = contentEncodings;
    }

    @Override
    public void onSubscribe(Subscription subscription) {
      this.upstream = subscription;
      downstream.onSubscribe(
          new Subscription() {
            @Override
            public void request(long n) {
              if (n <= 0) {
                upstream.cancel();
                downstream.onError(
                    new IllegalArgumentException(
                        "Reactive Streams request amount must be positive"));
                return;
              }
              if (requested.compareAndSet(false, true)) {
                upstream.request(Long.MAX_VALUE);
              }
            }

            @Override
            public void cancel() {
              upstream.cancel();
            }
          });
    }

    @Override
    public void onNext(ByteBuffer byteBuffer) {
      ByteBuffer copy = byteBuffer.asReadOnlyBuffer();
      byte[] chunk = new byte[copy.remaining()];
      copy.get(chunk);
      body.write(chunk, 0, chunk.length);
    }

    @Override
    public void onError(Throwable throwable) {
      downstream.onError(throwable);
    }

    @Override
    public void onComplete() {
      try {
        ProcessedResponseBody processedBody =
            processResponseBody(body.toByteArray(), httpRequest, holder, contentEncodings);
        byte[] outBytes = processedBody.bytes();
        if (outBytes.length > 0) {
          downstream.onNext(ByteBuffer.wrap(outBytes));
        }
        downstream.onComplete();
      } catch (IOException e) {
        downstream.onError(
            new RuntimeException("Failed to process vector search fields in response body", e));
      }
    }
  }

  // -------------------------------------------------------------------------
  // JSON serialisation helpers
  // -------------------------------------------------------------------------

  private static ArrayNode vectorIndexesToJson(List<VectorIndex> indexes) {
    ArrayNode arr = MAPPER.createArrayNode();
    for (VectorIndex vi : indexes) {
      arr.add(vectorIndexToJson(vi));
    }
    return arr;
  }

  private static ObjectNode vectorIndexToJson(VectorIndex vi) {
    ObjectNode node = MAPPER.createObjectNode();
    node.put("IndexName", vi.indexName());
    node.set("VectorAttribute", vectorAttributeToJson(vi.vectorAttribute()));
    if (vi.projection() != null) {
      node.set("Projection", projectionToJson(vi.projection()));
    }
    if (vi.similarityFunction() != null) {
      node.put("SimilarityFunction", vi.similarityFunction());
    }
    return node;
  }

  private static ObjectNode vectorAttributeToJson(VectorAttribute va) {
    ObjectNode node = MAPPER.createObjectNode();
    node.put("AttributeName", va.attributeName());
    node.put("Dimensions", va.dimensions());
    return node;
  }

  private static ObjectNode projectionToJson(Projection projection) {
    ObjectNode node = MAPPER.createObjectNode();
    if (projection.projectionType() != null) {
      node.put("ProjectionType", projection.projectionTypeAsString());
    }
    if (projection.nonKeyAttributes() != null && !projection.nonKeyAttributes().isEmpty()) {
      ArrayNode nka = node.putArray("NonKeyAttributes");
      projection.nonKeyAttributes().forEach(nka::add);
    }
    return node;
  }

  private static ArrayNode vectorIndexUpdatesToJson(List<VectorIndexUpdate> updates) {
    ArrayNode arr = MAPPER.createArrayNode();
    for (VectorIndexUpdate u : updates) {
      ObjectNode node = MAPPER.createObjectNode();
      if (u.create() != null) {
        node.set("Create", createVectorIndexActionToJson(u.create()));
      }
      if (u.delete() != null) {
        ObjectNode del = MAPPER.createObjectNode();
        del.put("IndexName", u.delete().indexName());
        node.set("Delete", del);
      }
      arr.add(node);
    }
    return arr;
  }

  private static ObjectNode createVectorIndexActionToJson(CreateVectorIndexAction action) {
    ObjectNode node = MAPPER.createObjectNode();
    node.put("IndexName", action.indexName());
    node.set("VectorAttribute", vectorAttributeToJson(action.vectorAttribute()));
    if (action.projection() != null) {
      node.set("Projection", projectionToJson(action.projection()));
    }
    if (action.similarityFunction() != null) {
      node.put("SimilarityFunction", action.similarityFunction());
    }
    return node;
  }

  private static ObjectNode vectorSearchToJson(VectorSearch vs) {
    ObjectNode node = MAPPER.createObjectNode();
    if (vs.queryVectorFloats() != null) {
      // Compact wire format: {"FLOAT32VECTOR": [1.0, 2.0, ...]}
      ObjectNode qv = MAPPER.createObjectNode();
      ArrayNode floats = qv.putArray("FLOAT32VECTOR");
      for (float f : vs.queryVectorFloats()) {
        floats.add(f);
      }
      node.set("QueryVector", qv);
    } else {
      node.set("QueryVector", attributeValueToJson(vs.queryVectorAttributeValue()));
    }
    if (vs.returnScores()) {
      node.put("ReturnScores", "SIMILARITY");
    }
    return node;
  }

  /**
   * Converts an {@link AttributeValue} to its DynamoDB JSON representation (e.g., {@code {"N":
   * "42"}}, {@code {"S": "hello"}}, {@code {"L": [...]}}).
   *
   * <p>This method is {@code public} so that callers can build raw DynamoDB-style JSON payloads
   * when working directly with the low-level HTTP API.
   */
  public static ObjectNode attributeValueToJson(AttributeValue av) {
    ObjectNode node = MAPPER.createObjectNode();
    if (av.s() != null) {
      node.put("S", av.s());
    } else if (av.n() != null) {
      node.put("N", av.n());
    } else if (Boolean.TRUE.equals(av.bool())) {
      node.put("BOOL", true);
    } else if (Boolean.FALSE.equals(av.bool())) {
      node.put("BOOL", false);
    } else if (Boolean.TRUE.equals(av.nul())) {
      node.put("NULL", true);
    } else if (av.b() != null) {
      // If this is a Float32Vector marker, emit the compact wire format directly.
      if (Float32Vector.hasFloat32VectorMagic(av.b().asByteArray())) {
        float[] floats = Float32Vector.bytesToFloats(av.b().asByteArray());
        ArrayNode f32v = node.putArray("FLOAT32VECTOR");
        for (float f : floats) {
          f32v.add(f);
        }
      } else {
        node.put("B", Base64.getEncoder().encodeToString(av.b().asByteArray()));
      }
    } else if (av.hasSs()) {
      ArrayNode arr = node.putArray("SS");
      av.ss().forEach(arr::add);
    } else if (av.hasNs()) {
      ArrayNode arr = node.putArray("NS");
      av.ns().forEach(arr::add);
    } else if (av.hasBs()) {
      ArrayNode arr = node.putArray("BS");
      av.bs().forEach(b -> arr.add(b.asByteArray()));
    } else if (av.hasL()) {
      ArrayNode arr = node.putArray("L");
      av.l().forEach(elem -> arr.add(attributeValueToJson(elem)));
    } else if (av.hasM()) {
      ObjectNode map = node.putObject("M");
      for (Map.Entry<String, AttributeValue> entry : av.m().entrySet()) {
        map.set(entry.getKey(), attributeValueToJson(entry.getValue()));
      }
    }
    return node;
  }

  // -------------------------------------------------------------------------
  // JSON deserialisation helpers
  // -------------------------------------------------------------------------

  private static void extractScores(JsonNode root, VectorSearchResultHolder holder) {
    JsonNode scoresNode = root.get("Scores");
    if (scoresNode != null && scoresNode.isArray()) {
      List<Double> scores = new ArrayList<>(scoresNode.size());
      for (JsonNode n : scoresNode) {
        scores.add(n.asDouble());
      }
      holder.setScores(scores);
    }
  }

  private static void extractTableDescriptionVectorIndexes(
      JsonNode root, VectorSearchResultHolder holder) {
    // CreateTable response wraps the table description under "TableDescription"
    JsonNode tableDesc = root.get("TableDescription");
    // DescribeTable response also uses "Table" in some SDK versions; try both
    if (tableDesc == null) {
      tableDesc = root.get("Table");
    }
    if (tableDesc == null) {
      // Might be a flat response (rare)
      tableDesc = root;
    }
    JsonNode viNode = tableDesc.get("VectorIndexes");
    if (viNode != null && viNode.isArray()) {
      List<VectorIndex> indexes = new ArrayList<>(viNode.size());
      for (JsonNode n : viNode) {
        indexes.add(parseVectorIndex((ObjectNode) n));
      }
      holder.setVectorIndexes(indexes);
    }
  }

  private static VectorIndex parseVectorIndex(ObjectNode node) {
    String indexName = node.get("IndexName").asText();
    ObjectNode vaNode = (ObjectNode) node.get("VectorAttribute");
    VectorAttribute va =
        VectorAttribute.builder()
            .attributeName(vaNode.get("AttributeName").asText())
            .dimensions(vaNode.get("Dimensions").asInt())
            .build();

    Projection projection = null;
    if (node.has("Projection")) {
      ObjectNode projNode = (ObjectNode) node.get("Projection");
      Projection.Builder projBuilder = Projection.builder();
      if (projNode.has("ProjectionType")) {
        projBuilder.projectionType(
            ProjectionType.fromValue(projNode.get("ProjectionType").asText()));
      }
      if (projNode.has("NonKeyAttributes")) {
        List<String> nka = new ArrayList<>();
        projNode.get("NonKeyAttributes").forEach(n -> nka.add(n.asText()));
        projBuilder.nonKeyAttributes(nka);
      }
      projection = projBuilder.build();
    }

    String similarityFunction =
        node.has("SimilarityFunction") ? node.get("SimilarityFunction").asText() : null;
    String indexStatus = node.has("IndexStatus") ? node.get("IndexStatus").asText() : null;
    Boolean backfilling = node.has("Backfilling") ? node.get("Backfilling").asBoolean() : null;

    return VectorIndex.builder()
        .indexName(indexName)
        .vectorAttribute(va)
        .projection(projection)
        .similarityFunction(similarityFunction)
        .indexStatus(indexStatus)
        .backfilling(backfilling)
        .build();
  }

  // -------------------------------------------------------------------------
  // FLOAT32VECTOR JSON replacement helpers
  // -------------------------------------------------------------------------

  /**
   * Recursively scans {@code node} for {@code {"B": "..."}} objects whose base64 value decodes to
   * bytes starting with the Float32Vector magic prefix, and replaces them in-place with {@code
   * {"FLOAT32VECTOR": [...]}}.
   *
   * @return {@code true} if any replacement was made
   */
  private static boolean replaceFloat32VectorInRequest(JsonNode node) {
    if (node.isObject()) {
      ObjectNode obj = (ObjectNode) node;
      JsonNode bField = obj.get("B");
      if (bField != null && bField.isTextual() && obj.size() == 1) {
        byte[] decoded;
        try {
          decoded = Base64.getDecoder().decode(bField.asText());
        } catch (IllegalArgumentException ignored) {
          decoded = new byte[0];
        }
        if (Float32Vector.hasFloat32VectorMagic(decoded)) {
          float[] floats = Float32Vector.bytesToFloats(decoded);
          obj.remove("B");
          ArrayNode arr = obj.putArray("FLOAT32VECTOR");
          for (float f : floats) {
            arr.add(f);
          }
          return true;
        }
      }
      // Not a Float32Vector marker node — recurse into children.
      boolean modified = false;
      for (JsonNode child : obj) {
        modified |= replaceFloat32VectorInRequest(child);
      }
      return modified;
    } else if (node.isArray()) {
      boolean modified = false;
      for (JsonNode child : node) {
        modified |= replaceFloat32VectorInRequest(child);
      }
      return modified;
    }
    return false;
  }

  /**
   * Recursively scans {@code node} for {@code {"FLOAT32VECTOR": [...]}} objects and replaces them
   * in-place with a standard DynamoDB list-of-numbers ({@code {"L": [{"N": "x"}, ...]}}) so the SDK
   * returns a plain {@code L}-typed {@link AttributeValue} that callers can use directly via {@link
   * software.amazon.awssdk.services.dynamodb.model.AttributeValue#l()}.
   *
   * @return {@code true} if any replacement was made
   */
  private static boolean replaceFloat32VectorInResponse(JsonNode node) {
    if (node.isObject()) {
      ObjectNode obj = (ObjectNode) node;
      JsonNode f32vField = obj.get("FLOAT32VECTOR");
      if (f32vField != null && f32vField.isArray() && obj.size() == 1) {
        ArrayNode lArray = MAPPER.createArrayNode();
        for (JsonNode numNode : f32vField) {
          ObjectNode n = MAPPER.createObjectNode();
          n.put("N", Float.toString((float) numNode.asDouble()));
          lArray.add(n);
        }
        obj.remove("FLOAT32VECTOR");
        obj.set("L", lArray);
        return true;
      }
      boolean modified = false;
      for (JsonNode child : obj) {
        modified |= replaceFloat32VectorInResponse(child);
      }
      return modified;
    } else if (node.isArray()) {
      boolean modified = false;
      for (JsonNode child : node) {
        modified |= replaceFloat32VectorInResponse(child);
      }
      return modified;
    }
    return false;
  }

  /**
   * Returns {@code true} if {@code haystack} contains {@code needle} as an ASCII substring. Used
   * for fast pre-screening of JSON bodies before committing to a full parse.
   */
  private static boolean containsAsciiSubstring(byte[] haystack, String needle) {
    byte[] needleBytes = needle.getBytes(StandardCharsets.US_ASCII);
    outer:
    for (int i = 0; i <= haystack.length - needleBytes.length; i++) {
      for (int j = 0; j < needleBytes.length; j++) {
        if (haystack[i + j] != needleBytes[j]) {
          continue outer;
        }
      }
      return true;
    }
    return false;
  }

  // -------------------------------------------------------------------------
  // I/O helpers
  // -------------------------------------------------------------------------

  private static String getTarget(Context.ModifyHttpRequest context) {
    return getTarget(context.httpRequest());
  }

  private static String getTarget(SdkHttpRequest httpRequest) {
    List<String> targets = httpRequest.headers().get("X-Amz-Target");
    if (targets == null || targets.isEmpty()) {
      return null;
    }
    return targets.get(0);
  }

  private static boolean responseMayContainAttributeValues(SdkHttpRequest httpRequest) {
    String target = getTarget(httpRequest);
    if (target == null) {
      return false;
    }
    switch (target) {
      case TARGET_BATCH_EXECUTE_STATEMENT:
      case TARGET_BATCH_GET_ITEM:
      case TARGET_BATCH_WRITE_ITEM:
      case TARGET_DELETE_ITEM:
      case TARGET_EXECUTE_STATEMENT:
      case TARGET_EXECUTE_TRANSACTION:
      case TARGET_GET_ITEM:
      case TARGET_PUT_ITEM:
      case TARGET_QUERY:
      case TARGET_SCAN:
      case TARGET_TRANSACT_GET_ITEMS:
      case TARGET_TRANSACT_WRITE_ITEMS:
      case TARGET_UPDATE_ITEM:
        return true;
      default:
        return false;
    }
  }

  private static byte[] readBytes(Optional<RequestBody> requestBodyOpt) throws IOException {
    if (!requestBodyOpt.isPresent()) {
      return null;
    }
    return readAllBytes(requestBodyOpt.get().contentStreamProvider().newStream());
  }

  private static List<String> getContentEncodings(SdkHttpResponse httpResponse) {
    List<String> encodings = new ArrayList<>();
    for (String headerValue : httpResponse.matchingHeaders(CONTENT_ENCODING_HEADER)) {
      for (String encoding : headerValue.split(",")) {
        String normalized = encoding.trim().toLowerCase(Locale.ROOT);
        if (normalized.isEmpty() || "identity".equals(normalized)) {
          continue;
        }
        if (!"gzip".equals(normalized)
            && !"x-gzip".equals(normalized)
            && !"deflate".equals(normalized)) {
          return new ArrayList<>();
        }
        encodings.add(normalized);
      }
    }
    return encodings;
  }

  private static SdkHttpResponse stripStaleResponseBodyHeaders(
      SdkHttpResponse httpResponse, boolean stripContentEncoding) {
    SdkHttpResponse.Builder builder = httpResponse.toBuilder();
    for (String headerName : httpResponse.headers().keySet()) {
      if (shouldStripResponseBodyHeader(headerName, stripContentEncoding)) {
        builder.removeHeader(headerName);
      }
    }
    return builder.build();
  }

  private static boolean shouldStripResponseBodyHeader(
      String headerName, boolean stripContentEncoding) {
    if (isHeader(headerName, CONTENT_LENGTH_HEADER)
        || isHeader(headerName, DYNAMODB_CRC32_HEADER)
        || isHeader(headerName, DYNAMODB_CRC32C_HEADER)) {
      return true;
    }
    if (headerName != null
        && headerName.toLowerCase(Locale.ROOT).startsWith(AWS_CHECKSUM_HEADER_PREFIX)) {
      return true;
    }
    return stripContentEncoding && isHeader(headerName, CONTENT_ENCODING_HEADER);
  }

  private static boolean isHeader(String actual, String expected) {
    return actual != null && actual.equalsIgnoreCase(expected);
  }

  private static byte[] decompressResponseBody(byte[] bytes, List<String> contentEncodings)
      throws IOException {
    byte[] decoded = bytes;
    for (int i = contentEncodings.size() - 1; i >= 0; i--) {
      decoded = decompressResponseBody(decoded, contentEncodings.get(i));
    }
    return decoded;
  }

  private static byte[] decompressResponseBody(byte[] bytes, String contentEncoding)
      throws IOException {
    if ("gzip".equals(contentEncoding) || "x-gzip".equals(contentEncoding)) {
      return readAllBytes(new GZIPInputStream(new ByteArrayInputStream(bytes)));
    }
    if ("deflate".equals(contentEncoding)) {
      return readAllBytes(new InflaterInputStream(new ByteArrayInputStream(bytes)));
    }
    return bytes;
  }

  private static byte[] readAllBytes(InputStream in) throws IOException {
    try {
      byte[] buf = new byte[4096];
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      int n;
      while ((n = in.read(buf)) != -1) {
        out.write(buf, 0, n);
      }
      return out.toByteArray();
    } finally {
      in.close();
    }
  }
}
