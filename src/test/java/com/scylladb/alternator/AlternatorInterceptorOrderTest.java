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
package com.scylladb.alternator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.scylladb.alternator.vectorsearch.VectorSearch;
import com.scylladb.alternator.vectorsearch.VectorSearchInterceptor;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.GZIPOutputStream;
import org.junit.Test;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptorChain;
import software.amazon.awssdk.core.interceptor.InterceptorContext;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.dynamodb.model.ListTablesRequest;

/** Tests ordering between required Alternator interceptors and caller-provided interceptors. */
public class AlternatorInterceptorOrderTest {

  private static final URI SEED_URI = URI.create("http://127.0.0.1:9999");

  @Test
  public void syncBuilderOrdersVectorPhasesAroundCallerInterceptors() {
    ExecutionInterceptor firstCallerInterceptor = new GzipRequestInterceptor(1);
    ExecutionInterceptor secondCallerInterceptor = new ExecutionInterceptor() {};

    AlternatorDynamoDbClientWrapper wrapper =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(SEED_URI)
            .withResponseCompression(ResponseCompressionAlgorithm.GZIP)
            .overrideConfiguration(
                c ->
                    c.addExecutionInterceptor(firstCallerInterceptor)
                        .addExecutionInterceptor(VectorSearchInterceptor.INSTANCE)
                        .addExecutionInterceptor(secondCallerInterceptor))
            .buildWithAlternatorAPI();
    try {
      List<ExecutionInterceptor> interceptors =
          wrapper
              .getClient()
              .serviceClientConfiguration()
              .overrideConfiguration()
              .executionInterceptors();

      int vectorRequestIndex = interceptors.indexOf(VectorSearchInterceptorPhases.REQUEST);
      int vectorResponseIndex = interceptors.indexOf(VectorSearchInterceptorPhases.RESPONSE);
      int responseCompressionIndex = indexOf(interceptors, ResponseCompressionInterceptor.class);
      assertTrue(vectorRequestIndex >= 0);
      assertTrue(vectorResponseIndex >= 0);
      assertTrue(responseCompressionIndex >= 0);
      assertEquals(vectorRequestIndex + 1, interceptors.indexOf(firstCallerInterceptor));
      assertEquals(vectorRequestIndex + 2, interceptors.indexOf(secondCallerInterceptor));
      assertTrue(interceptors.indexOf(secondCallerInterceptor) < responseCompressionIndex);
      assertTrue(responseCompressionIndex < vectorResponseIndex);
      assertFalse(interceptors.contains(VectorSearchInterceptor.INSTANCE));
    } finally {
      wrapper.close();
    }
  }

  @Test
  public void asyncBuilderOrdersVectorPhasesAroundCallerInterceptors() {
    ExecutionInterceptor firstCallerInterceptor = new GzipRequestInterceptor(1);
    ExecutionInterceptor secondCallerInterceptor = new ExecutionInterceptor() {};

    AlternatorDynamoDbAsyncClientWrapper wrapper =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(SEED_URI)
            .withResponseCompression(ResponseCompressionAlgorithm.GZIP)
            .overrideConfiguration(
                c ->
                    c.addExecutionInterceptor(firstCallerInterceptor)
                        .addExecutionInterceptor(VectorSearchInterceptor.INSTANCE)
                        .addExecutionInterceptor(secondCallerInterceptor))
            .buildWithAlternatorAPI();
    try {
      List<ExecutionInterceptor> interceptors =
          wrapper
              .getClient()
              .serviceClientConfiguration()
              .overrideConfiguration()
              .executionInterceptors();

      int vectorRequestIndex = interceptors.indexOf(VectorSearchInterceptorPhases.REQUEST);
      int vectorResponseIndex = interceptors.indexOf(VectorSearchInterceptorPhases.RESPONSE);
      int responseCompressionIndex = indexOf(interceptors, ResponseCompressionInterceptor.class);
      assertTrue(vectorRequestIndex >= 0);
      assertTrue(vectorResponseIndex >= 0);
      assertTrue(responseCompressionIndex >= 0);
      assertEquals(vectorRequestIndex + 1, interceptors.indexOf(firstCallerInterceptor));
      assertEquals(vectorRequestIndex + 2, interceptors.indexOf(secondCallerInterceptor));
      assertTrue(interceptors.indexOf(secondCallerInterceptor) < responseCompressionIndex);
      assertTrue(responseCompressionIndex < vectorResponseIndex);
      assertFalse(interceptors.contains(VectorSearchInterceptor.INSTANCE));
    } finally {
      wrapper.close();
    }
  }

  @Test
  public void callerSeesProcessedRequestAndResponseBodies() throws Exception {
    AtomicReference<String> callerRequestBody = new AtomicReference<>();
    AtomicReference<String> callerResponseBody = new AtomicReference<>();
    ExecutionInterceptor callerInterceptor =
        new ExecutionInterceptor() {
          @Override
          public Optional<RequestBody> modifyHttpContent(
              Context.ModifyHttpRequest context, ExecutionAttributes executionAttributes) {
            byte[] body = readAll(context.requestBody().get().contentStreamProvider().newStream());
            callerRequestBody.set(new String(body, StandardCharsets.UTF_8));
            return Optional.of(RequestBody.fromBytes(body));
          }

          @Override
          public Optional<InputStream> modifyHttpResponseContent(
              Context.ModifyHttpResponse context, ExecutionAttributes executionAttributes) {
            byte[] body = readAll(context.responseBody().get());
            callerResponseBody.set(new String(body, StandardCharsets.UTF_8));
            return Optional.of(new ByteArrayInputStream(body));
          }
        };

    ExecutionInterceptorChain chain =
        new ExecutionInterceptorChain(
            Arrays.asList(
                VectorSearchInterceptorPhases.REQUEST,
                callerInterceptor,
                new ResponseCompressionInterceptor(),
                VectorSearchInterceptorPhases.RESPONSE));
    ExecutionAttributes attributes = new ExecutionAttributes();
    attributes.putAttribute(
        VectorSearchInterceptor.VECTOR_SEARCH,
        VectorSearch.builder().queryVector(1.0f, 2.0f).build());
    SdkHttpRequest httpRequest =
        SdkHttpRequest.builder()
            .protocol("http")
            .host("localhost")
            .method(SdkHttpMethod.POST)
            .encodedPath("/")
            .putHeader("X-Amz-Target", "DynamoDB_20120810.Query")
            .build();

    chain.modifyHttpRequestAndHttpContent(
        InterceptorContext.builder()
            .request(ListTablesRequest.builder().build())
            .httpRequest(httpRequest)
            .requestBody(RequestBody.fromString("{\"TableName\":\"items\"}"))
            .build(),
        attributes);

    assertTrue(callerRequestBody.get().contains("\"VectorSearch\""));

    byte[] compressedResponse = gzip("{\"Items\":[{\"embedding\":{\"FLOAT32VECTOR\":[1.0,2.0]}}]}");
    InterceptorContext response =
        chain.modifyHttpResponse(
            InterceptorContext.builder()
                .request(ListTablesRequest.builder().build())
                .httpRequest(httpRequest)
                .httpResponse(
                    SdkHttpResponse.builder()
                        .statusCode(200)
                        .putHeader("Content-Encoding", "gzip")
                        .build())
                .responseBody(new ByteArrayInputStream(compressedResponse))
                .build(),
            attributes);

    assertTrue(callerResponseBody.get().contains("\"B\""));
    assertFalse(callerResponseBody.get().contains("FLOAT32VECTOR"));
    assertFalse(response.httpResponse().firstMatchingHeader("Content-Encoding").isPresent());
  }

  private static int indexOf(
      List<ExecutionInterceptor> interceptors, Class<? extends ExecutionInterceptor> type) {
    for (int i = 0; i < interceptors.size(); i++) {
      if (type.isInstance(interceptors.get(i))) {
        return i;
      }
    }
    return -1;
  }

  private static byte[] gzip(String value) throws IOException {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    try (GZIPOutputStream gzip = new GZIPOutputStream(output)) {
      gzip.write(value.getBytes(StandardCharsets.UTF_8));
    }
    return output.toByteArray();
  }

  private static byte[] readAll(InputStream input) {
    try {
      ByteArrayOutputStream output = new ByteArrayOutputStream();
      byte[] buffer = new byte[256];
      int count;
      while ((count = input.read(buffer)) != -1) {
        output.write(buffer, 0, count);
      }
      return output.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
