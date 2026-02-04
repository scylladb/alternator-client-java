package com.scylladb.alternator;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import java.util.zip.GZIPOutputStream;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttribute;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.SdkHttpRequest;

/**
 * An execution interceptor that compresses HTTP request bodies using GZIP.
 *
 * <p>This interceptor compresses request bodies that exceed the configured minimum size threshold
 * and adds the {@code Content-Encoding: gzip} header to indicate compression.
 *
 * <p>This is used internally by {@link AlternatorDynamoDbClient} and {@link
 * AlternatorDynamoDbAsyncClient} when compression is enabled via {@link AlternatorConfig}.
 *
 * @author dmitry.kropachev
 * @since 2.0.0
 */
public class GzipRequestInterceptor implements ExecutionInterceptor {

  private static final ExecutionAttribute<byte[]> ORIGINAL_BODY_BYTES =
      new ExecutionAttribute<>("GzipRequestInterceptor.originalBodyBytes");
  private static final ExecutionAttribute<Boolean> SHOULD_COMPRESS =
      new ExecutionAttribute<>("GzipRequestInterceptor.shouldCompress");

  private final int minCompressionSizeBytes;

  /**
   * Creates a new GZIP request interceptor.
   *
   * @param minCompressionSizeBytes minimum request body size in bytes to trigger compression
   */
  public GzipRequestInterceptor(int minCompressionSizeBytes) {
    this.minCompressionSizeBytes = minCompressionSizeBytes;
  }

  @Override
  public SdkHttpRequest modifyHttpRequest(
      Context.ModifyHttpRequest context, ExecutionAttributes executionAttributes) {

    Optional<RequestBody> requestBody = context.requestBody();
    if (!requestBody.isPresent()) {
      executionAttributes.putAttribute(SHOULD_COMPRESS, false);
      return context.httpRequest();
    }

    try {
      // Read the original content and cache it for modifyHttpContent
      byte[] originalContent = readAllBytes(requestBody.get().contentStreamProvider().newStream());
      executionAttributes.putAttribute(ORIGINAL_BODY_BYTES, originalContent);

      // Check if we should compress based on size
      boolean shouldCompress = originalContent.length >= minCompressionSizeBytes;
      executionAttributes.putAttribute(SHOULD_COMPRESS, shouldCompress);

      if (shouldCompress) {
        // Add Content-Encoding header
        return context.httpRequest().toBuilder().putHeader("Content-Encoding", "gzip").build();
      }

      return context.httpRequest();

    } catch (IOException e) {
      executionAttributes.putAttribute(SHOULD_COMPRESS, false);
      return context.httpRequest();
    }
  }

  @Override
  public Optional<RequestBody> modifyHttpContent(
      Context.ModifyHttpRequest context, ExecutionAttributes executionAttributes) {

    Boolean shouldCompress = executionAttributes.getAttribute(SHOULD_COMPRESS);
    if (shouldCompress == null || !shouldCompress) {
      // Return original body from cached bytes if available, otherwise return as-is
      byte[] cachedBytes = executionAttributes.getAttribute(ORIGINAL_BODY_BYTES);
      if (cachedBytes != null) {
        return Optional.of(RequestBody.fromBytes(cachedBytes));
      }
      return context.requestBody();
    }

    byte[] originalContent = executionAttributes.getAttribute(ORIGINAL_BODY_BYTES);
    if (originalContent == null) {
      return context.requestBody();
    }

    try {
      // Compress the content
      byte[] compressedContent = gzipCompress(originalContent);
      return Optional.of(RequestBody.fromBytes(compressedContent));

    } catch (IOException e) {
      // If compression fails, return original content
      return Optional.of(RequestBody.fromBytes(originalContent));
    }
  }

  private byte[] readAllBytes(InputStream is) throws IOException {
    try {
      ByteArrayOutputStream buffer = new ByteArrayOutputStream();
      byte[] data = new byte[4096];
      int bytesRead;
      while ((bytesRead = is.read(data, 0, data.length)) != -1) {
        buffer.write(data, 0, bytesRead);
      }
      return buffer.toByteArray();
    } finally {
      is.close();
    }
  }

  private byte[] gzipCompress(byte[] data) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream(data.length);
    try (GZIPOutputStream gzip = new GZIPOutputStream(bos)) {
      gzip.write(data);
    }
    return bos.toByteArray();
  }
}
