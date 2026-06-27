package com.scylladb.alternator;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.InflaterInputStream;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttribute;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.SdkHttpResponse;

/** Adds HTTP response compression negotiation and decompresses gzip/deflate responses. */
class ResponseCompressionInterceptor implements ExecutionInterceptor {

  static final String ACCEPT_ENCODING =
      ResponseCompressionAlgorithm.acceptEncoding(
          ResponseCompressionAlgorithm.supportedAlgorithms());

  private static final String ACCEPT_ENCODING_HEADER = "Accept-Encoding";
  private static final String CONTENT_ENCODING_HEADER = "Content-Encoding";
  private static final String CONTENT_LENGTH_HEADER = "Content-Length";
  private static final ExecutionAttribute<ResponseCompressionAlgorithm> RESPONSE_ENCODING =
      new ExecutionAttribute<>("ResponseCompressionInterceptor.responseEncoding");
  private final Set<ResponseCompressionAlgorithm> algorithmSet;
  private final String acceptEncoding;

  ResponseCompressionInterceptor() {
    this(ResponseCompressionAlgorithm.supportedAlgorithms());
  }

  ResponseCompressionInterceptor(Collection<ResponseCompressionAlgorithm> algorithms) {
    List<ResponseCompressionAlgorithm> validatedAlgorithms =
        ResponseCompressionAlgorithm.validatedList(algorithms);
    this.algorithmSet = new HashSet<>(validatedAlgorithms);
    this.acceptEncoding = ResponseCompressionAlgorithm.acceptEncoding(validatedAlgorithms);
  }

  @Override
  public SdkHttpRequest modifyHttpRequest(
      Context.ModifyHttpRequest context, ExecutionAttributes executionAttributes) {
    return withAcceptEncoding(context.httpRequest());
  }

  @Override
  public SdkHttpResponse modifyHttpResponse(
      Context.ModifyHttpResponse context, ExecutionAttributes executionAttributes) {
    Optional<ResponseCompressionAlgorithm> encoding = responseEncoding(context.httpResponse());
    encoding.ifPresent(value -> executionAttributes.putAttribute(RESPONSE_ENCODING, value));
    return encoding.isPresent()
        ? stripCompressionHeaders(context.httpResponse())
        : context.httpResponse();
  }

  @Override
  public Optional<InputStream> modifyHttpResponseContent(
      Context.ModifyHttpResponse context, ExecutionAttributes executionAttributes) {
    ResponseCompressionAlgorithm encoding = executionAttributes.getAttribute(RESPONSE_ENCODING);
    if (encoding == null || !context.responseBody().isPresent()) {
      return Optional.empty();
    }

    try {
      return Optional.of(decompress(context.responseBody().get(), encoding));
    } catch (IOException e) {
      throw SdkClientException.create(
          "Failed to initialize " + encoding.contentEncoding() + " response decompression", e);
    }
  }

  @Override
  public Optional<Publisher<ByteBuffer>> modifyAsyncHttpResponseContent(
      Context.ModifyHttpResponse context, ExecutionAttributes executionAttributes) {
    ResponseCompressionAlgorithm encoding = executionAttributes.getAttribute(RESPONSE_ENCODING);
    if (encoding == null || !context.responsePublisher().isPresent()) {
      return Optional.empty();
    }
    return Optional.of(new DecompressingPublisher(context.responsePublisher().get(), encoding));
  }

  private SdkHttpRequest withAcceptEncoding(SdkHttpRequest request) {
    return request.toBuilder().putHeader(ACCEPT_ENCODING_HEADER, acceptEncoding).build();
  }

  private Optional<ResponseCompressionAlgorithm> responseEncoding(SdkHttpResponse response) {
    List<String> values = response.matchingHeaders(CONTENT_ENCODING_HEADER);
    List<String> tokens = new ArrayList<>();
    for (String value : values) {
      for (String token : value.split(",")) {
        tokens.add(token);
      }
    }
    if (tokens.size() == 1) {
      return ResponseCompressionAlgorithm.fromContentEncoding(tokens.get(0))
          .filter(algorithmSet::contains);
    }
    return Optional.empty();
  }

  private static SdkHttpResponse stripCompressionHeaders(SdkHttpResponse response) {
    SdkHttpResponse.Builder builder = response.toBuilder();
    for (String headerName : response.headers().keySet()) {
      if (isHeader(headerName, CONTENT_ENCODING_HEADER)
          || isHeader(headerName, CONTENT_LENGTH_HEADER)) {
        builder.removeHeader(headerName);
      }
    }
    return builder.build();
  }

  private static InputStream decompress(
      InputStream compressed, ResponseCompressionAlgorithm encoding) throws IOException {
    switch (encoding) {
      case GZIP:
        return new GZIPInputStream(compressed);
      case DEFLATE:
        return new InflaterInputStream(compressed);
      default:
        throw new IllegalArgumentException("Unsupported response encoding: " + encoding);
    }
  }

  private static byte[] decompress(byte[] compressed, ResponseCompressionAlgorithm encoding)
      throws IOException {
    try (InputStream input = decompress(new ByteArrayInputStream(compressed), encoding)) {
      ByteArrayOutputStream output = new ByteArrayOutputStream();
      byte[] buffer = new byte[4096];
      int read;
      while ((read = input.read(buffer)) != -1) {
        output.write(buffer, 0, read);
      }
      return output.toByteArray();
    }
  }

  private static boolean isHeader(String actual, String expected) {
    return actual != null && actual.equalsIgnoreCase(expected);
  }

  private static final class DecompressingPublisher implements Publisher<ByteBuffer> {
    private final Publisher<ByteBuffer> delegate;
    private final ResponseCompressionAlgorithm encoding;

    private DecompressingPublisher(
        Publisher<ByteBuffer> delegate, ResponseCompressionAlgorithm encoding) {
      this.delegate = delegate;
      this.encoding = encoding;
    }

    @Override
    public void subscribe(Subscriber<? super ByteBuffer> subscriber) {
      delegate.subscribe(new DecompressingSubscriber(subscriber, encoding));
    }
  }

  private static final class DecompressingSubscriber
      implements Subscriber<ByteBuffer>, Subscription {
    private final Subscriber<? super ByteBuffer> downstream;
    private final ResponseCompressionAlgorithm encoding;
    private final ByteArrayOutputStream compressedBytes = new ByteArrayOutputStream();
    private Subscription upstream;
    private boolean upstreamRequested;
    private boolean completed;
    private boolean cancelled;
    private boolean emitted;
    private long demand;
    private byte[] decompressedBytes;

    private DecompressingSubscriber(
        Subscriber<? super ByteBuffer> downstream, ResponseCompressionAlgorithm encoding) {
      this.downstream = downstream;
      this.encoding = encoding;
    }

    @Override
    public void onSubscribe(Subscription subscription) {
      this.upstream = subscription;
      downstream.onSubscribe(this);
    }

    @Override
    public void onNext(ByteBuffer byteBuffer) {
      if (isCancelled()) {
        return;
      }
      ByteBuffer copy = byteBuffer.asReadOnlyBuffer();
      byte[] bytes = new byte[copy.remaining()];
      copy.get(bytes);
      compressedBytes.write(bytes, 0, bytes.length);
    }

    @Override
    public void onError(Throwable throwable) {
      if (!isCancelled()) {
        downstream.onError(throwable);
      }
    }

    @Override
    public void onComplete() {
      byte[] decompressed;
      try {
        decompressed = decompress(compressedBytes.toByteArray(), encoding);
      } catch (IOException e) {
        if (!isCancelled()) {
          downstream.onError(
              SdkClientException.create(
                  "Failed to decompress " + encoding.contentEncoding() + " response", e));
        }
        return;
      }
      synchronized (this) {
        decompressedBytes = decompressed;
        completed = true;
      }
      emitIfReady();
    }

    @Override
    public void request(long n) {
      if (n <= 0) {
        cancel();
        downstream.onError(new IllegalArgumentException("Demand must be positive"));
        return;
      }

      boolean shouldRequestUpstream = false;
      synchronized (this) {
        demand = saturatedAdd(demand, n);
        if (!upstreamRequested) {
          upstreamRequested = true;
          shouldRequestUpstream = true;
        }
      }
      if (shouldRequestUpstream) {
        upstream.request(Long.MAX_VALUE);
      }
      emitIfReady();
    }

    @Override
    public void cancel() {
      Subscription subscription;
      synchronized (this) {
        cancelled = true;
        subscription = upstream;
      }
      if (subscription != null) {
        subscription.cancel();
      }
    }

    private void emitIfReady() {
      ByteBuffer item = null;
      synchronized (this) {
        if (!cancelled && completed && !emitted && demand > 0) {
          emitted = true;
          item = ByteBuffer.wrap(decompressedBytes);
        }
      }
      if (item != null) {
        downstream.onNext(item);
        if (!isCancelled()) {
          downstream.onComplete();
        }
      }
    }

    private synchronized boolean isCancelled() {
      return cancelled;
    }

    private static long saturatedAdd(long left, long right) {
      long result = left + right;
      return result < 0 ? Long.MAX_VALUE : result;
    }
  }
}
