package com.scylladb.alternator;

import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.async.AsyncExecuteRequest;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;

/**
 * A wrapper around SdkAsyncHttpClient that filters HTTP headers to a specified whitelist.
 *
 * <p>This wrapper intercepts requests before they are executed and removes headers not in the
 * configured whitelist. This runs after all AWS SDK processing, ensuring complete header filtering
 * including SDK-internal headers like User-Agent and amz-sdk-request.
 *
 * @author dmitry.kropachev
 * @since 2.0.0
 */
public class HeadersFilteringSdkAsyncHttpClient implements SdkAsyncHttpClient {

  private final SdkAsyncHttpClient delegate;
  private final Set<String> allowedHeadersLowercase;

  /**
   * Creates a new headers filtering async HTTP client wrapper.
   *
   * @param delegate the underlying SdkAsyncHttpClient to wrap
   * @param allowedHeaders the set of header names to preserve (case-insensitive)
   */
  public HeadersFilteringSdkAsyncHttpClient(
      SdkAsyncHttpClient delegate, Set<String> allowedHeaders) {
    this.delegate = delegate;
    this.allowedHeadersLowercase = new HashSet<>();
    if (allowedHeaders != null) {
      for (String header : allowedHeaders) {
        if (header != null && !header.isEmpty()) {
          this.allowedHeadersLowercase.add(header.toLowerCase(Locale.ROOT));
        }
      }
    }
  }

  @Override
  public CompletableFuture<Void> execute(AsyncExecuteRequest request) {
    // Filter headers from the request
    SdkHttpRequest originalHttpRequest = request.request();
    SdkHttpRequest.Builder filteredRequestBuilder = originalHttpRequest.toBuilder();

    // Clear all headers and re-add only whitelisted ones
    filteredRequestBuilder.clearHeaders();
    originalHttpRequest
        .headers()
        .forEach(
            (headerName, values) -> {
              if (allowedHeadersLowercase.contains(headerName.toLowerCase(Locale.ROOT))) {
                values.forEach(value -> filteredRequestBuilder.appendHeader(headerName, value));
              }
            });

    // Create new execute request with filtered headers
    AsyncExecuteRequest filteredExecuteRequest =
        AsyncExecuteRequest.builder()
            .request(filteredRequestBuilder.build())
            .requestContentPublisher(request.requestContentPublisher())
            .responseHandler(request.responseHandler())
            .fullDuplex(request.fullDuplex())
            .metricCollector(request.metricCollector().orElse(null))
            .build();

    return delegate.execute(filteredExecuteRequest);
  }

  @Override
  public void close() {
    delegate.close();
  }

  @Override
  public String clientName() {
    return delegate.clientName();
  }
}
