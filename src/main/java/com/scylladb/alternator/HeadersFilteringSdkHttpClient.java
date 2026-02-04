package com.scylladb.alternator;

import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpRequest;

/**
 * A wrapper around SdkHttpClient that filters HTTP headers to a specified whitelist.
 *
 * <p>This wrapper intercepts requests before they are executed and removes headers not in the
 * configured whitelist. This runs after all AWS SDK processing, ensuring complete header filtering
 * including SDK-internal headers like User-Agent and amz-sdk-request.
 *
 * @author dmitry.kropachev
 * @since 2.0.0
 */
public class HeadersFilteringSdkHttpClient implements SdkHttpClient {

  private final SdkHttpClient delegate;
  private final Set<String> allowedHeadersLowercase;

  /**
   * Creates a new headers filtering HTTP client wrapper.
   *
   * @param delegate the underlying SdkHttpClient to wrap
   * @param allowedHeaders the set of header names to preserve (case-insensitive)
   */
  public HeadersFilteringSdkHttpClient(SdkHttpClient delegate, Set<String> allowedHeaders) {
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
  public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
    // Filter headers from the request
    SdkHttpRequest originalHttpRequest = request.httpRequest();
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
    HttpExecuteRequest filteredExecuteRequest =
        HttpExecuteRequest.builder()
            .request(filteredRequestBuilder.build())
            .contentStreamProvider(request.contentStreamProvider().orElse(null))
            .build();

    return delegate.prepareRequest(filteredExecuteRequest);
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
