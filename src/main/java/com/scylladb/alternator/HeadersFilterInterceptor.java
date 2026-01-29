package com.scylladb.alternator;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.http.SdkHttpRequest;

/**
 * An execution interceptor that filters HTTP request headers to a specified whitelist.
 *
 * <p>This interceptor removes HTTP headers that are not in the configured whitelist, reducing
 * network traffic by eliminating headers that Alternator does not use. According to benchmarks,
 * this can reduce outgoing traffic by up to 56% depending on workload and encryption.
 *
 * <p>This is used internally by {@link AlternatorDynamoDbClient} and {@link
 * AlternatorDynamoDbAsyncClient} when header optimization is enabled via {@link AlternatorConfig}.
 *
 * @author dmitry.kropachev
 * @since 1.0.6
 */
public class HeadersFilterInterceptor implements ExecutionInterceptor {

  private final Set<String> allowedHeadersLowercase;

  /**
   * Creates a new headers filter interceptor.
   *
   * <p>The interceptor will preserve only headers whose names (case-insensitive) are in the
   * provided set. HTTP headers are case-insensitive per RFC 7230.
   *
   * @param allowedHeaders the set of header names to preserve (case-insensitive)
   */
  public HeadersFilterInterceptor(Set<String> allowedHeaders) {
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
  public SdkHttpRequest modifyHttpRequest(
      Context.ModifyHttpRequest context, ExecutionAttributes executionAttributes) {

    SdkHttpRequest httpRequest = context.httpRequest();
    SdkHttpRequest.Builder requestBuilder = httpRequest.toBuilder();

    // Clear all existing headers
    requestBuilder.clearHeaders();

    // Re-add only whitelisted headers
    Map<String, List<String>> originalHeaders = httpRequest.headers();
    for (Map.Entry<String, List<String>> entry : originalHeaders.entrySet()) {
      String headerName = entry.getKey();
      if (allowedHeadersLowercase.contains(headerName.toLowerCase(Locale.ROOT))) {
        for (String value : entry.getValue()) {
          requestBuilder.appendHeader(headerName, value);
        }
      }
    }

    return requestBuilder.build();
  }
}
