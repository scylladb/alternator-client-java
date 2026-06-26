package com.scylladb.alternator;

import java.util.concurrent.CompletableFuture;
import java.util.function.UnaryOperator;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.async.AsyncExecuteRequest;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;

/** A wrapper around SdkAsyncHttpClient that rewrites or removes the User-Agent header. */
class UserAgentSdkAsyncHttpClient implements SdkAsyncHttpClient {
  private final SdkAsyncHttpClient delegate;
  private final UnaryOperator<String> userAgentTransformer;

  UserAgentSdkAsyncHttpClient(
      SdkAsyncHttpClient delegate, UnaryOperator<String> userAgentTransformer) {
    this.delegate = delegate;
    this.userAgentTransformer = userAgentTransformer;
  }

  @Override
  public CompletableFuture<Void> execute(AsyncExecuteRequest request) {
    SdkHttpRequest transformedRequest =
        AlternatorUserAgent.transform(request.request(), userAgentTransformer);

    AsyncExecuteRequest transformedExecuteRequest =
        AsyncExecuteRequest.builder()
            .request(transformedRequest)
            .requestContentPublisher(request.requestContentPublisher())
            .responseHandler(request.responseHandler())
            .fullDuplex(request.fullDuplex())
            .metricCollector(request.metricCollector().orElse(null))
            .build();

    return delegate.execute(transformedExecuteRequest);
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
