package com.scylladb.alternator;

import java.util.function.UnaryOperator;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpRequest;

/** A wrapper around SdkHttpClient that rewrites or removes the User-Agent header. */
class UserAgentSdkHttpClient implements SdkHttpClient {
  private final SdkHttpClient delegate;
  private final UnaryOperator<String> userAgentTransformer;

  UserAgentSdkHttpClient(SdkHttpClient delegate, UnaryOperator<String> userAgentTransformer) {
    this.delegate = delegate;
    this.userAgentTransformer = userAgentTransformer;
  }

  @Override
  public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
    SdkHttpRequest transformedRequest =
        AlternatorUserAgent.transform(request.httpRequest(), userAgentTransformer);

    HttpExecuteRequest transformedExecuteRequest =
        HttpExecuteRequest.builder()
            .request(transformedRequest)
            .contentStreamProvider(request.contentStreamProvider().orElse(null))
            .build();

    return delegate.prepareRequest(transformedExecuteRequest);
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
