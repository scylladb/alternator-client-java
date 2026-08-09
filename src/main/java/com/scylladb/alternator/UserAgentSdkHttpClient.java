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

import com.scylladb.alternator.internal.DnsFallbackSdkHttpClient;
import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.function.UnaryOperator;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpRequest;

/** A wrapper around SdkHttpClient that rewrites or removes the User-Agent header. */
class UserAgentSdkHttpClient implements SdkHttpClient, DnsFallbackSdkHttpClient {
  private final SdkHttpClient delegate;
  private final UnaryOperator<String> userAgentTransformer;

  UserAgentSdkHttpClient(SdkHttpClient delegate, UnaryOperator<String> userAgentTransformer) {
    this.delegate = delegate;
    this.userAgentTransformer = userAgentTransformer;
  }

  @Override
  public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
    return delegate.prepareRequest(transform(request));
  }

  @Override
  public boolean supportsDnsFallback(String scheme) {
    return delegate instanceof DnsFallbackSdkHttpClient
        && ((DnsFallbackSdkHttpClient) delegate).supportsDnsFallback(scheme);
  }

  @Override
  public List<InetAddress> resolve(String hostname) throws IOException {
    return dnsFallbackDelegate().resolve(hostname);
  }

  @Override
  public ExecutableHttpRequest prepareRequestForAddress(
      HttpExecuteRequest request, InetAddress address) {
    return dnsFallbackDelegate().prepareRequestForAddress(transform(request), address);
  }

  private HttpExecuteRequest transform(HttpExecuteRequest request) {
    SdkHttpRequest transformedRequest =
        AlternatorUserAgent.transform(request.httpRequest(), userAgentTransformer);

    return HttpExecuteRequest.builder()
        .request(transformedRequest)
        .contentStreamProvider(request.contentStreamProvider().orElse(null))
        .build();
  }

  private DnsFallbackSdkHttpClient dnsFallbackDelegate() {
    if (!(delegate instanceof DnsFallbackSdkHttpClient)) {
      throw new UnsupportedOperationException("Delegate does not support DNS address fallback");
    }
    return (DnsFallbackSdkHttpClient) delegate;
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
