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
package com.scylladb.alternator.internal;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpRequest;

/**
 * Address-aware wrapper for transports that can safely use a numeric HTTP request endpoint.
 *
 * <p>HTTPS is intentionally unsupported. The public {@code AwsCrtHttpClient.Builder} API does not
 * expose a DNS resolver, a connection-address override, or the underlying CRT TLS server-name
 * option. Replacing the URI hostname would therefore change SNI and certificate verification to the
 * numeric address. Implementing the required address/server-name split would require unstable AWS
 * SDK internal APIs or a separate HTTP transport implementation.
 */
final class IpDnsFallbackSdkHttpClient implements DnsFallbackSdkHttpClient {

  interface Resolver {
    List<InetAddress> resolve(String hostname) throws IOException;
  }

  private final SdkHttpClient delegate;
  private final Resolver resolver;

  IpDnsFallbackSdkHttpClient(SdkHttpClient delegate) {
    this(delegate, hostname -> Arrays.asList(InetAddress.getAllByName(hostname)));
  }

  IpDnsFallbackSdkHttpClient(SdkHttpClient delegate, Resolver resolver) {
    this.delegate = delegate;
    this.resolver = resolver;
  }

  @Override
  public boolean supportsDnsFallback(String scheme) {
    // A logical Host header is sufficient for plain HTTP. It cannot restore TLS identity.
    return "http".equalsIgnoreCase(scheme);
  }

  @Override
  public List<InetAddress> resolve(String hostname) throws IOException {
    return resolver.resolve(hostname);
  }

  @Override
  public ExecutableHttpRequest prepareRequestForAddress(
      HttpExecuteRequest request, InetAddress address) {
    SdkHttpRequest logicalRequest = request.httpRequest();
    SdkHttpRequest addressedRequest = logicalRequest.toBuilder().host(addressHost(address)).build();
    HttpExecuteRequest addressedExecuteRequest =
        HttpExecuteRequest.builder()
            .request(addressedRequest)
            .contentStreamProvider(request.contentStreamProvider().orElse(null))
            .build();
    return delegate.prepareRequest(addressedExecuteRequest);
  }

  @Override
  public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
    return delegate.prepareRequest(request);
  }

  @Override
  public void close() {
    delegate.close();
  }

  @Override
  public String clientName() {
    return delegate.clientName();
  }

  private static String addressHost(InetAddress address) {
    String host = address.getHostAddress();
    return host.indexOf(':') >= 0 ? "[" + host + "]" : host;
  }
}
