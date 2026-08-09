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

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.http.conn.DnsResolver;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpRequest;

/** Apache polling client that pins each addressed request without changing its logical URI. */
final class ApacheDnsFallbackSdkHttpClient implements DnsFallbackSdkHttpClient {

  interface AddressClientFactory {
    SdkHttpClient create(String logicalHostname, InetAddress address);
  }

  private final SdkHttpClient delegate;
  private final BoundedDnsResolver resolver;
  private final AddressClientFactory addressClientFactory;
  private final Set<SdkHttpClient> activeAddressClients = ConcurrentHashMap.newKeySet();
  private final AtomicBoolean closed = new AtomicBoolean(false);

  ApacheDnsFallbackSdkHttpClient(
      SdkHttpClient delegate, DnsResolver resolver, AddressClientFactory addressClientFactory) {
    this(delegate, resolver, addressClientFactory, LiveNodesPollingLimits.DEFAULT, null);
  }

  ApacheDnsFallbackSdkHttpClient(
      SdkHttpClient delegate,
      DnsResolver resolver,
      AddressClientFactory addressClientFactory,
      LiveNodesPollingLimits limits,
      BoundedDnsResolver.Service resolverService) {
    this.delegate = delegate;
    BoundedDnsResolver.Resolver adaptedResolver =
        hostname -> {
          InetAddress[] addresses = resolver.resolve(hostname);
          return addresses == null ? null : Arrays.asList(addresses);
        };
    this.resolver =
        resolverService == null
            ? new BoundedDnsResolver(
                resolver, adaptedResolver, limits.dnsTimeoutMillis, limits.maxDnsAddresses)
            : new BoundedDnsResolver(
                resolverService,
                resolver,
                adaptedResolver,
                limits.dnsTimeoutMillis,
                limits.maxDnsAddresses);
    this.addressClientFactory = addressClientFactory;
  }

  @Override
  public boolean supportsDnsFallback(String scheme) {
    return "http".equalsIgnoreCase(scheme) || "https".equalsIgnoreCase(scheme);
  }

  @Override
  public List<InetAddress> resolve(String hostname) throws IOException {
    return resolver.resolve(hostname);
  }

  @Override
  public List<InetAddress> resolve(String hostname, long timeoutMillis) throws IOException {
    return resolver.resolve(hostname, timeoutMillis);
  }

  @Override
  public ExecutableHttpRequest prepareRequestForAddress(
      HttpExecuteRequest request, InetAddress address) {
    if (closed.get()) {
      throw new IllegalStateException("HTTP client is closed");
    }

    String logicalHostname = request.httpRequest().host();
    SdkHttpClient addressClient = addressClientFactory.create(logicalHostname, address);
    activeAddressClients.add(addressClient);
    if (closed.get()) {
      closeAddressClient(addressClient);
      throw new IllegalStateException("HTTP client is closed");
    }
    try {
      return new OwnerClosingExecutableRequest(
          addressClient.prepareRequest(withNormalizedTlsPeer(request)),
          () -> closeAddressClient(addressClient));
    } catch (RuntimeException e) {
      closeAddressClient(addressClient);
      throw e;
    }
  }

  @Override
  public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
    return delegate.prepareRequest(request);
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      resolver.close();
      RuntimeException failure = null;
      SdkHttpClient[] clients = activeAddressClients.toArray(new SdkHttpClient[0]);
      for (SdkHttpClient activeClient : clients) {
        try {
          closeAddressClient(activeClient);
        } catch (RuntimeException e) {
          failure = accumulate(failure, e);
        }
      }
      try {
        delegate.close();
      } catch (RuntimeException e) {
        failure = accumulate(failure, e);
      }
      if (failure != null) {
        throw failure;
      }
    }
  }

  @Override
  public String clientName() {
    return delegate.clientName();
  }

  private static HttpExecuteRequest withNormalizedTlsPeer(HttpExecuteRequest request) {
    SdkHttpRequest httpRequest = request.httpRequest();
    if (!"https".equalsIgnoreCase(httpRequest.protocol())) {
      return request;
    }
    String logicalHost = httpRequest.host();
    if (!logicalHost.endsWith(".")) {
      return request;
    }
    String tlsPeerName = LogicalHost.tlsPeerName(httpRequest.host());
    if (LogicalHost.isIpLiteral(tlsPeerName) || tlsPeerName.equals(logicalHost)) {
      return request;
    }

    HttpExecuteRequest.Builder normalized =
        HttpExecuteRequest.builder().request(httpRequest.toBuilder().host(tlsPeerName).build());
    request.contentStreamProvider().ifPresent(normalized::contentStreamProvider);
    request.metricCollector().ifPresent(normalized::metricCollector);
    return normalized.build();
  }

  private static RuntimeException accumulate(
      RuntimeException accumulated, RuntimeException failure) {
    if (accumulated == null) {
      return failure;
    }
    accumulated.addSuppressed(failure);
    return accumulated;
  }

  private void closeAddressClient(SdkHttpClient addressClient) {
    if (activeAddressClients.remove(addressClient)) {
      addressClient.close();
    }
  }

  private static final class OwnerClosingExecutableRequest implements ExecutableHttpRequest {
    private final ExecutableHttpRequest delegate;
    private final Runnable closeOwner;
    private final AtomicBoolean ownerClosed = new AtomicBoolean(false);

    OwnerClosingExecutableRequest(ExecutableHttpRequest delegate, Runnable closeOwner) {
      this.delegate = delegate;
      this.closeOwner = closeOwner;
    }

    @Override
    public HttpExecuteResponse call() throws IOException {
      try {
        HttpExecuteResponse response = delegate.call();
        if (!response.responseBody().isPresent()) {
          closeOwner();
          return response;
        }

        AbortableInputStream body = response.responseBody().get();
        AbortableInputStream ownerClosingBody =
            AbortableInputStream.create(
                new OwnerClosingInputStream(body, this::closeOwner),
                () -> {
                  try {
                    body.abort();
                  } finally {
                    closeOwner();
                  }
                });
        return HttpExecuteResponse.builder()
            .response(response.httpResponse())
            .responseBody(ownerClosingBody)
            .build();
      } catch (IOException | RuntimeException e) {
        closeOwner();
        throw e;
      }
    }

    @Override
    public void abort() {
      try {
        delegate.abort();
      } finally {
        closeOwner();
      }
    }

    private void closeOwner() {
      if (ownerClosed.compareAndSet(false, true)) {
        closeOwner.run();
      }
    }
  }

  private static final class OwnerClosingInputStream extends FilterInputStream {
    private final Runnable closeOwner;

    OwnerClosingInputStream(InputStream delegate, Runnable closeOwner) {
      super(delegate);
      this.closeOwner = closeOwner;
    }

    @Override
    public void close() throws IOException {
      try {
        super.close();
      } finally {
        closeOwner.run();
      }
    }
  }
}
