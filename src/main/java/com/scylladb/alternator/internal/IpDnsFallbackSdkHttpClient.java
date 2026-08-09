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

import com.scylladb.alternator.TlsConfig;
import java.io.IOException;
import java.net.InetAddress;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.SdkHttpClient;

/**
 * Address-aware polling wrapper used with transports that cannot override DNS per request.
 *
 * <p>Ordinary requests continue through the delegate. Addressed {@code /localnodes} GETs use a
 * bounded one-shot socket transport so the numeric connect address can remain independent of the
 * logical URI, Host header, TLS SNI name, certificate hostname, and signing headers. This avoids
 * depending on AWS CRT implementation internals.
 */
final class IpDnsFallbackSdkHttpClient implements DnsFallbackSdkHttpClient {

  static final LiveNodesPollingLimits DEFAULT_LIMITS = LiveNodesPollingLimits.DEFAULT;

  private static final Resolver SYSTEM_RESOLVER =
      hostname -> Arrays.asList(InetAddress.getAllByName(hostname));

  interface Resolver {
    List<InetAddress> resolve(String hostname) throws IOException;
  }

  private final SdkHttpClient delegate;
  private final BoundedDnsResolver resolver;
  private final SSLSocketFactory sslSocketFactory;
  private final boolean verifyHostname;
  private final LiveNodesPollingLimits limits;
  private final Set<OneShotAddressedHttpRequest> activeRequests = ConcurrentHashMap.newKeySet();
  private final ScheduledThreadPoolExecutor timeoutExecutor;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final Object lifecycleLock = new Object();

  IpDnsFallbackSdkHttpClient(SdkHttpClient delegate) {
    this(delegate, (TlsConfig) null);
  }

  IpDnsFallbackSdkHttpClient(SdkHttpClient delegate, Resolver resolver) {
    this(delegate, null, resolver, defaultSslSocketFactory(null), DEFAULT_LIMITS);
  }

  IpDnsFallbackSdkHttpClient(SdkHttpClient delegate, TlsConfig tlsConfig) {
    this(delegate, tlsConfig, SYSTEM_RESOLVER);
  }

  IpDnsFallbackSdkHttpClient(SdkHttpClient delegate, TlsConfig tlsConfig, Resolver resolver) {
    this(delegate, tlsConfig, resolver, defaultSslSocketFactory(tlsConfig), DEFAULT_LIMITS);
  }

  IpDnsFallbackSdkHttpClient(
      SdkHttpClient delegate,
      TlsConfig tlsConfig,
      Resolver resolver,
      SSLSocketFactory sslSocketFactory,
      LiveNodesPollingLimits limits) {
    this(delegate, tlsConfig, resolver, sslSocketFactory, limits, null);
  }

  IpDnsFallbackSdkHttpClient(
      SdkHttpClient delegate,
      TlsConfig tlsConfig,
      Resolver resolver,
      SSLSocketFactory sslSocketFactory,
      LiveNodesPollingLimits limits,
      BoundedDnsResolver.Service resolverService) {
    this.delegate = delegate;
    this.resolver =
        resolverService == null
            ? new BoundedDnsResolver(
                resolver, resolver::resolve, limits.dnsTimeoutMillis, limits.maxDnsAddresses)
            : new BoundedDnsResolver(
                resolverService,
                resolver,
                resolver::resolve,
                limits.dnsTimeoutMillis,
                limits.maxDnsAddresses);
    this.sslSocketFactory = sslSocketFactory;
    this.verifyHostname = tlsConfig == null || tlsConfig.isVerifyHostname();
    this.limits = limits;
    this.timeoutExecutor = new ScheduledThreadPoolExecutor(1, new AddressTimeoutThreadFactory());
    this.timeoutExecutor.setRemoveOnCancelPolicy(true);
    this.timeoutExecutor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
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
  public List<InetAddress> resolve(String hostname, long timeoutMillis, boolean seedCandidate)
      throws IOException {
    return resolver.resolve(hostname, timeoutMillis, seedCandidate);
  }

  @Override
  public ExecutableHttpRequest prepareRequestForAddress(
      HttpExecuteRequest request, InetAddress address) {
    if (closed.get()) {
      throw new IllegalStateException("HTTP client is closed");
    }
    return new OneShotAddressedHttpRequest(
        request,
        address,
        sslSocketFactory,
        verifyHostname,
        limits,
        new OneShotAddressedHttpRequest.Lifecycle() {
          @Override
          public ScheduledFuture<?> activate(
              OneShotAddressedHttpRequest addressedRequest, Runnable timeoutTask)
              throws IOException {
            synchronized (lifecycleLock) {
              if (closed.get()) {
                throw new IOException("HTTP client is closed");
              }
              activeRequests.add(addressedRequest);
              try {
                return timeoutExecutor.schedule(
                    timeoutTask, limits.attemptTimeoutMillis, TimeUnit.MILLISECONDS);
              } catch (RejectedExecutionException e) {
                activeRequests.remove(addressedRequest);
                addressedRequest.abortForClientClose();
                throw new IOException("HTTP client is closed", e);
              }
            }
          }

          @Override
          public void deactivate(OneShotAddressedHttpRequest addressedRequest) {
            activeRequests.remove(addressedRequest);
          }
        });
  }

  @Override
  public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
    return delegate.prepareRequest(request);
  }

  @Override
  public void close() {
    synchronized (lifecycleLock) {
      if (!closed.compareAndSet(false, true)) {
        return;
      }
      resolver.close();
      activeRequests.forEach(OneShotAddressedHttpRequest::abortForClientClose);
      timeoutExecutor.shutdownNow();
    }
    try {
      delegate.close();
    } finally {
      activeRequests.clear();
    }
  }

  @Override
  public String clientName() {
    return delegate.clientName();
  }

  int activeRequestCount() {
    return activeRequests.size();
  }

  boolean isTimeoutExecutorShutdown() {
    return timeoutExecutor.isShutdown();
  }

  boolean isResolverClosed() {
    return resolver.isClosed();
  }

  private static SSLSocketFactory defaultSslSocketFactory(TlsConfig tlsConfig) {
    TlsConfig effectiveTlsConfig = tlsConfig == null ? TlsConfig.systemDefault() : tlsConfig;
    TrustManager[] trustManagers =
        effectiveTlsConfig.isTrustAllCertificates()
            ? TlsContextFactory.createTrustAllManagers()
            : TlsContextFactory.createTrustManagers(effectiveTlsConfig);
    try {
      SSLContext context = SSLContext.getInstance("TLS");
      context.init(null, trustManagers, new SecureRandom());
      return context.getSocketFactory();
    } catch (GeneralSecurityException e) {
      throw new RuntimeException("Failed to create addressed polling TLS context", e);
    }
  }

  private static final class AddressTimeoutThreadFactory implements ThreadFactory {
    @Override
    public Thread newThread(Runnable runnable) {
      Thread thread = new Thread(runnable, "alternator-addressed-poll-timeout");
      thread.setDaemon(true);
      return thread;
    }
  }
}
