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
import java.io.InterruptedIOException;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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

  static final Limits DEFAULT_LIMITS =
      new Limits(3_000, 3_000, 5_000, 10_000, 64 * 1024, 1024 * 1024);

  interface Resolver {
    List<InetAddress> resolve(String hostname) throws IOException;
  }

  static final class Limits {
    final int dnsTimeoutMillis;
    final int connectTimeoutMillis;
    final int readTimeoutMillis;
    final int responseTimeoutMillis;
    final int maxHeaderBytes;
    final int maxBodyBytes;

    Limits(
        int connectTimeoutMillis,
        int readTimeoutMillis,
        int responseTimeoutMillis,
        int maxHeaderBytes,
        int maxBodyBytes) {
      this(
          3_000,
          connectTimeoutMillis,
          readTimeoutMillis,
          responseTimeoutMillis,
          maxHeaderBytes,
          maxBodyBytes);
    }

    Limits(
        int dnsTimeoutMillis,
        int connectTimeoutMillis,
        int readTimeoutMillis,
        int responseTimeoutMillis,
        int maxHeaderBytes,
        int maxBodyBytes) {
      this.dnsTimeoutMillis = requirePositive(dnsTimeoutMillis, "dnsTimeoutMillis");
      this.connectTimeoutMillis = requirePositive(connectTimeoutMillis, "connectTimeoutMillis");
      this.readTimeoutMillis = requirePositive(readTimeoutMillis, "readTimeoutMillis");
      this.responseTimeoutMillis = requirePositive(responseTimeoutMillis, "responseTimeoutMillis");
      this.maxHeaderBytes = requirePositive(maxHeaderBytes, "maxHeaderBytes");
      this.maxBodyBytes = requirePositive(maxBodyBytes, "maxBodyBytes");
    }

    private static int requirePositive(int value, String name) {
      if (value <= 0) {
        throw new IllegalArgumentException(name + " must be positive");
      }
      return value;
    }
  }

  private final SdkHttpClient delegate;
  private final Resolver resolver;
  private final SSLSocketFactory sslSocketFactory;
  private final boolean verifyHostname;
  private final Limits limits;
  private final Set<OneShotAddressedHttpRequest> activeRequests = ConcurrentHashMap.newKeySet();
  private final Set<Future<?>> activeResolutions = ConcurrentHashMap.newKeySet();
  private final ThreadPoolExecutor resolverExecutor;
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
    this(delegate, tlsConfig, hostname -> Arrays.asList(InetAddress.getAllByName(hostname)));
  }

  IpDnsFallbackSdkHttpClient(SdkHttpClient delegate, TlsConfig tlsConfig, Resolver resolver) {
    this(delegate, tlsConfig, resolver, defaultSslSocketFactory(tlsConfig), DEFAULT_LIMITS);
  }

  IpDnsFallbackSdkHttpClient(
      SdkHttpClient delegate,
      TlsConfig tlsConfig,
      Resolver resolver,
      SSLSocketFactory sslSocketFactory,
      Limits limits) {
    this.delegate = delegate;
    this.resolver = resolver;
    this.sslSocketFactory = sslSocketFactory;
    this.verifyHostname = tlsConfig == null || tlsConfig.isVerifyHostname();
    this.limits = limits;
    this.resolverExecutor =
        new ThreadPoolExecutor(
            1,
            1,
            0L,
            TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(1),
            new ResolverThreadFactory(),
            new ThreadPoolExecutor.AbortPolicy());
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
    FutureTask<List<InetAddress>> resolution = new FutureTask<>(() -> resolver.resolve(hostname));
    synchronized (lifecycleLock) {
      if (closed.get()) {
        throw new IOException("HTTP client is closed");
      }
      activeResolutions.add(resolution);
      try {
        resolverExecutor.execute(resolution);
      } catch (RejectedExecutionException e) {
        activeResolutions.remove(resolution);
        resolution.cancel(true);
        throw new IOException("DNS resolver is busy or the HTTP client is closed", e);
      }
    }

    try {
      return resolution.get(limits.dnsTimeoutMillis, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      SocketTimeoutException timeout =
          new SocketTimeoutException(
              "DNS resolution for " + hostname + " exceeded " + limits.dnsTimeoutMillis + " ms");
      timeout.initCause(e);
      throw timeout;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      InterruptedIOException interrupted =
          new InterruptedIOException("Interrupted while resolving " + hostname);
      interrupted.initCause(e);
      throw interrupted;
    } catch (CancellationException e) {
      throw new IOException("DNS resolution was cancelled because the HTTP client closed", e);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      throw new IOException("DNS resolution failed for " + hostname, cause);
    } finally {
      resolution.cancel(true);
      activeResolutions.remove(resolution);
      resolverExecutor.purge();
    }
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
                    timeoutTask, limits.responseTimeoutMillis, TimeUnit.MILLISECONDS);
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
      activeResolutions.forEach(resolution -> resolution.cancel(true));
      activeRequests.forEach(OneShotAddressedHttpRequest::abortForClientClose);
      resolverExecutor.shutdownNow();
      timeoutExecutor.shutdownNow();
    }
    try {
      delegate.close();
    } finally {
      activeResolutions.clear();
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

  boolean isResolverExecutorShutdown() {
    return resolverExecutor.isShutdown();
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

  private static final class ResolverThreadFactory implements ThreadFactory {
    @Override
    public Thread newThread(Runnable runnable) {
      Thread thread = new Thread(runnable, "alternator-bounded-dns-resolver");
      thread.setDaemon(true);
      return thread;
    }
  }
}
