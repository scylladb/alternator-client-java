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
import java.io.InterruptedIOException;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/** Shared, bounded, coalescing execution for platform DNS resolvers. */
final class BoundedDnsResolver implements AutoCloseable {

  interface Resolver {
    List<InetAddress> resolve(String hostname) throws IOException;
  }

  private static final Service SHARED_SERVICE = new Service(4, 32, 64);
  // Keep two resolver workers unavailable to learned-node lookups so a retained DNS seed can still
  // recover the client after every general worker is stuck in a non-cancellable platform call,
  // and one stuck seed cannot block the next retained seed.
  private static final Service SHARED_SEED_SERVICE = new Service(2, 32, 64);

  private final Service service;
  private final Service seedService;
  private final Object resolverIdentity;
  private final Resolver resolver;
  private final int timeoutMillis;
  private final int maximumAnswers;
  private final CompletableFuture<Void> closed = new CompletableFuture<>();

  BoundedDnsResolver(
      Object resolverIdentity, Resolver resolver, int timeoutMillis, int maximumAnswers) {
    this(
        SHARED_SERVICE,
        SHARED_SEED_SERVICE,
        resolverIdentity,
        resolver,
        timeoutMillis,
        maximumAnswers);
  }

  BoundedDnsResolver(
      Service service,
      Object resolverIdentity,
      Resolver resolver,
      int timeoutMillis,
      int maximumAnswers) {
    this(service, service, resolverIdentity, resolver, timeoutMillis, maximumAnswers);
  }

  BoundedDnsResolver(
      Service service,
      Service seedService,
      Object resolverIdentity,
      Resolver resolver,
      int timeoutMillis,
      int maximumAnswers) {
    this.service = service;
    this.seedService = seedService;
    this.resolverIdentity = resolverIdentity;
    this.resolver = resolver;
    this.timeoutMillis = requirePositive(timeoutMillis, "timeoutMillis");
    this.maximumAnswers = requirePositive(maximumAnswers, "maximumAnswers");
  }

  List<InetAddress> resolve(String hostname) throws IOException {
    return resolve(hostname, timeoutMillis);
  }

  List<InetAddress> resolve(String hostname, long callerTimeoutMillis) throws IOException {
    return resolve(hostname, callerTimeoutMillis, false);
  }

  List<InetAddress> resolve(String hostname, long callerTimeoutMillis, boolean seedCandidate)
      throws IOException {
    if (closed.isDone()) {
      throw new IOException("DNS resolver client is closed");
    }
    long waitMillis = Math.min(timeoutMillis, callerTimeoutMillis);
    if (waitMillis <= 0) {
      throw timeout(hostname, 0, null);
    }

    Service targetService = seedCandidate ? seedService : service;
    Service.Lease lease = targetService.acquire(resolverIdentity, resolver, hostname);
    try {
      long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(waitMillis);
      List<InetAddress> result = null;
      while (result == null) {
        if (closed.isDone()) {
          throw new IOException("DNS resolution was cancelled because the HTTP client closed");
        }
        long remainingNanos = deadline - System.nanoTime();
        if (remainingNanos <= 0) {
          throw timeout(hostname, waitMillis, null);
        }
        long pollNanos = Math.min(remainingNanos, TimeUnit.MILLISECONDS.toNanos(25));
        try {
          result = lease.result().get(pollNanos, TimeUnit.NANOSECONDS);
        } catch (TimeoutException e) {
          // Polling avoids attaching an unbounded chain of dependent futures to stuck work.
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          InterruptedIOException interrupted =
              new InterruptedIOException("Interrupted while resolving " + hostname);
          interrupted.initCause(e);
          throw interrupted;
        } catch (ExecutionException e) {
          Throwable cause = e.getCause();
          if (cause instanceof IOException) {
            throw (IOException) cause;
          }
          throw new IOException("DNS resolution failed for " + hostname, cause);
        }
      }
      if (closed.isDone()) {
        throw new IOException("DNS resolution was cancelled because the HTTP client closed");
      }
      return limitAnswers(result, maximumAnswers);
    } finally {
      lease.close();
    }
  }

  @Override
  public void close() {
    closed.complete(null);
  }

  boolean isClosed() {
    return closed.isDone();
  }

  private static List<InetAddress> limitAnswers(List<InetAddress> addresses, int maximum) {
    List<InetAddress> limited = new ArrayList<>(Math.min(addresses.size(), maximum));
    for (InetAddress address : addresses) {
      if (address != null && !limited.contains(address)) {
        limited.add(address);
        if (limited.size() == maximum) {
          break;
        }
      }
    }
    return Collections.unmodifiableList(limited);
  }

  private static SocketTimeoutException timeout(
      String hostname, long timeoutMillis, Exception cause) {
    SocketTimeoutException timeout =
        new SocketTimeoutException(
            "DNS resolution for " + hostname + " exceeded " + timeoutMillis + " ms");
    if (cause != null) {
      timeout.initCause(cause);
    }
    return timeout;
  }

  private static int requirePositive(int value, String name) {
    if (value <= 0) {
      throw new IllegalArgumentException(name + " must be positive");
    }
    return value;
  }

  /** A process-wide pool; package-visible construction is reserved for deterministic tests. */
  static final class Service implements AutoCloseable {
    private final int maximumAnswers;
    private final ThreadPoolExecutor executor;
    private final ConcurrentHashMap<Key, ResolutionTask> inFlight = new ConcurrentHashMap<>();
    private final Runnable queuedCancellationHook;
    private final Object lifecycleLock = new Object();
    private boolean closed;

    Service(int workerCount, int queueCapacity, int maximumAnswers) {
      this(workerCount, queueCapacity, maximumAnswers, () -> {});
    }

    Service(
        int workerCount, int queueCapacity, int maximumAnswers, Runnable queuedCancellationHook) {
      this.maximumAnswers = requirePositive(maximumAnswers, "maximumAnswers");
      if (queuedCancellationHook == null) {
        throw new IllegalArgumentException("queuedCancellationHook must not be null");
      }
      this.queuedCancellationHook = queuedCancellationHook;
      this.executor =
          new ThreadPoolExecutor(
              requirePositive(workerCount, "workerCount"),
              workerCount,
              60L,
              TimeUnit.SECONDS,
              new ArrayBlockingQueue<>(requirePositive(queueCapacity, "queueCapacity")),
              new ResolverThreadFactory(),
              new ThreadPoolExecutor.AbortPolicy());
      this.executor.allowCoreThreadTimeOut(true);
    }

    Lease acquire(Object resolverIdentity, Resolver resolver, String hostname) throws IOException {
      if (resolverIdentity == null || resolver == null || hostname == null || hostname.isEmpty()) {
        throw new IOException("DNS resolution requires a resolver and hostname");
      }
      synchronized (lifecycleLock) {
        if (closed) {
          throw new IOException("Shared DNS resolver service is closed");
        }
      }
      Key key = new Key(resolverIdentity, hostname);
      while (true) {
        ResolutionTask existing = inFlight.get(key);
        if (existing != null) {
          if (existing.tryAddWaiter()) {
            return new Lease(this, existing);
          }
          inFlight.remove(key, existing);
          executor.remove(existing);
          existing.cancelQueued();
          continue;
        }

        ResolutionTask created = new ResolutionTask(this, key, resolver, hostname, maximumAnswers);
        created.tryAddWaiter();
        ResolutionTask raced;
        synchronized (lifecycleLock) {
          if (closed) {
            created.cancelServiceClosed();
            created.releaseWaiter();
            throw new IOException("Shared DNS resolver service is closed");
          }
          raced = inFlight.putIfAbsent(key, created);
          if (raced == null) {
            try {
              executor.execute(created);
              return new Lease(this, created);
            } catch (RejectedExecutionException e) {
              inFlight.remove(key, created);
              created.reject(e);
              created.releaseWaiter();
              throw new IOException("Shared DNS resolver work queue is full", e);
            }
          }
        }
        if (raced != null) {
          created.releaseWaiter();
          continue;
        }
      }
    }

    private void release(ResolutionTask task) {
      if (task.releaseWaiterAndAbandonIfQueued()) {
        try {
          queuedCancellationHook.run();
        } finally {
          inFlight.remove(task.key, task);
          executor.remove(task);
          task.cancelQueued();
        }
      }
    }

    private void completed(ResolutionTask task) {
      inFlight.remove(task.key, task);
    }

    int inFlightCount() {
      return inFlight.size();
    }

    int activeWorkerCount() {
      return executor.getActiveCount();
    }

    int queuedTaskCount() {
      return executor.getQueue().size();
    }

    int largestPoolSize() {
      return executor.getLargestPoolSize();
    }

    int waiterCount(Object resolverIdentity, String hostname) {
      ResolutionTask task = inFlight.get(new Key(resolverIdentity, hostname));
      return task == null ? 0 : task.waiterCount();
    }

    int dependentFutureCount(Object resolverIdentity, String hostname) {
      ResolutionTask task = inFlight.get(new Key(resolverIdentity, hostname));
      return task == null ? 0 : task.result.getNumberOfDependents();
    }

    @Override
    public void close() {
      List<ResolutionTask> tasks;
      synchronized (lifecycleLock) {
        if (closed) {
          return;
        }
        closed = true;
        executor.shutdownNow();
        tasks = new ArrayList<>(inFlight.values());
        inFlight.clear();
      }
      for (ResolutionTask task : tasks) {
        task.cancelServiceClosed();
      }
    }

    static final class Lease implements AutoCloseable {
      private final Service owner;
      private final ResolutionTask task;
      private final AtomicBoolean released = new AtomicBoolean(false);

      Lease(Service owner, ResolutionTask task) {
        this.owner = owner;
        this.task = task;
      }

      CompletableFuture<List<InetAddress>> result() {
        return task.result;
      }

      @Override
      public void close() {
        if (released.compareAndSet(false, true)) {
          owner.release(task);
        }
      }
    }

    private static final class ResolutionTask implements Runnable {
      private final Service owner;
      private final Key key;
      private final Resolver resolver;
      private final String hostname;
      private final int maximumAnswers;
      private final CompletableFuture<List<InetAddress>> result = new CompletableFuture<>();
      private int waiters;
      private TaskState state = TaskState.QUEUED;

      ResolutionTask(
          Service owner, Key key, Resolver resolver, String hostname, int maximumAnswers) {
        this.owner = owner;
        this.key = key;
        this.resolver = resolver;
        this.hostname = hostname;
        this.maximumAnswers = maximumAnswers;
      }

      @Override
      public void run() {
        if (!beginRun()) {
          owner.completed(this);
          return;
        }
        try {
          List<InetAddress> addresses = resolver.resolve(hostname);
          if (addresses == null) {
            throw new IOException("DNS resolver returned null for " + hostname);
          }
          Set<InetAddress> unique = new LinkedHashSet<>();
          for (InetAddress address : addresses) {
            if (address != null) {
              unique.add(address);
              if (unique.size() == maximumAnswers) {
                break;
              }
            }
          }
          result.complete(Collections.unmodifiableList(new ArrayList<>(unique)));
        } catch (Throwable failure) {
          result.completeExceptionally(failure);
        } finally {
          markTerminal();
          owner.completed(this);
        }
      }

      synchronized boolean tryAddWaiter() {
        if (state != TaskState.QUEUED && state != TaskState.RUNNING) {
          return false;
        }
        waiters++;
        return true;
      }

      synchronized void releaseWaiter() {
        if (waiters <= 0) {
          throw new IllegalStateException("DNS resolution task has no waiting callers");
        }
        waiters--;
      }

      synchronized boolean releaseWaiterAndAbandonIfQueued() {
        if (waiters <= 0) {
          throw new IllegalStateException("DNS resolution task has no waiting callers");
        }
        waiters--;
        if (waiters == 0 && state == TaskState.QUEUED) {
          state = TaskState.ABANDONED;
          return true;
        }
        return false;
      }

      synchronized boolean beginRun() {
        if (state != TaskState.QUEUED) {
          return false;
        }
        state = TaskState.RUNNING;
        return true;
      }

      synchronized int waiterCount() {
        return waiters;
      }

      synchronized void reject(RejectedExecutionException failure) {
        state = TaskState.TERMINAL;
        result.completeExceptionally(failure);
      }

      synchronized void cancelQueued() {
        state = TaskState.TERMINAL;
        result.completeExceptionally(
            new IOException("DNS resolution no longer has waiting callers"));
      }

      synchronized void cancelServiceClosed() {
        state = TaskState.TERMINAL;
        result.completeExceptionally(new IOException("Shared DNS resolver service is closed"));
      }

      synchronized void markTerminal() {
        state = TaskState.TERMINAL;
      }

      private enum TaskState {
        QUEUED,
        RUNNING,
        ABANDONED,
        TERMINAL
      }
    }

    private static final class Key {
      private final Object resolverIdentity;
      private final String hostname;
      private final int hashCode;

      Key(Object resolverIdentity, String hostname) {
        this.resolverIdentity = resolverIdentity;
        this.hostname = hostname.toLowerCase(Locale.ROOT);
        this.hashCode = 31 * System.identityHashCode(resolverIdentity) + this.hostname.hashCode();
      }

      @Override
      public boolean equals(Object other) {
        if (this == other) {
          return true;
        }
        if (!(other instanceof Key)) {
          return false;
        }
        Key that = (Key) other;
        return resolverIdentity == that.resolverIdentity && hostname.equals(that.hostname);
      }

      @Override
      public int hashCode() {
        return hashCode;
      }
    }
  }

  private static final class ResolverThreadFactory implements ThreadFactory {
    private final AtomicInteger sequence = new AtomicInteger();

    @Override
    public Thread newThread(Runnable runnable) {
      Thread thread =
          new Thread(runnable, "alternator-shared-dns-resolver-" + sequence.incrementAndGet());
      thread.setDaemon(true);
      return thread;
    }
  }
}
