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

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

/** Concurrency and resource-bound tests for the shared DNS service. */
public class BoundedDnsResolverTest {

  @Test(timeout = 5000)
  public void testStuckLearnedAndFirstSeedCannotConsumeAllReservedSeedCapacity() throws Exception {
    BoundedDnsResolver.Service generalService = new BoundedDnsResolver.Service(1, 2, 8);
    BoundedDnsResolver.Service seedService = new BoundedDnsResolver.Service(2, 2, 8);
    CountDownLatch learnedEntered = new CountDownLatch(1);
    CountDownLatch releaseLearned = new CountDownLatch(1);
    CountDownLatch stalledSeedEntered = new CountDownLatch(1);
    CountDownLatch releaseStalledSeed = new CountDownLatch(1);
    BoundedDnsResolver resolver =
        new BoundedDnsResolver(
            generalService,
            seedService,
            new Object(),
            hostname -> {
              if ("learned.test".equals(hostname)) {
                learnedEntered.countDown();
                awaitIgnoringInterrupts(releaseLearned);
              } else if ("stalled-seed.test".equals(hostname)) {
                stalledSeedEntered.countDown();
                awaitIgnoringInterrupts(releaseStalledSeed);
              }
              return Collections.singletonList(InetAddress.getByName("192.0.2.10"));
            },
            50,
            8);
    ExecutorService caller = Executors.newSingleThreadExecutor();
    try {
      Future<?> learned =
          caller.submit(
              () -> {
                assertTimesOut(resolver, "learned.test", 50);
                return null;
              });
      assertTrue(learnedEntered.await(1, TimeUnit.SECONDS));

      assertTimesOut(resolver, "stalled-seed.test", 50, true);
      assertTrue(stalledSeedEntered.await(1, TimeUnit.SECONDS));

      assertEquals(
          InetAddress.getByName("192.0.2.10"), resolver.resolve("seed.test", 500, true).get(0));
      learned.get(1, TimeUnit.SECONDS);
      assertEquals(1, generalService.activeWorkerCount());
      assertTrue(seedService.activeWorkerCount() >= 1);
      assertEquals(2, seedService.largestPoolSize());
    } finally {
      releaseLearned.countDown();
      releaseStalledSeed.countDown();
      caller.shutdownNow();
      resolver.close();
      generalService.close();
      seedService.close();
    }
  }

  @Test(timeout = 5000)
  public void testStuckHostIsCoalescedWithoutRetainedWaitersAndHealthyHostCanRecover()
      throws Exception {
    BoundedDnsResolver.Service service = new BoundedDnsResolver.Service(2, 4, 16);
    Object resolverIdentity = new Object();
    CountDownLatch stalledEntered = new CountDownLatch(1);
    CountDownLatch releaseStalled = new CountDownLatch(1);
    AtomicInteger stalledCalls = new AtomicInteger();
    AtomicInteger healthyCalls = new AtomicInteger();
    BoundedDnsResolver resolver =
        new BoundedDnsResolver(
            service,
            resolverIdentity,
            hostname -> {
              if ("stalled.test".equals(hostname)) {
                stalledCalls.incrementAndGet();
                stalledEntered.countDown();
                awaitIgnoringInterrupts(releaseStalled);
                return Collections.singletonList(InetAddress.getByName("192.0.2.1"));
              }
              healthyCalls.incrementAndGet();
              return Collections.singletonList(InetAddress.getByName("192.0.2.2"));
            },
            30,
            8);
    try {
      assertTimesOut(resolver, "stalled.test", 30);
      assertTrue(stalledEntered.await(1, TimeUnit.SECONDS));

      for (int i = 0; i < 100; i++) {
        assertTimesOut(resolver, "stalled.test", 1);
      }

      assertEquals("all callers must share the one running lookup", 1, stalledCalls.get());
      assertEquals(1, service.inFlightCount());
      assertEquals(0, service.waiterCount(resolverIdentity, "stalled.test"));
      assertEquals(0, service.dependentFutureCount(resolverIdentity, "stalled.test"));
      assertEquals(InetAddress.getByName("192.0.2.2"), resolver.resolve("healthy.test").get(0));
      assertEquals(1, healthyCalls.get());
      assertTrue(
          "the test service must stay within its worker cap", service.largestPoolSize() <= 2);

      releaseStalled.countDown();
      awaitNoInFlight(service);
      assertEquals(InetAddress.getByName("192.0.2.1"), resolver.resolve("stalled.test").get(0));
      assertEquals("completed work may be resolved again for recovery", 2, stalledCalls.get());
    } finally {
      releaseStalled.countDown();
      resolver.close();
      service.close();
    }
  }

  @Test(timeout = 5000)
  public void testClosePromptlyReleasesCallerWhenResolverIgnoresInterrupts() throws Exception {
    BoundedDnsResolver.Service service = new BoundedDnsResolver.Service(1, 2, 8);
    Object resolverIdentity = new Object();
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    BoundedDnsResolver resolver =
        new BoundedDnsResolver(
            service,
            resolverIdentity,
            hostname -> {
              entered.countDown();
              awaitIgnoringInterrupts(release);
              return Collections.singletonList(InetAddress.getLoopbackAddress());
            },
            4_000,
            8);
    ExecutorService caller = Executors.newSingleThreadExecutor();
    try {
      Future<?> resolution = caller.submit(() -> resolver.resolve("blocked.test"));
      assertTrue(entered.await(1, TimeUnit.SECONDS));

      resolver.close();

      try {
        resolution.get(500, TimeUnit.MILLISECONDS);
        fail("Expected close to cancel the waiting caller");
      } catch (ExecutionException e) {
        assertTrue(e.getCause().toString(), e.getCause() instanceof IOException);
      }
      assertEquals(0, service.waiterCount(resolverIdentity, "blocked.test"));
      assertEquals(0, service.dependentFutureCount(resolverIdentity, "blocked.test"));
      assertEquals(1, service.inFlightCount());
    } finally {
      release.countDown();
      caller.shutdownNow();
      resolver.close();
      service.close();
    }
  }

  @Test(timeout = 5000)
  public void testAcquireDuringQueuedCancellationDoesNotJoinCancelledTask() throws Exception {
    CountDownLatch cancellationStarted = new CountDownLatch(1);
    CountDownLatch finishCancellation = new CountDownLatch(1);
    CountDownLatch workerEntered = new CountDownLatch(1);
    CountDownLatch releaseWorker = new CountDownLatch(1);
    BoundedDnsResolver.Service service =
        new BoundedDnsResolver.Service(
            1,
            1,
            8,
            () -> {
              cancellationStarted.countDown();
              awaitIgnoringInterrupts(finishCancellation);
            });
    Object resolverIdentity = new Object();
    AtomicInteger replacementCalls = new AtomicInteger();
    BoundedDnsResolver.Resolver resolver =
        hostname -> {
          if ("worker.test".equals(hostname)) {
            workerEntered.countDown();
            awaitIgnoringInterrupts(releaseWorker);
            return Collections.singletonList(InetAddress.getLoopbackAddress());
          }
          replacementCalls.incrementAndGet();
          return Collections.singletonList(InetAddress.getByName("192.0.2.9"));
        };
    ExecutorService releaser = Executors.newSingleThreadExecutor();
    BoundedDnsResolver.Service.Lease worker = null;
    BoundedDnsResolver.Service.Lease queued = null;
    BoundedDnsResolver.Service.Lease replacement = null;
    try {
      worker = service.acquire(resolverIdentity, resolver, "worker.test");
      assertTrue(workerEntered.await(1, TimeUnit.SECONDS));
      queued = service.acquire(resolverIdentity, resolver, "race.test");

      BoundedDnsResolver.Service.Lease leaseToRelease = queued;
      Future<?> cancellation = releaser.submit(leaseToRelease::close);
      assertTrue(cancellationStarted.await(1, TimeUnit.SECONDS));

      replacement = service.acquire(resolverIdentity, resolver, "race.test");
      finishCancellation.countDown();
      cancellation.get(1, TimeUnit.SECONDS);
      releaseWorker.countDown();

      assertEquals(InetAddress.getByName("192.0.2.9"), replacement.result().get().get(0));
      assertEquals(
          "the replacement lease must run exactly one fresh lookup", 1, replacementCalls.get());
    } finally {
      finishCancellation.countDown();
      releaseWorker.countDown();
      if (replacement != null) {
        replacement.close();
      }
      if (queued != null) {
        queued.close();
      }
      if (worker != null) {
        worker.close();
      }
      releaser.shutdownNow();
      service.close();
    }
  }

  @Test(timeout = 5000)
  public void testServiceCloseCompletesRunningAndQueuedWorkAndRejectsNewAcquires()
      throws Exception {
    CountDownLatch runningEntered = new CountDownLatch(1);
    CountDownLatch releaseRunning = new CountDownLatch(1);
    BoundedDnsResolver.Service service = new BoundedDnsResolver.Service(1, 1, 8);
    Object resolverIdentity = new Object();
    BoundedDnsResolver.Resolver resolver =
        hostname -> {
          if ("running.test".equals(hostname)) {
            runningEntered.countDown();
            awaitIgnoringInterrupts(releaseRunning);
          }
          return Collections.singletonList(InetAddress.getLoopbackAddress());
        };
    BoundedDnsResolver.Service.Lease running = null;
    BoundedDnsResolver.Service.Lease queued = null;
    try {
      running = service.acquire(resolverIdentity, resolver, "running.test");
      assertTrue(runningEntered.await(1, TimeUnit.SECONDS));
      queued = service.acquire(resolverIdentity, resolver, "queued.test");

      service.close();

      assertServiceClosed(running);
      assertServiceClosed(queued);
      try {
        service.acquire(resolverIdentity, resolver, "new.test");
        fail("Expected a closed service to reject new DNS work");
      } catch (IOException expected) {
        assertTrue(expected.getMessage().contains("closed"));
      }
    } finally {
      releaseRunning.countDown();
      if (queued != null) {
        queued.close();
      }
      if (running != null) {
        running.close();
      }
      service.close();
    }
  }

  private static void assertTimesOut(
      BoundedDnsResolver resolver, String hostname, long timeoutMillis) throws Exception {
    assertTimesOut(resolver, hostname, timeoutMillis, false);
  }

  private static void assertTimesOut(
      BoundedDnsResolver resolver, String hostname, long timeoutMillis, boolean seedCandidate)
      throws Exception {
    try {
      resolver.resolve(hostname, timeoutMillis, seedCandidate);
      fail("Expected DNS timeout");
    } catch (SocketTimeoutException expected) {
      // Expected.
    }
  }

  private static void awaitIgnoringInterrupts(CountDownLatch latch) {
    boolean complete = false;
    while (!complete) {
      try {
        complete = latch.await(20, TimeUnit.MILLISECONDS);
      } catch (InterruptedException ignored) {
        // Model a platform resolver that ignores cancellation and shutdown interrupts.
      }
    }
  }

  private static void awaitNoInFlight(BoundedDnsResolver.Service service) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
    while (service.inFlightCount() != 0 && System.nanoTime() < deadline) {
      Thread.yield();
    }
    assertEquals(0, service.inFlightCount());
  }

  private static void assertServiceClosed(BoundedDnsResolver.Service.Lease lease) throws Exception {
    try {
      lease.result().get(500, TimeUnit.MILLISECONDS);
      fail("Expected service close to complete DNS work exceptionally");
    } catch (ExecutionException expected) {
      assertTrue(expected.getCause().toString(), expected.getCause() instanceof IOException);
      assertTrue(expected.getCause().getMessage().contains("closed"));
    }
  }
}
