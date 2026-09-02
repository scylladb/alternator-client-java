package com.scylladb.alternator.internal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.scylladb.alternator.AlternatorConfig;
import com.scylladb.alternator.NodeHealthConfig;
import com.scylladb.alternator.NodeHealthObservation;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import org.junit.Test;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.SdkHttpClient;

public class NodeHealthQueryPlanTest {
  private static final URI ACTIVE = node("active.local");
  private static final URI QUARANTINED_A = node("quarantined-a.local");
  private static final URI QUARANTINED_B = node("quarantined-b.local");

  @Test
  public void regularPlanSamplesOnEveryNthLogicalQueryAndSuppressesDelegateDuplicate() {
    AlternatorLiveNodes liveNodes =
        liveNodes(
            Arrays.asList(ACTIVE, QUARANTINED_A),
            config().withQuarantineTrafficInterval(2).build(),
            System::nanoTime);
    quarantine(liveNodes, QUARANTINED_A);

    NodeHealthQueryPlan first = regular(liveNodes, QUARANTINED_A, ACTIVE);
    assertEquals(ACTIVE, first.nextRouteCandidate());

    NodeHealthQueryPlan second = regular(liveNodes, QUARANTINED_A, ACTIVE);
    assertEquals(QUARANTINED_A, second.nextRouteCandidate());
    assertEquals(ACTIVE, second.nextRouteCandidate());
    assertNull(second.nextRouteCandidate());
  }

  @Test
  public void idleTriggerUsesInjectedMonotonicClockAndCanBeDisabled() {
    AtomicLong clock = new AtomicLong();
    AlternatorLiveNodes enabled =
        liveNodes(
            Arrays.asList(ACTIVE, QUARANTINED_A),
            config()
                .withQuarantineTrafficInterval(100)
                .withQuarantineTrafficIdlePeriodMs(250)
                .build(),
            clock::get);
    quarantine(enabled, QUARANTINED_A);

    clock.set(TimeUnit.MILLISECONDS.toNanos(249));
    assertEquals(ACTIVE, regular(enabled, ACTIVE, QUARANTINED_A).nextRouteCandidate());
    clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(250));
    assertEquals(QUARANTINED_A, regular(enabled, ACTIVE, QUARANTINED_A).nextRouteCandidate());

    clock.set(0);
    AlternatorLiveNodes disabled =
        liveNodes(
            Arrays.asList(ACTIVE, QUARANTINED_A),
            config()
                .withQuarantineTrafficInterval(100)
                .withQuarantineTrafficIdlePeriodMs(0)
                .build(),
            clock::get);
    quarantine(disabled, QUARANTINED_A);
    clock.set(TimeUnit.DAYS.toNanos(1));
    assertEquals(ACTIVE, regular(disabled, ACTIVE, QUARANTINED_A).nextRouteCandidate());
  }

  @Test
  public void defaultIdleTriggerArmsAfterOneHundredMilliseconds() {
    AtomicLong clock = new AtomicLong();
    NodeHealthConfig nodeHealthConfig =
        NodeHealthConfig.builder()
            .withConsecutiveFailureThreshold(1)
            .withDownNodeRecoverySuccessThreshold(1)
            .withQuarantineTrafficInterval(100)
            .build();
    AlternatorLiveNodes liveNodes =
        liveNodes(Arrays.asList(ACTIVE, QUARANTINED_A), nodeHealthConfig, clock::get);
    quarantine(liveNodes, QUARANTINED_A);

    clock.set(TimeUnit.MILLISECONDS.toNanos(100));
    assertEquals(QUARANTINED_A, regular(liveNodes, ACTIVE, QUARANTINED_A).nextRouteCandidate());
  }

  @Test
  public void regularSamplesUseRoundRobinQuarantineOrder() {
    AlternatorLiveNodes liveNodes =
        liveNodes(
            Arrays.asList(ACTIVE, QUARANTINED_A, QUARANTINED_B),
            config().withQuarantineTrafficInterval(1).build(),
            System::nanoTime);
    quarantine(liveNodes, QUARANTINED_A);
    quarantine(liveNodes, QUARANTINED_B);

    assertEquals(
        QUARANTINED_A,
        regular(liveNodes, ACTIVE, QUARANTINED_B, QUARANTINED_A).nextRouteCandidate());
    assertEquals(
        QUARANTINED_B,
        regular(liveNodes, ACTIVE, QUARANTINED_A, QUARANTINED_B).nextRouteCandidate());
    assertEquals(
        QUARANTINED_A,
        regular(liveNodes, ACTIVE, QUARANTINED_A, QUARANTINED_B).nextRouteCandidate());
  }

  @Test
  public void concurrentQueriesConsumeOnePendingSample() throws Exception {
    AlternatorLiveNodes liveNodes =
        liveNodes(
            Arrays.asList(ACTIVE, QUARANTINED_A),
            config().withQuarantineTrafficInterval(2).build(),
            System::nanoTime);
    quarantine(liveNodes, QUARANTINED_A);
    NodeHealthQueryPlan first = regular(liveNodes, ACTIVE, QUARANTINED_A);
    NodeHealthQueryPlan second = regular(liveNodes, ACTIVE, QUARANTINED_A);
    CountDownLatch start = new CountDownLatch(1);
    List<URI> selected = Collections.synchronizedList(new ArrayList<>());
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      executor.submit(() -> selectAfter(start, first, selected));
      executor.submit(() -> selectAfter(start, second, selected));
      start.countDown();
    } finally {
      executor.shutdown();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }

    assertEquals(1, Collections.frequency(selected, QUARANTINED_A));
    assertEquals(1, Collections.frequency(selected, ACTIVE));
  }

  @Test
  public void failedReservationRearmsPendingSample() {
    FailingReservationLiveNodes liveNodes =
        new FailingReservationLiveNodes(
            Arrays.asList(ACTIVE, QUARANTINED_A, QUARANTINED_B),
            config().withQuarantineTrafficInterval(2).build(),
            System::nanoTime);
    quarantine(liveNodes, QUARANTINED_A);
    quarantine(liveNodes, QUARANTINED_B);

    assertEquals(ACTIVE, regular(liveNodes, ACTIVE, QUARANTINED_A).nextRouteCandidate());
    assertEquals(ACTIVE, regular(liveNodes, ACTIVE, QUARANTINED_A).nextRouteCandidate());
    assertEquals(
        QUARANTINED_B,
        regular(liveNodes, ACTIVE, QUARANTINED_A, QUARANTINED_B).nextRouteCandidate());
  }

  @Test
  public void affinityNeverInjectsAndConsumesOnlyNaturallyEncounteredQuarantine() {
    AlternatorLiveNodes liveNodes =
        liveNodes(
            Arrays.asList(ACTIVE, QUARANTINED_A),
            config().withQuarantineTrafficInterval(1).build(),
            System::nanoTime);
    quarantine(liveNodes, QUARANTINED_A);

    NodeHealthQueryPlan activeFirst = affinity(liveNodes, ACTIVE, QUARANTINED_A);
    assertEquals(ACTIVE, activeFirst.nextRouteCandidate());
    assertEquals(QUARANTINED_A, activeFirst.nextRouteCandidate());

    NodeHealthQueryPlan quarantineFirst = affinity(liveNodes, QUARANTINED_A, ACTIVE);
    assertEquals(QUARANTINED_A, quarantineFirst.nextRouteCandidate());
    assertEquals(ACTIVE, quarantineFirst.nextRouteCandidate());
  }

  @Test
  public void affinityActiveOnlySuccessLeavesPendingSampleArmed() {
    AlternatorLiveNodes liveNodes =
        liveNodes(
            Arrays.asList(ACTIVE, QUARANTINED_A),
            config().withQuarantineTrafficInterval(1).build(),
            System::nanoTime);
    quarantine(liveNodes, QUARANTINED_A);

    assertEquals(ACTIVE, affinity(liveNodes, ACTIVE, QUARANTINED_A).nextRouteCandidate());
    assertEquals(QUARANTINED_A, affinity(liveNodes, QUARANTINED_A, ACTIVE).nextRouteCandidate());
  }

  @Test
  public void retriesDoNotAdvanceLogicalQueryCounter() {
    AlternatorLiveNodes liveNodes =
        liveNodes(
            Arrays.asList(ACTIVE, QUARANTINED_A),
            config().withQuarantineTrafficInterval(2).build(),
            System::nanoTime);
    quarantine(liveNodes, QUARANTINED_A);

    NodeHealthQueryPlan firstQuery = affinity(liveNodes, ACTIVE, QUARANTINED_A);
    assertEquals(ACTIVE, firstQuery.nextRouteCandidate());
    assertNull(firstQuery.nextRouteCandidate());
    assertEquals(QUARANTINED_A, affinity(liveNodes, QUARANTINED_A, ACTIVE).nextRouteCandidate());
  }

  @Test
  public void affinityPreservesDelegateOrderForActiveNodes() {
    List<URI> expected = Arrays.asList(QUARANTINED_B, ACTIVE, QUARANTINED_A);
    AlternatorLiveNodes liveNodes =
        liveNodes(expected, config().withQuarantineTrafficInterval(100).build(), System::nanoTime);
    NodeHealthQueryPlan plan = affinity(liveNodes, expected.toArray(new URI[0]));

    List<URI> actual = new ArrayList<>();
    URI selected;
    while ((selected = plan.nextRouteCandidate()) != null) {
      actual.add(selected);
    }
    assertEquals(expected, actual);
  }

  @Test
  public void retryTransitionsRecheckHealthAndRetainDeferredNodes() {
    AlternatorLiveNodes liveNodes =
        liveNodes(
            Arrays.asList(ACTIVE, QUARANTINED_A),
            config()
                .withConsecutiveFailureThreshold(1)
                .withQuarantineFailureThreshold(1)
                .withQuarantineTrafficInterval(100)
                .build(),
            System::nanoTime);
    quarantine(liveNodes, QUARANTINED_A);
    NodeHealthQueryPlan plan = affinity(liveNodes, QUARANTINED_A, ACTIVE);

    assertEquals(ACTIVE, plan.nextRouteCandidate());
    liveNodes.reportNodeResult(ACTIVE, NodeHealthObservation.TRAFFIC_FAILURE);
    assertEquals(QUARANTINED_A, plan.nextRouteCandidate());
    liveNodes.reportNodeResult(QUARANTINED_A, NodeHealthObservation.TRAFFIC_FAILURE);
    assertNull(plan.nextRouteCandidate());
  }

  @Test
  public void selectedQuarantineBecomingDownDoesNotBlockActiveRetry() {
    AlternatorLiveNodes liveNodes =
        liveNodes(
            Arrays.asList(ACTIVE, QUARANTINED_A),
            config().withQuarantineTrafficInterval(1).build(),
            System::nanoTime);
    quarantine(liveNodes, QUARANTINED_A);
    NodeHealthQueryPlan plan = regular(liveNodes, ACTIVE, QUARANTINED_A);

    assertEquals(QUARANTINED_A, plan.nextRouteCandidate());
    liveNodes.reportNodeResult(QUARANTINED_A, NodeHealthObservation.TRAFFIC_FAILURE);
    assertEquals(ACTIVE, plan.nextRouteCandidate());
  }

  @Test
  public void noActivePlanRoutesAllQuarantinedInDelegateOrderButRejectsAllDown() {
    AlternatorLiveNodes liveNodes =
        liveNodes(
            Arrays.asList(QUARANTINED_A, QUARANTINED_B),
            config().withConsecutiveFailureThreshold(1).build(),
            System::nanoTime);
    quarantine(liveNodes, QUARANTINED_A);
    quarantine(liveNodes, QUARANTINED_B);
    NodeHealthQueryPlan plan = affinity(liveNodes, QUARANTINED_B, QUARANTINED_A);
    assertEquals(QUARANTINED_B, plan.nextRouteCandidate());
    assertEquals(QUARANTINED_A, plan.nextRouteCandidate());

    liveNodes.reportNodeResult(QUARANTINED_A, NodeHealthObservation.TRAFFIC_FAILURE);
    liveNodes.reportNodeResult(QUARANTINED_B, NodeHealthObservation.TRAFFIC_FAILURE);
    assertNull(affinity(liveNodes, QUARANTINED_A, QUARANTINED_B).nextRouteCandidate());
  }

  @Test
  public void probePlanUsesActiveFirstThenQuarantineWithoutConsumingTrafficSample() {
    AlternatorLiveNodes liveNodes =
        liveNodes(
            Arrays.asList(ACTIVE, QUARANTINED_A),
            config().withQuarantineTrafficInterval(1).build(),
            System::nanoTime);
    quarantine(liveNodes, QUARANTINED_A);

    assertEquals(ACTIVE, affinity(liveNodes, ACTIVE, QUARANTINED_A).nextRouteCandidate());
    NodeHealthQueryPlan probe =
        liveNodes.newProbeQueryPlan(ordered(liveNodes, QUARANTINED_A, ACTIVE));
    assertEquals(ACTIVE, probe.nextRouteCandidate());
    assertEquals(QUARANTINED_A, probe.nextRouteCandidate());
    assertEquals(QUARANTINED_A, regular(liveNodes, ACTIVE, QUARANTINED_A).nextRouteCandidate());
  }

  @Test
  public void localNodesProbeDoesNotResetIdleTrigger() {
    AtomicLong clock = new AtomicLong();
    AlternatorLiveNodes liveNodes =
        liveNodes(
            Arrays.asList(ACTIVE, QUARANTINED_A),
            config()
                .withQuarantineTrafficInterval(100)
                .withQuarantineTrafficIdlePeriodMs(100)
                .build(),
            clock::get);
    quarantine(liveNodes, QUARANTINED_A);

    clock.set(TimeUnit.MILLISECONDS.toNanos(90));
    try {
      liveNodes.checkIfRackDatacenterFeatureIsSupported();
    } catch (AlternatorLiveNodes.FailedToCheck expected) {
      // The no-op client deliberately makes the control-plane request fail.
    }
    clock.set(TimeUnit.MILLISECONDS.toNanos(100));
    assertEquals(QUARANTINED_A, regular(liveNodes, ACTIVE, QUARANTINED_A).nextRouteCandidate());
  }

  private static void selectAfter(
      CountDownLatch start, NodeHealthQueryPlan plan, List<URI> selected) {
    try {
      start.await();
      selected.add(plan.nextRouteCandidate());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private static NodeHealthConfig.Builder config() {
    return NodeHealthConfig.builder()
        .withConsecutiveFailureThreshold(1)
        .withDownNodeRecoverySuccessThreshold(1)
        .withQuarantineFailureThreshold(1)
        .withQuarantineTrafficIdlePeriodMs(0);
  }

  private static NodeHealthQueryPlan regular(AlternatorLiveNodes liveNodes, URI... order) {
    return liveNodes.newRegularQueryPlan(ordered(liveNodes, order));
  }

  private static NodeHealthQueryPlan affinity(AlternatorLiveNodes liveNodes, URI... order) {
    return liveNodes.newAffinityQueryPlan(ordered(liveNodes, order));
  }

  private static LazyQueryPlan ordered(AlternatorLiveNodes liveNodes, URI... order) {
    return new LazyQueryPlan(liveNodes, Arrays.asList(order));
  }

  private static void quarantine(AlternatorLiveNodes liveNodes, URI node) {
    liveNodes.reportNodeResult(node, NodeHealthObservation.TRAFFIC_FAILURE);
    liveNodes.reportNodeResult(node, NodeHealthObservation.PROBE_SUCCESS);
  }

  private static AlternatorLiveNodes liveNodes(
      List<URI> nodes, NodeHealthConfig config, LongSupplier clock) {
    return new AlternatorLiveNodes(alternatorConfig(nodes, config), new NoopHttpClient(), clock);
  }

  private static AlternatorConfig alternatorConfig(List<URI> nodes, NodeHealthConfig config) {
    List<String> hosts = new ArrayList<>();
    for (URI node : nodes) {
      hosts.add(node.getHost());
    }
    return AlternatorConfig.builder()
        .withSeedHosts(hosts)
        .withScheme("http")
        .withPort(8080)
        .withNodeHealthConfig(config)
        .build();
  }

  private static URI node(String host) {
    return URI.create("http://" + host + ":8080");
  }

  private static final class FailingReservationLiveNodes extends AlternatorLiveNodes {
    private boolean failNextReservation = true;

    private FailingReservationLiveNodes(
        List<URI> nodes, NodeHealthConfig config, LongSupplier clock) {
      super(alternatorConfig(nodes, config), new NoopHttpClient(), clock);
    }

    @Override
    URI reserveQuarantineSample() {
      URI reserved = super.reserveQuarantineSample();
      if (reserved != null && failNextReservation) {
        failNextReservation = false;
        reportNodeResult(reserved, NodeHealthObservation.TRAFFIC_FAILURE);
      }
      return reserved;
    }
  }

  private static final class NoopHttpClient implements SdkHttpClient {
    @Override
    public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
      return new ExecutableHttpRequest() {
        @Override
        public software.amazon.awssdk.http.HttpExecuteResponse call() throws IOException {
          throw new IOException("not used");
        }

        @Override
        public void abort() {}
      };
    }

    @Override
    public void close() {}

    @Override
    public String clientName() {
      return "noop";
    }
  }
}
