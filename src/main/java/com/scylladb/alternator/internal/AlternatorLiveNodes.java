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

import com.scylladb.alternator.AlternatorConfig;
import com.scylladb.alternator.routing.ClusterScope;
import com.scylladb.alternator.routing.DatacenterScope;
import com.scylladb.alternator.routing.RackScope;
import com.scylladb.alternator.routing.RoutingScope;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;

/**
 * Maintains and automatically updates a list of known live Alternator nodes. Live Alternator nodes
 * should answer alternatorScheme (http or https) requests on port alternatorPort. One of these
 * livenodes will be used, at round-robin order, for every connection. For cluster-wide routing,
 * including a filtered scope chain that explicitly falls back to cluster routing, the list starts
 * with the configured seeds. For a strict filtered chain, seeds remain discovery-only until a
 * matching node list is learned. A thread periodically replaces the routing list with an up-to-date
 * list retrieved by making "/localnodes" requests to known nodes and seeds.
 *
 * @author dmitry.kropachev
 */
public class AlternatorLiveNodes extends Thread {
  private static final int MAX_ROUTING_SCOPE_DEPTH = 64;
  private static final long DEFAULT_SHUTDOWN_TIMEOUT_MS = 5_000;

  private final AtomicReference<List<URI>> liveNodes;
  private final List<URI> initialNodes;
  private final Set<URI> initialNodeSet;
  private final AtomicInteger nextLiveNodeIndex;
  private final AlternatorConfig config;
  private final AtomicBoolean running = new AtomicBoolean(false);
  private final AtomicBoolean shutdownRequested = new AtomicBoolean(false);
  private final SdkHttpClient pollingHttpClient;
  private final boolean ownsPollingClient;
  private final AtomicBoolean pollingClientClosed = new AtomicBoolean(false);
  private final AtomicLong lastActivityTime = new AtomicLong(0);
  private final AtomicBoolean refreshRequested = new AtomicBoolean(false);
  private final Object refreshMonitor = new Object();
  private final AtomicLong refreshSequence = new AtomicLong(0);
  private final Object publicationLock = new Object();
  private final LocalNodesResponseParser localNodesResponseParser;
  private final LiveNodesPollingLimits pollingLimits;
  private long lastPublishedRefresh;
  private RoutingScope publishedScope;

  private static Logger logger = Logger.getLogger(AlternatorLiveNodes.class.getName());

  /** {@inheritDoc} */
  @Override
  public void run() {
    logger.log(Level.INFO, "AlternatorLiveNodes thread started");
    running.set(true);
    try {
      while (!shutdownRequested.get()) {
        try {
          updateLiveNodes();
        } catch (IOException e) {
          if (shutdownRequested.get()) {
            logger.log(Level.FINE, "AlternatorLiveNodes polling stopped during shutdown", e);
            return;
          }
          logger.log(Level.SEVERE, "AlternatorLiveNodes failed to sync nodes list", e);
        } catch (RuntimeException e) {
          if (shutdownRequested.get()) {
            logger.log(Level.FINE, "AlternatorLiveNodes polling stopped during shutdown", e);
            return;
          }
          logger.log(Level.SEVERE, "AlternatorLiveNodes polling failed unexpectedly", e);
        }
        try {
          waitForNextRefresh();
        } catch (InterruptedException e) {
          if (shutdownRequested.get()) {
            logger.log(Level.INFO, "AlternatorLiveNodes thread interrupted and stopping");
            Thread.currentThread().interrupt(); // Restore interrupted status
            return;
          }
          logger.log(Level.FINE, "AlternatorLiveNodes thread interrupted without shutdown request");
        }
      }
    } finally {
      running.set(false);
      closePollingClient();
      logger.log(Level.INFO, "AlternatorLiveNodes thread stopped");
    }
  }

  /** Closes the polling HTTP client if this instance owns it. */
  private void closePollingClient() {
    if (ownsPollingClient
        && pollingHttpClient != null
        && pollingClientClosed.compareAndSet(false, true)) {
      pollingHttpClient.close();
    }
  }

  /**
   * Initiates a graceful shutdown of the background thread.
   *
   * <p>This method signals the thread to stop and returns immediately. Use {@link #join()} or
   * {@link #join(long)} to wait for the thread to terminate.
   *
   * @since 2.0.4
   */
  public void shutdown() {
    shutdownRequested.set(true);
    synchronized (refreshMonitor) {
      refreshMonitor.notifyAll();
    }
    this.interrupt();
    closePollingClient();
  }

  /**
   * Initiates shutdown and waits for the background thread to stop.
   *
   * @return true if the thread stopped before the default timeout, false otherwise
   * @since 2.0.5
   */
  public boolean shutdownAndWait() {
    return shutdownAndWait(DEFAULT_SHUTDOWN_TIMEOUT_MS);
  }

  /**
   * Initiates shutdown and waits up to the requested timeout for the background thread to stop.
   *
   * @param timeoutMs maximum time to wait in milliseconds
   * @return true if the thread stopped before the timeout, false otherwise
   * @since 2.0.5
   */
  public boolean shutdownAndWait(long timeoutMs) {
    shutdown();
    if (Thread.currentThread() == this) {
      return false;
    }
    if (timeoutMs <= 0) {
      return !isAlive();
    }
    try {
      join(timeoutMs);
      return !isAlive();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
  }

  /**
   * Checks if the background thread is currently running.
   *
   * @return true if the thread is running, false otherwise
   * @since 2.0.4
   */
  public boolean isRunning() {
    return running.get();
  }

  /**
   * Determines the appropriate refresh interval based on recent activity.
   *
   * <p>If there has been activity within the idle refresh interval, use the active refresh
   * interval. Otherwise, use the idle refresh interval.
   *
   * @return the refresh interval in milliseconds
   */
  private long getRefreshInterval() {
    long lastActivity = lastActivityTime.get();
    long idleThreshold = config.getIdleRefreshIntervalMs();
    long timeSinceActivity = System.currentTimeMillis() - lastActivity;

    if (timeSinceActivity < idleThreshold) {
      return config.getActiveRefreshIntervalMs();
    }
    return idleThreshold;
  }

  /**
   * Marks that there has been recent activity (a request was made). This affects the refresh
   * interval used by the background thread.
   */
  public void recordRequestActivity() {
    long now = System.currentTimeMillis();
    long previous = lastActivityTime.getAndSet(now);
    if (previous == 0 || now - previous >= config.getIdleRefreshIntervalMs()) {
      refreshRequested.set(true);
      synchronized (refreshMonitor) {
        refreshMonitor.notifyAll();
      }
    }
  }

  private void waitForNextRefresh() throws InterruptedException {
    synchronized (refreshMonitor) {
      if (refreshRequested.getAndSet(false) || shutdownRequested.get()) {
        return;
      }
      refreshMonitor.wait(getRefreshInterval());
      // Consume the request that woke this wait. Otherwise the next loop iteration would observe
      // it again and perform a duplicate immediate refresh.
      refreshRequested.set(false);
    }
  }

  /**
   * Constructor for AlternatorLiveNodes.
   *
   * @param liveNode a {@link java.net.URI} object
   * @param datacenter a {@link java.lang.String} object
   * @param rack a {@link java.lang.String} object
   * @deprecated Use {@link #AlternatorLiveNodes(AlternatorConfig, SdkHttpClient)} instead.
   */
  @Deprecated
  public AlternatorLiveNodes(URI liveNode, String datacenter, String rack) {
    this(
        AlternatorConfig.builder()
            .withSeedNode(liveNode)
            .withRoutingScope(deriveRoutingScope(datacenter, rack))
            .build());
  }

  /**
   * Constructor for AlternatorLiveNodes with RoutingScope.
   *
   * @param liveNode a {@link java.net.URI} object
   * @param routingScope the routing scope for node targeting
   * @since 2.0.0
   * @deprecated Use {@link #AlternatorLiveNodes(AlternatorConfig, SdkHttpClient)} instead.
   */
  @Deprecated
  public AlternatorLiveNodes(URI liveNode, RoutingScope routingScope) {
    this(AlternatorConfig.builder().withSeedNode(liveNode).withRoutingScope(routingScope).build());
  }

  /**
   * Constructor for AlternatorLiveNodes with a seed URI and AlternatorConfig.
   *
   * @param seedUri the seed URI for the initial node
   * @param config the Alternator configuration containing routing scope and other settings
   * @since 2.0.0
   * @deprecated Use {@link #AlternatorLiveNodes(AlternatorConfig, SdkHttpClient)} with config
   *     containing seed node.
   */
  @Deprecated
  public AlternatorLiveNodes(URI seedUri, AlternatorConfig config) {
    this(config.getSeedHosts().isEmpty() ? configWithSeedUri(seedUri, config) : config);
  }

  private static AlternatorConfig configWithSeedUri(URI seedUri, AlternatorConfig config) {
    AlternatorConfig.Builder builder =
        AlternatorConfig.builder()
            .withSeedNode(seedUri)
            .withRoutingScope(config.getRoutingScope())
            .withCompressionAlgorithm(config.getCompressionAlgorithm())
            .withMinCompressionSizeBytes(config.getMinCompressionSizeBytes())
            .withOptimizeHeaders(config.isOptimizeHeaders())
            .withHeadersWhitelist(config.getHeadersWhitelist());
    if (config.isResponseCompressionEnabled()) {
      builder.withResponseCompression(config.getResponseCompressionAlgorithms());
    } else {
      builder.withResponseCompressionDisabled();
    }
    return builder.build();
  }

  /**
   * Constructor for AlternatorLiveNodes.
   *
   * @param liveNodes a {@link java.util.List} object of URIs
   * @param scheme a {@link java.lang.String} object (ignored, extracted from URIs)
   * @param port a int (ignored, extracted from URIs)
   * @param datacenter a {@link java.lang.String} object
   * @param rack a {@link java.lang.String} object
   * @since 1.0.1
   * @deprecated Use {@link #AlternatorLiveNodes(AlternatorConfig, SdkHttpClient)} instead.
   */
  @Deprecated
  public AlternatorLiveNodes(
      List<URI> liveNodes, String scheme, int port, String datacenter, String rack) {
    this(
        AlternatorConfig.builder()
            .withSeedHosts(extractHosts(liveNodes))
            .withRoutingScope(deriveRoutingScope(datacenter, rack))
            .withScheme(scheme)
            .withPort(port)
            .build());
  }

  /**
   * Constructor for AlternatorLiveNodes with RoutingScope.
   *
   * @param seeds a {@link java.util.List} object of URIs
   * @param scheme a {@link java.lang.String} object (ignored, extracted from URIs)
   * @param port a int (ignored, extracted from URIs)
   * @param routingScope the routing scope for node targeting
   * @since 2.0.0
   * @deprecated Use {@link #AlternatorLiveNodes(AlternatorConfig, SdkHttpClient)} instead.
   */
  @Deprecated
  public AlternatorLiveNodes(
      List<String> seeds, String scheme, int port, RoutingScope routingScope) {
    this(
        AlternatorConfig.builder()
            .withSeedHosts(seeds)
            .withRoutingScope(routingScope)
            .withScheme(scheme)
            .withPort(port)
            .build());
  }

  private static List<String> extractHosts(List<URI> seeds) {
    return seeds.stream()
        .map(URI::getHost)
        .filter(Objects::nonNull)
        .distinct()
        .collect(Collectors.toList());
  }

  /**
   * Derives a RoutingScope from legacy datacenter/rack parameters.
   *
   * @param datacenter the datacenter name (may be null or empty)
   * @param rack the rack name (may be null or empty)
   * @return the derived routing scope
   */
  private static RoutingScope deriveRoutingScope(String datacenter, String rack) {
    String dc = datacenter != null ? datacenter : "";
    String r = rack != null ? rack : "";
    if (dc.isEmpty()) {
      return ClusterScope.create();
    }
    if (r.isEmpty()) {
      return DatacenterScope.of(dc, ClusterScope.create());
    }
    return RackScope.of(dc, r, DatacenterScope.of(dc, ClusterScope.create()));
  }

  /**
   * Constructor for AlternatorLiveNodes with AlternatorConfig.
   *
   * <p>Creates an internal polling HTTP client using the detected sync implementation on the
   * classpath.
   *
   * @param config the Alternator configuration
   * @throws RuntimeException if config is null or contains no seed hosts
   * @since 2.0.0
   */
  public AlternatorLiveNodes(AlternatorConfig config) {
    this(config, createDefaultPollingClient(config), true, LiveNodesPollingLimits.DEFAULT);
  }

  /**
   * Constructor for AlternatorLiveNodes with AlternatorConfig and an externally-provided polling
   * HTTP client.
   *
   * <p>The provided polling client will NOT be closed by this instance; the caller is responsible
   * for its lifecycle.
   *
   * @param config the Alternator configuration
   * @param pollingHttpClient the SdkHttpClient to use for polling /localnodes
   * @throws RuntimeException if config is null or contains no seed hosts
   * @since 2.1.0
   */
  public AlternatorLiveNodes(AlternatorConfig config, SdkHttpClient pollingHttpClient) {
    this(config, pollingHttpClient, false, LiveNodesPollingLimits.DEFAULT);
  }

  AlternatorLiveNodes(
      AlternatorConfig config,
      SdkHttpClient pollingHttpClient,
      LiveNodesPollingLimits pollingLimits) {
    this(config, pollingHttpClient, false, pollingLimits);
  }

  private AlternatorLiveNodes(
      AlternatorConfig config,
      SdkHttpClient pollingHttpClient,
      boolean ownsPollingClient,
      LiveNodesPollingLimits pollingLimits) {
    if (config == null) {
      throw new RuntimeException("config cannot be null");
    }
    if (pollingHttpClient == null) {
      throw new RuntimeException("pollingHttpClient cannot be null");
    }
    List<String> seedHosts = config.getSeedHosts();
    if (seedHosts == null || seedHosts.isEmpty()) {
      throw new RuntimeException("config must contain at least one seed host");
    }
    this.pollingLimits = Objects.requireNonNull(pollingLimits, "pollingLimits");
    this.localNodesResponseParser =
        new LocalNodesResponseParser(
            config.getScheme(), config.getPort(), this.pollingLimits.maxSnapshotNodes);
    List<URI> configuredInitialNodes = hostsToUris(seedHosts);
    try {
      ensureSnapshotWithinLimits(configuredInitialNodes);
    } catch (DiscoverySnapshotLimitException e) {
      throw new IllegalArgumentException("Configured seed snapshot exceeds discovery limits", e);
    }
    this.initialNodes = Collections.unmodifiableList(configuredInitialNodes);
    this.initialNodeSet = Collections.unmodifiableSet(new LinkedHashSet<>(configuredInitialNodes));
    this.liveNodes = new AtomicReference<>();
    this.nextLiveNodeIndex = new AtomicInteger(0);
    this.config = config;
    this.pollingHttpClient = pollingHttpClient;
    this.ownsPollingClient = ownsPollingClient;
    try {
      this.validate();
    } catch (ValidationError e) {
      throw new RuntimeException(e);
    }
    // Seeds are valid routing targets when any valid configured fallback authorizes cluster-wide
    // routing. In a strict filtered chain, the entrypoint may sit outside the requested rack or
    // datacenter, so seeds remain discovery-only until a matching /localnodes response is learned.
    RoutingScope clusterAuthorization = clusterScopeInValidChain(config.getRoutingScope());
    if (clusterAuthorization != null) {
      this.liveNodes.set(initialNodes);
      this.publishedScope = clusterAuthorization;
    } else {
      this.liveNodes.set(Collections.emptyList());
      this.publishedScope = null;
    }
  }

  /**
   * Creates a default polling client by detecting which sync HTTP client is on the classpath.
   *
   * @param config the Alternator configuration
   * @return a small SdkHttpClient for polling
   */
  private static SdkHttpClient createDefaultPollingClient(AlternatorConfig config) {
    SyncClientDetector.SyncClientType type = SyncClientDetector.detect();
    return SyncClientDetector.createPollingClient(
        type, config != null ? config.getTlsConfig() : null);
  }

  /** {@inheritDoc} */
  @Override
  public void start() {
    if (!running.compareAndSet(false, true)) {
      return;
    }
    // setDaemon(true) allows the program to exit even if the thread is still running.
    this.setDaemon(true);
    super.start();
  }

  /**
   * Validates that the given URI is a valid URL.
   *
   * @param uri a {@link java.net.URI} object to validate
   * @throws ValidationError if the URI is not a valid URL
   * @since 1.0.1
   */
  public void validateURI(URI uri) throws ValidationError {
    try {
      uri.toURL();
    } catch (MalformedURLException e) {
      throw new ValidationError("Invalid URI: " + uri, e);
    }
  }

  /**
   * Validates the configuration and all initial node URIs.
   *
   * @throws ValidationError if any configuration or URI is invalid
   * @since 1.0.1
   */
  public void validate() throws ValidationError {
    this.validateConfig();
    for (URI liveNode : initialNodes) {
      this.validateURI(liveNode);
    }
  }

  /** Exception thrown when configuration validation fails. */
  public static class ValidationError extends Exception {
    /**
     * Constructs a new ValidationError with the specified message.
     *
     * @param message the detail message
     */
    public ValidationError(String message) {
      super(message);
    }

    /**
     * Constructs a new ValidationError with the specified message and cause.
     *
     * @param message the detail message
     * @param cause the cause of this exception
     */
    public ValidationError(String message, Throwable cause) {
      super(message, cause);
    }
  }

  private void validateConfig() throws ValidationError {
    try {
      // Make sure that the configured scheme and port are valid values.
      this.hostToURI("1.1.1.1");
    } catch (MalformedURLException | URISyntaxException e) {
      throw new ValidationError("failed to validate configuration", e);
    }
  }

  private URI hostToURI(String host) throws URISyntaxException, MalformedURLException {
    return localNodesResponseParser.hostToURI(host);
  }

  private List<URI> hostsToUris(List<String> hosts) {
    List<URI> uris = new ArrayList<>();
    for (String host : hosts) {
      try {
        uris.add(hostToURI(host));
      } catch (URISyntaxException | MalformedURLException e) {
        throw new RuntimeException("Invalid host: " + host, e);
      }
    }
    return uris;
  }

  /**
   * nextAsURI.
   *
   * @return a {@link java.net.URI} object
   */
  public URI nextAsURI() {
    recordRequestActivity();
    List<URI> nodes = liveNodes.get();
    if (nodes.isEmpty()) {
      throw new IllegalStateException("No live nodes available");
    }
    return nodes.get(Math.abs(nextLiveNodeIndex.getAndIncrement() % nodes.size()));
  }

  /**
   * nextAsURI.
   *
   * @param path a {@link java.lang.String} object
   * @param query a {@link java.lang.String} object
   * @return a {@link java.net.URI} object
   * @since 1.0.1
   */
  public URI nextAsURI(String path, String query) {
    try {
      URI uri = this.nextAsURI();
      return withPathAndQuery(uri, path, query);
    } catch (URISyntaxException e) {
      // Should never happen, nextAsURI content is already validated
      throw new RuntimeException(e);
    }
  }

  private URI withPathAndQuery(URI uri, String path, String query) throws URISyntaxException {
    return new URI(
        uri.getScheme(),
        null,
        LogicalHost.withoutBrackets(uri.getHost()),
        uri.getPort(),
        path,
        query,
        null);
  }

  void updateLiveNodes() throws IOException {
    throwIfShutdownRequested();
    long refresh = refreshSequence.incrementAndGet();
    List<RoutingScope> scopes = routingScopes(this.config.getRoutingScope());
    IOException lastException = null;
    Set<RoutingScope> authoritativelyEmptyScopes =
        Collections.newSetFromMap(new IdentityHashMap<>());
    long cycleDeadline = deadlineAfterMillis(pollingLimits.cycleTimeoutMillis);
    for (int scopeIndex = 0; scopeIndex < scopes.size(); scopeIndex++) {
      throwIfShutdownRequested();
      RoutingScope scope = scopes.get(scopeIndex);
      boolean scopeAuthoritativelyEmpty = false;
      try {
        long scopeDeadline =
            fairDeadline(
                cycleDeadline, pollingLimits.cycleTimeoutMillis, scopes.size() - scopeIndex);
        List<URI> nodes = getNodesForScope(scope, scopeDeadline);
        if (!nodes.isEmpty()) {
          throwIfShutdownRequested();
          List<URI> replacement = Collections.unmodifiableList(new ArrayList<>(nodes));
          if (publishIfCurrent(refresh, replacement, scope)) {
            logger.log(
                Level.FINE, "Updated hosts to " + replacement + " using " + scope.getDescription());
          } else {
            logger.log(
                Level.FINE, "Ignored a stale live-node refresh for " + scope.getDescription());
          }
          return;
        }
        scopeAuthoritativelyEmpty = true;
      } catch (IOException e) {
        if (shutdownRequested.get()) {
          throw e;
        }
        logger.log(Level.WARNING, "Failed to discover nodes for " + scope.getDescription(), e);
        lastException = e;
      }
      // A normal empty return is authoritative for this exact strict scope. Cluster emptiness is
      // unusable discovery and never disproves an existing route.
      if (scopeAuthoritativelyEmpty && !(scope instanceof ClusterScope)) {
        authoritativelyEmptyScopes.add(scope);
      }
      if (scopeIndex + 1 < scopes.size()) {
        logger.log(
            Level.WARNING,
            "No nodes found for "
                + scope.getDescription()
                + ", falling back to "
                + scopes.get(scopeIndex + 1).getDescription());
      }
    }
    throwIfShutdownRequested();
    if (clearIfPublishedScopeAuthoritativelyEmpty(refresh, authoritativelyEmptyScopes)) {
      logger.log(Level.WARNING, "The published routing scope is now authoritatively empty");
      return;
    }
    // Failed refreshes keep the current list. Configured seeds remain separate discovery
    // candidates even after a successful snapshot replaces them.
    if (lastException != null) {
      logger.log(
          Level.WARNING,
          "All nodes unreachable in every routing scope, keeping existing node list");
    } else {
      logger.log(Level.WARNING, "No nodes found in any routing scope, keeping existing node list");
    }
  }

  private boolean publishIfCurrent(
      long refresh, List<URI> replacement, RoutingScope replacementScope) {
    synchronized (publicationLock) {
      if (refresh <= lastPublishedRefresh) {
        return false;
      }
      liveNodes.set(replacement);
      publishedScope = replacementScope;
      lastPublishedRefresh = refresh;
      return true;
    }
  }

  private boolean clearIfPublishedScopeAuthoritativelyEmpty(
      long refresh, Set<RoutingScope> emptyScopes) {
    synchronized (publicationLock) {
      if (refresh <= lastPublishedRefresh
          || publishedScope == null
          || publishedScope instanceof ClusterScope
          || !emptyScopes.contains(publishedScope)) {
        return false;
      }
      liveNodes.set(Collections.emptyList());
      publishedScope = null;
      lastPublishedRefresh = refresh;
      return true;
    }
  }

  private List<URI> getNodesForScope(RoutingScope scope) throws IOException {
    return getNodesForScope(scope, deadlineAfterMillis(pollingLimits.cycleTimeoutMillis));
  }

  private List<URI> getNodesForScope(RoutingScope scope, long cycleDeadline) throws IOException {
    boolean clusterScope = scope instanceof ClusterScope;
    String query = scope.getLocalNodesQuery();
    String requestQuery = query.isEmpty() ? null : query;
    List<URI> liveCandidates = liveDiscoveryCandidates();
    List<URI> seedCandidates = initialDiscoveryCandidates(liveCandidates);

    DiscoveryAttempt liveAttempt =
        discoverNodes(
            scope, liveCandidates, requestQuery, "live node", cycleDeadline, seedCandidates.size());
    if (!clusterScope && !liveAttempt.nodes.isEmpty()) {
      return liveAttempt.nodes;
    }
    if (deadlineExpired(cycleDeadline)) {
      if (clusterScope && !liveAttempt.nodes.isEmpty()) {
        return mergeClusterLastKnownGood(liveAttempt.nodes);
      }
      if (liveAttempt.authoritativeEmpty) {
        return Collections.emptyList();
      }
      throw deadlineExceeded("live-node discovery cycle");
    }

    DiscoveryAttempt seedAttempt =
        discoverNodes(scope, seedCandidates, requestQuery, "seed node", cycleDeadline, 0);
    if (!clusterScope && !seedAttempt.nodes.isEmpty()) {
      return seedAttempt.nodes;
    }

    if (clusterScope) {
      Set<URI> aggregate = new LinkedHashSet<>();
      long aggregateBytes = addToSnapshotWithinLimits(aggregate, liveAttempt.nodes, 0);
      addToSnapshotWithinLimits(aggregate, seedAttempt.nodes, aggregateBytes);
      if (!aggregate.isEmpty()) {
        if (liveAttempt.lastException != null
            || seedAttempt.lastException != null
            || liveAttempt.authoritativeEmpty
            || seedAttempt.authoritativeEmpty) {
          return mergeClusterLastKnownGood(new ArrayList<>(aggregate));
        }
        return new ArrayList<>(aggregate);
      }
    }

    if (liveAttempt.authoritativeEmpty || seedAttempt.authoritativeEmpty) {
      return Collections.emptyList();
    }
    if (seedAttempt.lastException != null) {
      throw seedAttempt.lastException;
    }
    if (liveAttempt.lastException != null) {
      throw liveAttempt.lastException;
    }
    return Collections.emptyList();
  }

  private List<URI> mergeClusterLastKnownGood(List<URI> discovered)
      throws DiscoverySnapshotLimitException {
    Set<URI> aggregate = new LinkedHashSet<>();
    long bytes = addToSnapshotWithinLimits(aggregate, discovered, 0);
    for (URI node : liveNodes.get()) {
      if (aggregate.contains(node)) {
        continue;
      }
      long nodeBytes = snapshotBytes(node);
      if (aggregate.size() >= pollingLimits.maxSnapshotNodes
          || bytes + nodeBytes > pollingLimits.maxSnapshotBytes) {
        continue;
      }
      aggregate.add(node);
      bytes += nodeBytes;
    }
    return new ArrayList<>(aggregate);
  }

  private DiscoveryAttempt discoverNodes(
      RoutingScope scope,
      List<URI> candidates,
      String requestQuery,
      String candidateDescription,
      long cycleDeadline,
      int followingCandidateCount)
      throws IOException {
    IOException lastException = null;
    boolean authoritativeEmpty = false;
    Set<URI> nodes = new LinkedHashSet<>();
    long snapshotBytes = 0;
    int failedCandidates = 0;
    URI firstFailedCandidate = null;
    for (int index = 0; index < candidates.size(); index++) {
      throwIfShutdownRequested();
      if (deadlineExpired(cycleDeadline)) {
        lastException = deadlineExceeded("live-node discovery cycle");
        break;
      }
      URI candidate = candidates.get(index);
      try {
        int remainingCandidates = candidates.size() - index + followingCandidateCount;
        long candidateDeadline =
            fairDeadline(cycleDeadline, pollingLimits.candidateTimeoutMillis, remainingCandidates);
        CandidateResult result =
            getNodes(
                withPathAndRawQuery(candidate, "/localnodes", requestQuery),
                candidateDeadline,
                initialNodeSet.contains(candidate));
        authoritativeEmpty |= result.sawEmptyResponse;
        if (result.nodes.isEmpty()) {
          authoritativeEmpty = true;
        } else {
          if (!(scope instanceof ClusterScope)) {
            ensureSnapshotWithinLimits(result.nodes);
            logCandidateFailures(
                scope, candidateDescription, failedCandidates, firstFailedCandidate, lastException);
            return new DiscoveryAttempt(result.nodes, lastException, authoritativeEmpty);
          }
          snapshotBytes = addToSnapshotWithinLimits(nodes, result.nodes, snapshotBytes);
        }
      } catch (DiscoverySnapshotLimitException e) {
        throw e;
      } catch (IOException e) {
        if (shutdownRequested.get()) {
          throw e;
        }
        failedCandidates++;
        if (firstFailedCandidate == null) {
          firstFailedCandidate = candidate;
        }
        lastException = e;
        if (deadlineExpired(cycleDeadline)) {
          break;
        }
      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      }
    }

    logCandidateFailures(
        scope, candidateDescription, failedCandidates, firstFailedCandidate, lastException);
    return new DiscoveryAttempt(new ArrayList<>(nodes), lastException, authoritativeEmpty);
  }

  private long addToSnapshotWithinLimits(
      Set<URI> aggregate, List<URI> discovered, long currentBytes)
      throws DiscoverySnapshotLimitException {
    Set<URI> additions = new LinkedHashSet<>();
    long addedBytes = 0;
    for (URI node : discovered) {
      if (aggregate.contains(node) || !additions.add(node)) {
        continue;
      }
      if (aggregate.size() + additions.size() > pollingLimits.maxSnapshotNodes) {
        throw snapshotLimitExceeded();
      }
      addedBytes += snapshotBytes(node);
      if (currentBytes + addedBytes > pollingLimits.maxSnapshotBytes) {
        throw snapshotLimitExceeded();
      }
    }
    aggregate.addAll(additions);
    return currentBytes + addedBytes;
  }

  private void ensureSnapshotWithinLimits(List<URI> nodes) throws DiscoverySnapshotLimitException {
    long bytes = 0;
    for (int index = 0; index < nodes.size(); index++) {
      if (index >= pollingLimits.maxSnapshotNodes) {
        throw snapshotLimitExceeded();
      }
      bytes += snapshotBytes(nodes.get(index));
      if (bytes > pollingLimits.maxSnapshotBytes) {
        throw snapshotLimitExceeded();
      }
    }
  }

  private DiscoverySnapshotLimitException snapshotLimitExceeded() {
    return new DiscoverySnapshotLimitException(
        "Discovered live-node snapshot exceeds "
            + pollingLimits.maxSnapshotNodes
            + " nodes or "
            + pollingLimits.maxSnapshotBytes
            + " bytes");
  }

  private static long snapshotBytes(URI node) {
    // URI.toASCIIString() contains only ASCII, so its character length is its encoded byte size.
    return node.toASCIIString().length() + 1;
  }

  private static void logCandidateFailures(
      RoutingScope scope,
      String candidateDescription,
      int failedCandidates,
      URI firstFailedCandidate,
      IOException lastException) {
    if (failedCandidates == 0) {
      return;
    }
    String cause =
        lastException == null
            ? "unknown"
            : lastException.getClass().getSimpleName()
                + ": "
                + LocalNodesResponseParser.boundedLogSample(lastException.getMessage());
    logger.log(
        Level.WARNING,
        "Failed to contact {0} {1}(s) for {2}; first candidate={3}; last failure={4}",
        new Object[] {
          failedCandidates,
          candidateDescription,
          scope.getDescription(),
          LocalNodesResponseParser.boundedLogSample(String.valueOf(firstFailedCandidate)),
          cause
        });
  }

  private List<URI> liveDiscoveryCandidates() {
    return new ArrayList<>(new LinkedHashSet<>(liveNodes.get()));
  }

  private List<URI> initialDiscoveryCandidates(List<URI> alreadyTried) {
    Set<URI> candidates = new LinkedHashSet<>(initialNodes);
    candidates.removeAll(alreadyTried);
    return new ArrayList<>(candidates);
  }

  private static class DiscoveryAttempt {
    final List<URI> nodes;
    final IOException lastException;
    final boolean authoritativeEmpty;

    DiscoveryAttempt(List<URI> nodes, IOException lastException, boolean authoritativeEmpty) {
      this.nodes = nodes;
      this.lastException = lastException;
      this.authoritativeEmpty = authoritativeEmpty;
    }
  }

  private static final class DiscoverySnapshotLimitException extends IOException {
    DiscoverySnapshotLimitException(String message) {
      super(message);
    }
  }

  private URI withPathAndRawQuery(URI uri, String path, String rawQuery) throws URISyntaxException {
    URI withoutQuery =
        new URI(
            uri.getScheme(),
            null,
            LogicalHost.withoutBrackets(uri.getHost()),
            uri.getPort(),
            path,
            null,
            null);
    if (rawQuery == null || rawQuery.isEmpty()) {
      return withoutQuery;
    }
    try {
      return new URI(withoutQuery.toASCIIString() + "?" + rawQuery);
    } catch (URISyntaxException e) {
      return new URI(
          uri.getScheme(),
          null,
          LogicalHost.withoutBrackets(uri.getHost()),
          uri.getPort(),
          path,
          rawQuery,
          null);
    }
  }

  private CandidateResult getNodes(URI uri, long candidateDeadline) throws IOException {
    return getNodes(uri, candidateDeadline, false);
  }

  private CandidateResult getNodes(URI uri, long candidateDeadline, boolean seedCandidate)
      throws IOException {
    SdkHttpRequest sdkRequest =
        SdkHttpRequest.builder()
            .uri(uri)
            .method(SdkHttpMethod.GET)
            .putHeader("Host", LogicalHost.authority(uri.getHost(), uri.getPort()))
            .putHeader("Connection", "keep-alive")
            .build();
    HttpExecuteRequest executeRequest = HttpExecuteRequest.builder().request(sdkRequest).build();
    if (pollingHttpClient instanceof DnsFallbackSdkHttpClient) {
      DnsFallbackSdkHttpClient dnsFallbackClient = (DnsFallbackSdkHttpClient) pollingHttpClient;
      if (dnsFallbackClient.supportsDnsFallback(uri.getScheme())) {
        return getNodesByResolvedAddress(
            uri, executeRequest, dnsFallbackClient, candidateDeadline, seedCandidate);
      }
    }
    long attemptDeadline = fairDeadline(candidateDeadline, pollingLimits.attemptTimeoutMillis, 1);
    remainingMillis(attemptDeadline, "/localnodes request preparation");
    return getNodes(
        prepareRequest(
            () -> pollingHttpClient.prepareRequest(executeRequest), "polling HTTP request"),
        attemptDeadline,
        seedCandidate);
  }

  private CandidateResult getNodesByResolvedAddress(
      URI logicalUri,
      HttpExecuteRequest executeRequest,
      DnsFallbackSdkHttpClient dnsFallbackClient,
      long candidateDeadline,
      boolean seedCandidate)
      throws IOException {
    String logicalHost = LogicalHost.withoutBrackets(logicalUri.getHost());
    List<InetAddress> resolvedAddresses;
    if (LogicalHost.isIpLiteral(logicalHost)) {
      resolvedAddresses = Collections.singletonList(InetAddress.getByName(logicalHost));
    } else {
      resolvedAddresses =
          dnsFallbackClient.resolve(
              logicalHost, remainingMillis(candidateDeadline, "DNS resolution"), seedCandidate);
    }
    throwIfShutdownRequested();
    if (resolvedAddresses == null) {
      throw new IOException("DNS resolver returned null for " + logicalUri.getHost());
    }
    Set<InetAddress> addresses = new LinkedHashSet<>();
    for (InetAddress address : resolvedAddresses) {
      if (address != null) {
        addresses.add(address);
        if (addresses.size() == pollingLimits.maxDnsAddresses) {
          break;
        }
      }
    }
    if (addresses.isEmpty()) {
      throw new IOException("DNS returned no addresses for " + logicalUri.getHost());
    }

    IOException lastException = null;
    boolean authoritativeEmpty = false;
    List<InetAddress> addressList = new ArrayList<>(addresses);
    for (int index = 0; index < addressList.size(); index++) {
      throwIfShutdownRequested();
      if (deadlineExpired(candidateDeadline)) {
        lastException = deadlineExceeded("/localnodes address attempts");
        break;
      }
      InetAddress address = addressList.get(index);
      try {
        long attemptDeadline =
            fairDeadline(
                candidateDeadline, pollingLimits.attemptTimeoutMillis, addressList.size() - index);
        remainingMillis(attemptDeadline, "/localnodes request preparation");
        CandidateResult result =
            getNodes(
                prepareRequest(
                    () -> dnsFallbackClient.prepareRequestForAddress(executeRequest, address),
                    "addressed polling HTTP request"),
                attemptDeadline,
                seedCandidate);
        if (!result.nodes.isEmpty()) {
          return new CandidateResult(result.nodes, authoritativeEmpty || result.sawEmptyResponse);
        }
        authoritativeEmpty = true;
        logger.log(
            Level.FINE,
            "/localnodes from "
                + logicalUri.getHost()
                + " at "
                + address.getHostAddress()
                + " returned no usable nodes; trying next DNS address");
      } catch (IOException e) {
        if (shutdownRequested.get()) {
          throw e;
        }
        lastException = e;
        logger.log(
            Level.FINE,
            "Failed /localnodes request to "
                + logicalUri.getHost()
                + " at "
                + address.getHostAddress()
                + "; trying next DNS address: "
                + e.getClass().getSimpleName()
                + ": "
                + LocalNodesResponseParser.boundedLogSample(e.getMessage()));
        if (deadlineExpired(candidateDeadline)) {
          break;
        }
      }
    }

    if (authoritativeEmpty) {
      return CandidateResult.authoritativeEmpty();
    }
    if (lastException != null) {
      throw lastException;
    }
    throw new IOException("No /localnodes address attempt completed for " + logicalUri.getHost());
  }

  private CandidateResult getNodes(
      ExecutableHttpRequest preparedRequest, long attemptDeadline, boolean seedCandidate)
      throws IOException {
    long timeoutMillis;
    try {
      timeoutMillis = remainingMillis(attemptDeadline, "/localnodes request");
    } catch (IOException | RuntimeException e) {
      abortPreparedRequest(preparedRequest);
      throw e;
    }
    BoundedPollingAttempt.Result response =
        BoundedPollingAttempt.execute(
            preparedRequest, timeoutMillis, pollingLimits.maxResponseBytes, seedCandidate);
    if (response.statusCode != 200) {
      throw new IOException("/localnodes returned HTTP status " + response.statusCode);
    }
    if (!response.isSuccess()) {
      throw new IOException("/localnodes returned HTTP 200 without a response body");
    }
    String responseBody = decodeUtf8(response.body);
    return new CandidateResult(parseLocalNodesResponse(responseBody));
  }

  private static ExecutableHttpRequest prepareRequest(
      RequestPreparation preparation, String description) throws IOException {
    try {
      ExecutableHttpRequest request = preparation.prepare();
      if (request == null) {
        throw new IOException("Failed to prepare " + description + ": client returned null");
      }
      return request;
    } catch (RuntimeException e) {
      throw new IOException("Failed to prepare " + description, e);
    }
  }

  private interface RequestPreparation {
    ExecutableHttpRequest prepare();
  }

  private static String decodeUtf8(byte[] responseBody) throws IOException {
    try {
      return StandardCharsets.UTF_8
          .newDecoder()
          .onMalformedInput(CodingErrorAction.REPORT)
          .onUnmappableCharacter(CodingErrorAction.REPORT)
          .decode(ByteBuffer.wrap(responseBody))
          .toString();
    } catch (CharacterCodingException e) {
      throw new IOException("/localnodes response is not valid UTF-8", e);
    }
  }

  private static void abortPreparedRequest(ExecutableHttpRequest preparedRequest) {
    try {
      preparedRequest.abort();
    } catch (RuntimeException ignored) {
      // Best-effort cleanup for a request that never reached the polling worker.
    }
  }

  private List<URI> parseLocalNodesResponse(String responseStr) throws IOException {
    try {
      return localNodesResponseParser.parse(responseStr);
    } catch (LocalNodesResponseParser.InvalidLocalNodesResponseException e) {
      String response = responseStr == null ? "" : responseStr;
      logger.log(
          Level.WARNING,
          "Malformed /localnodes response: characters={0}, hash={1}, sample={2}",
          new Object[] {
            response.length(),
            Integer.toHexString(response.hashCode()),
            LocalNodesResponseParser.boundedLogSample(response)
          });
      throw e;
    }
  }

  private static final class CandidateResult {
    final List<URI> nodes;
    final boolean sawEmptyResponse;

    CandidateResult(List<URI> nodes) {
      this(nodes, nodes.isEmpty());
    }

    CandidateResult(List<URI> nodes, boolean sawEmptyResponse) {
      this.nodes = nodes;
      this.sawEmptyResponse = sawEmptyResponse;
    }

    static CandidateResult authoritativeEmpty() {
      return new CandidateResult(Collections.emptyList(), true);
    }
  }

  private static long deadlineAfterMillis(long timeoutMillis) {
    return System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
  }

  private static List<RoutingScope> routingScopes(RoutingScope scope) throws IOException {
    List<RoutingScope> scopes = new ArrayList<>();
    Set<RoutingScope> seen = Collections.newSetFromMap(new IdentityHashMap<>());
    for (RoutingScope current = scope; current != null; current = current.getFallback()) {
      if (scopes.size() >= MAX_ROUTING_SCOPE_DEPTH) {
        throw new IOException(
            "Routing scope fallback chain exceeds " + MAX_ROUTING_SCOPE_DEPTH + " entries");
      }
      if (!seen.add(current)) {
        throw new IOException(
            "Routing scope fallback cycle detected at " + current.getDescription());
      }
      scopes.add(current);
    }
    return scopes;
  }

  private static RoutingScope clusterScopeInValidChain(RoutingScope scope) {
    Set<RoutingScope> seen = Collections.newSetFromMap(new IdentityHashMap<>());
    RoutingScope current = scope;
    RoutingScope clusterScope = null;
    int depth = 0;
    while (current != null && depth < MAX_ROUTING_SCOPE_DEPTH) {
      if (!seen.add(current)) {
        return null;
      }
      if (clusterScope == null && current instanceof ClusterScope) {
        clusterScope = current;
      }
      current = current.getFallback();
      depth++;
    }
    return current == null ? clusterScope : null;
  }

  private static long fairDeadline(long outerDeadline, long maximumMillis, int remainingItems)
      throws SocketTimeoutException {
    long now = System.nanoTime();
    long remainingNanos = outerDeadline - now;
    if (remainingNanos <= 0) {
      throw deadlineExceeded("live-node discovery cycle");
    }
    long fairNanos = remainingNanos / Math.max(1, remainingItems);
    fairNanos = Math.max(TimeUnit.MILLISECONDS.toNanos(1), fairNanos);
    long budgetNanos =
        Math.min(remainingNanos, Math.min(TimeUnit.MILLISECONDS.toNanos(maximumMillis), fairNanos));
    return now + budgetNanos;
  }

  private static long remainingMillis(long deadline, String operation)
      throws SocketTimeoutException {
    long remainingNanos = deadline - System.nanoTime();
    if (remainingNanos <= 0) {
      throw deadlineExceeded(operation);
    }
    return Math.max(1, TimeUnit.NANOSECONDS.toMillis(remainingNanos));
  }

  private static SocketTimeoutException deadlineExceeded(String operation) {
    return new SocketTimeoutException(operation + " exceeded its deadline");
  }

  private void throwIfShutdownRequested() throws InterruptedIOException {
    if (shutdownRequested.get()) {
      throw new InterruptedIOException("Live-node discovery was cancelled during shutdown");
    }
  }

  private static boolean deadlineExpired(long deadline) {
    return deadline - System.nanoTime() <= 0;
  }

  /**
   * Returns the polling HTTP client. Intended for testing only.
   *
   * @return the polling SdkHttpClient
   */
  SdkHttpClient getPollingHttpClient() {
    return pollingHttpClient;
  }

  /** Exception thrown when a check operation cannot be completed. */
  public static class FailedToCheck extends Exception {
    /**
     * Constructs a new FailedToCheck with the specified message and cause.
     *
     * @param message the detail message
     * @param cause the cause of this exception
     */
    public FailedToCheck(String message, Throwable cause) {
      super(message, cause);
    }

    /**
     * Constructs a new FailedToCheck with the specified message.
     *
     * @param message the detail message
     */
    public FailedToCheck(String message) {
      super(message);
    }
  }

  /**
   * Validates the server's node list for the configured routing scope. This method checks whether
   * the server returns a non-empty node list for the configured routing scope.
   *
   * <p>If the server returns a non-empty node list, no exception is thrown.
   *
   * @throws FailedToCheck if the server cannot be reached.
   * @throws ValidationError if the server returns an empty node list.
   * @since 1.0.1
   */
  public void checkIfRackAndDatacenterSetCorrectly() throws FailedToCheck, ValidationError {
    RoutingScope scope = this.config.getRoutingScope();
    String query = scope.getLocalNodesQuery();
    if (query.isEmpty()) {
      // ClusterScope - no filtering needed
      return;
    }
    try {
      List<URI> nodes = getNodesForScope(scope);
      if (nodes.isEmpty()) {
        throw new ValidationError(
            "node returned empty list for "
                + scope.getDescription()
                + ", routing scope may be set incorrectly");
      }
    } catch (IOException e) {
      throw new FailedToCheck("failed to read list of nodes from the node", e);
    }
  }

  /**
   * Returns true if remote node supports /localnodes?rack=`rack`&amp;dc=`datacenter`. If it can't
   * conclude by any reason it throws {@link FailedToCheck}.
   *
   * @return a {@link java.lang.Boolean} object
   * @throws FailedToCheck if the check cannot be completed.
   * @since 1.0.1
   */
  public Boolean checkIfRackDatacenterFeatureIsSupported() throws FailedToCheck {
    List<URI> candidates = liveDiscoveryCandidates();
    for (URI seed : initialDiscoveryCandidates(candidates)) {
      candidates.add(seed);
    }
    long cycleDeadline = deadlineAfterMillis(pollingLimits.cycleTimeoutMillis);
    IOException lastException = null;
    for (int index = 0; index < candidates.size(); index++) {
      URI candidate = candidates.get(index);
      try {
        long candidateDeadline =
            fairDeadline(
                cycleDeadline, pollingLimits.candidateTimeoutMillis, candidates.size() - index);
        URI uri = withPathAndRawQuery(candidate, "/localnodes", null);
        URI fakeRackUrl = withPathAndRawQuery(candidate, "/localnodes", "rack=fakeRack");
        List<URI> hostsWithFakeRack =
            getNodes(
                    fakeRackUrl,
                    fairDeadline(candidateDeadline, pollingLimits.attemptTimeoutMillis, 2),
                    initialNodeSet.contains(candidate))
                .nodes;
        List<URI> hostsWithoutRack =
            getNodes(
                    uri,
                    fairDeadline(candidateDeadline, pollingLimits.attemptTimeoutMillis, 1),
                    initialNodeSet.contains(candidate))
                .nodes;
        if (hostsWithoutRack.isEmpty()) {
          lastException = new IOException(String.format("host %s returned empty list", uri));
          continue;
        }
        // When rack filtering is not supported server returns same nodes.
        return hostsWithFakeRack.size() != hostsWithoutRack.size();
      } catch (IOException | URISyntaxException e) {
        lastException =
            e instanceof IOException
                ? (IOException) e
                : new IOException("Invalid discovery URI for " + candidate, e);
      }
    }
    throw new FailedToCheck(
        "failed to read list of nodes from every discovery candidate", lastException);
  }

  /**
   * Returns the routing scope configured for this instance.
   *
   * @return the routing scope (never null)
   * @since 2.0.0
   */
  public RoutingScope getRoutingScope() {
    return config.getRoutingScope();
  }

  /**
   * Returns the internal live nodes list directly. This is intended for use by {@link
   * LazyQueryPlan} to avoid copying the list on every access.
   *
   * <p>Note: The returned list should not be modified. It may be replaced atomically at any time by
   * the background refresh thread.
   *
   * <p>This method is protected to allow test mocks to override it.
   *
   * @return the current live nodes list (not a copy)
   */
  protected List<URI> getLiveNodesInternal() {
    return liveNodes.get();
  }

  /**
   * Returns a snapshot of the current live nodes list.
   *
   * @return an unmodifiable list of the current live node URIs
   * @since 2.0.0
   */
  public List<URI> getLiveNodes() {
    return Collections.unmodifiableList(new ArrayList<>(liveNodes.get()));
  }
}
