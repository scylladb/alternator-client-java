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
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
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
 * livenodes will be used, at round-robin order, for every connection. The list of live nodes starts
 * with one or more known nodes, but then a thread periodically replaces this list by an up-to-date
 * list retrieved from making a "/localnodes" requests to one of these nodes.
 *
 * @author dmitry.kropachev
 */
public class AlternatorLiveNodes extends Thread {
  private static final long DEFAULT_SHUTDOWN_TIMEOUT_MS = 5_000;

  private final AtomicReference<List<URI>> liveNodes;
  private final List<URI> initialNodes;
  private final AtomicInteger nextLiveNodeIndex;
  private final AlternatorConfig config;
  private final AtomicBoolean running = new AtomicBoolean(false);
  private final AtomicBoolean shutdownRequested = new AtomicBoolean(false);
  private final SdkHttpClient pollingHttpClient;
  private final boolean ownsPollingClient;
  private final AtomicBoolean pollingClientClosed = new AtomicBoolean(false);
  private final AtomicLong lastActivityTime = new AtomicLong(0);
  private final LocalNodesResponseParser localNodesResponseParser;
  private final LiveNodesPollingLimits pollingLimits;

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
          Thread.sleep(getRefreshInterval());
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
  private void markActivity() {
    lastActivityTime.set(System.currentTimeMillis());
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
    this.localNodesResponseParser =
        new LocalNodesResponseParser(config.getScheme(), config.getPort());
    this.initialNodes = hostsToUris(seedHosts);
    this.liveNodes = new AtomicReference<>();
    this.nextLiveNodeIndex = new AtomicInteger(0);
    this.config = config;
    this.pollingHttpClient = pollingHttpClient;
    this.ownsPollingClient = ownsPollingClient;
    this.pollingLimits = Objects.requireNonNull(pollingLimits, "pollingLimits");
    try {
      this.validate();
    } catch (ValidationError e) {
      throw new RuntimeException(e);
    }
    this.liveNodes.set(initialNodes);
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
    markActivity();
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
    RoutingScope scope = this.config.getRoutingScope();
    IOException lastException = null;
    long cycleDeadline = deadlineAfterMillis(pollingLimits.cycleTimeoutMillis);
    while (scope != null) {
      try {
        long scopeDeadline =
            fairDeadline(cycleDeadline, pollingLimits.cycleTimeoutMillis, countScopes(scope));
        List<URI> nodes = getNodesForScope(scope, scopeDeadline);
        if (!nodes.isEmpty()) {
          liveNodes.set(nodes);
          logger.log(
              Level.FINE, "Updated hosts to " + liveNodes + " using " + scope.getDescription());
          return;
        }
      } catch (IOException e) {
        logger.log(Level.WARNING, "Failed to discover nodes for " + scope.getDescription(), e);
        lastException = e;
      }
      RoutingScope fallback = scope.getFallback();
      if (fallback != null) {
        logger.log(
            Level.WARNING,
            "No nodes found for "
                + scope.getDescription()
                + ", falling back to "
                + fallback.getDescription());
      }
      scope = fallback;
    }
    // No nodes found in any scope - keep the current list. Initial seed nodes are retained
    // separately and remain discovery candidates without being injected into the routing list.
    if (lastException != null) {
      logger.log(
          Level.WARNING,
          "All nodes unreachable in every routing scope, keeping existing node list");
    } else {
      logger.log(Level.WARNING, "No nodes found in any routing scope, keeping existing node list");
    }
  }

  private List<URI> getNodesForScope(RoutingScope scope) throws IOException {
    return getNodesForScope(scope, deadlineAfterMillis(pollingLimits.cycleTimeoutMillis));
  }

  private List<URI> getNodesForScope(RoutingScope scope, long cycleDeadline) throws IOException {
    String query = scope.getLocalNodesQuery();
    String requestQuery = query.isEmpty() ? null : query;
    List<URI> liveCandidates = liveDiscoveryCandidates();
    List<URI> seedCandidates = initialDiscoveryCandidates(liveCandidates);

    DiscoveryAttempt liveAttempt =
        discoverNodes(
            scope, liveCandidates, requestQuery, "live node", cycleDeadline, seedCandidates.size());
    if (!liveAttempt.nodes.isEmpty()) {
      return liveAttempt.nodes;
    }
    if (deadlineExpired(cycleDeadline)) {
      if (liveAttempt.authoritativeEmpty) {
        return Collections.emptyList();
      }
      throw deadlineExceeded("live-node discovery cycle");
    }

    DiscoveryAttempt seedAttempt =
        discoverNodes(scope, seedCandidates, requestQuery, "seed node", cycleDeadline, 0);
    if (!seedAttempt.nodes.isEmpty()) {
      return seedAttempt.nodes;
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

  private DiscoveryAttempt discoverNodes(
      RoutingScope scope,
      List<URI> candidates,
      String requestQuery,
      String candidateDescription,
      long cycleDeadline,
      int followingCandidateCount) {
    IOException lastException = null;
    boolean authoritativeEmpty = false;
    Set<URI> nodes = new LinkedHashSet<>();
    int failedCandidates = 0;
    URI firstFailedCandidate = null;
    for (int index = 0; index < candidates.size(); index++) {
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
                withPathAndRawQuery(candidate, "/localnodes", requestQuery), candidateDeadline);
        if (result.nodes.isEmpty()) {
          authoritativeEmpty = true;
        } else {
          if (!(scope instanceof ClusterScope)) {
            logCandidateFailures(
                scope, candidateDescription, failedCandidates, firstFailedCandidate, lastException);
            return new DiscoveryAttempt(result.nodes, lastException, authoritativeEmpty);
          }
          nodes.addAll(result.nodes);
        }
      } catch (IOException e) {
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
        return getNodesByResolvedAddress(uri, executeRequest, dnsFallbackClient, candidateDeadline);
      }
    }
    long attemptDeadline = fairDeadline(candidateDeadline, pollingLimits.attemptTimeoutMillis, 1);
    remainingMillis(attemptDeadline, "/localnodes request preparation");
    return getNodes(pollingHttpClient.prepareRequest(executeRequest), attemptDeadline);
  }

  private CandidateResult getNodesByResolvedAddress(
      URI logicalUri,
      HttpExecuteRequest executeRequest,
      DnsFallbackSdkHttpClient dnsFallbackClient,
      long candidateDeadline)
      throws IOException {
    List<InetAddress> resolvedAddresses =
        dnsFallbackClient.resolve(
            LogicalHost.withoutBrackets(logicalUri.getHost()),
            remainingMillis(candidateDeadline, "DNS resolution"));
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
                dnsFallbackClient.prepareRequestForAddress(executeRequest, address),
                attemptDeadline);
        if (!result.nodes.isEmpty()) {
          return result;
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

  private CandidateResult getNodes(ExecutableHttpRequest preparedRequest, long attemptDeadline)
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
            preparedRequest, timeoutMillis, pollingLimits.maxResponseBytes);
    if (response.statusCode != 200) {
      throw new IOException("/localnodes returned HTTP status " + response.statusCode);
    }
    if (!response.isSuccess()) {
      throw new IOException("/localnodes returned HTTP 200 without a response body");
    }
    String responseBody = new String(response.body, StandardCharsets.UTF_8);
    return new CandidateResult(parseLocalNodesResponse(responseBody));
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

    CandidateResult(List<URI> nodes) {
      this.nodes = nodes;
    }

    static CandidateResult authoritativeEmpty() {
      return new CandidateResult(Collections.emptyList());
    }
  }

  private static long deadlineAfterMillis(long timeoutMillis) {
    return System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
  }

  private static int countScopes(RoutingScope scope) {
    int count = 0;
    for (RoutingScope current = scope; current != null; current = current.getFallback()) {
      count++;
    }
    return count;
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
    URI uri = nextAsURI("/localnodes", null);
    URI fakeRackUrl;
    try {
      fakeRackUrl =
          new URI(
              uri.getScheme(),
              null,
              LogicalHost.withoutBrackets(uri.getHost()),
              uri.getPort(),
              uri.getPath(),
              "rack=fakeRack",
              null);
    } catch (URISyntaxException e) {
      // Should not ever happen
      throw new FailedToCheck("Invalid URI: " + uri, e);
    }
    try {
      long cycleDeadline = deadlineAfterMillis(pollingLimits.cycleTimeoutMillis);
      List<URI> hostsWithFakeRack =
          getNodes(
                  fakeRackUrl, fairDeadline(cycleDeadline, pollingLimits.candidateTimeoutMillis, 2))
              .nodes;
      List<URI> hostsWithoutRack =
          getNodes(uri, fairDeadline(cycleDeadline, pollingLimits.candidateTimeoutMillis, 1)).nodes;
      if (hostsWithoutRack.isEmpty()) {
        // This should not normally happen.
        // If list of nodes is empty, it is impossible to conclude if it supports rack/datacenter
        // filtering or not.
        throw new FailedToCheck(String.format("host %s returned empty list", uri));
      }
      // When rack filtering is not supported server returns same nodes.
      return hostsWithFakeRack.size() != hostsWithoutRack.size();
    } catch (IOException e) {
      throw new FailedToCheck("failed to read list of nodes from the node", e);
    }
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
