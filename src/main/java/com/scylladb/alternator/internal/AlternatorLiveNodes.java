package com.scylladb.alternator.internal;

import com.scylladb.alternator.AlternatorConfig;
import com.scylladb.alternator.NodeHealthObservation;
import com.scylladb.alternator.NodeHealthState;
import com.scylladb.alternator.NodeHealthStatus;
import com.scylladb.alternator.routing.ClusterScope;
import com.scylladb.alternator.routing.DatacenterScope;
import com.scylladb.alternator.routing.RackScope;
import com.scylladb.alternator.routing.RoutingScope;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;

/**
 * Maintains and automatically updates a list of discovered Alternator nodes in the configured
 * routing scope. Node health is tracked separately from discovery; query plans start from
 * discovered nodes and apply health rules only when choosing nodes for routing.
 *
 * @author dmitry.kropachev
 */
public class AlternatorLiveNodes extends Thread {
  private static final long DEFAULT_SHUTDOWN_TIMEOUT_MS = 5_000;

  private final String alternatorScheme;
  private final int alternatorPort;

  /**
   * Nodes discovered for the configured routing scope.
   *
   * <p>This is not a health-filtered live-node list. Health state lives in {@link #healthStore};
   * down and quarantined nodes can remain here so key-affinity hashing uses a stable scoped node
   * ring. Initial seed nodes are kept separately and used as discovery fallbacks without being
   * published into this routing ring unless they are returned by discovery for the configured
   * scope. Query plans later skip down nodes and sample quarantined nodes according to the
   * configured quarantine interval.
   */
  private final AtomicReference<List<URI>> discoveredNodes;

  private final List<URI> initialNodes;
  private final AlternatorConfig config;
  private final AtomicBoolean running = new AtomicBoolean(false);
  private final AtomicBoolean shutdownRequested = new AtomicBoolean(false);
  private final AtomicBoolean pollingClientClosed = new AtomicBoolean(false);
  private final SdkHttpClient pollingHttpClient;
  private final boolean ownsPollingClient;
  private final AtomicLong lastActivityTime = new AtomicLong(0);
  private final NodeHealthStore healthStore;
  private final AtomicLong quarantineSamplingCounter = new AtomicLong(0);

  private static Logger logger = Logger.getLogger(AlternatorLiveNodes.class.getName());

  /** {@inheritDoc} */
  @Override
  public void run() {
    logger.log(Level.INFO, "AlternatorLiveNodes thread started");
    running.set(true);
    try {
      long nextRefreshAt = 0;
      long probePeriodMs = config.getNodeHealthConfig().getDownNodeProbePeriodMs();
      long nextDownProbeAt =
          probePeriodMs > 0 ? System.currentTimeMillis() + probePeriodMs : Long.MAX_VALUE;
      while (!shutdownRequested.get()) {
        long now = System.currentTimeMillis();
        if (now >= nextRefreshAt) {
          try {
            refreshDiscoveredNodes();
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
          } finally {
            nextRefreshAt = System.currentTimeMillis() + getRefreshInterval();
          }
        }
        if (probePeriodMs > 0 && now >= nextDownProbeAt) {
          try {
            runDownNodeProbes();
          } catch (RuntimeException e) {
            if (shutdownRequested.get()) {
              logger.log(
                  Level.FINE, "AlternatorLiveNodes down-node probing stopped during shutdown", e);
              return;
            }
            logger.log(Level.SEVERE, "AlternatorLiveNodes down-node probing failed", e);
          } finally {
            nextDownProbeAt = System.currentTimeMillis() + probePeriodMs;
          }
        }
        try {
          long wakeAt = Math.min(nextRefreshAt, nextDownProbeAt);
          long sleepMs = Math.max(1, wakeAt - System.currentTimeMillis());
          Thread.sleep(sleepMs);
        } catch (InterruptedException e) {
          if (shutdownRequested.get()) {
            logger.log(Level.INFO, "AlternatorLiveNodes thread interrupted and stopping");
            Thread.currentThread().interrupt(); // Restore interrupted status
            return;
          }
          logger.log(Level.FINE, "AlternatorLiveNodes thread interrupted without shutdown request");
          nextRefreshAt = 0;
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
      boolean stopped = !isAlive();
      if (stopped) {
        closePollingClient();
      }
      return stopped;
    }
    try {
      join(timeoutMs);
      boolean stopped = !isAlive();
      if (stopped) {
        closePollingClient();
      }
      return stopped;
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
            .withHeadersWhitelist(config.getHeadersWhitelist())
            .withNodeHealthConfig(config.getNodeHealthConfig());
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
    this(config, createDefaultPollingClient(config), true);
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
    this(config, pollingHttpClient, false);
  }

  private AlternatorLiveNodes(
      AlternatorConfig config, SdkHttpClient pollingHttpClient, boolean ownsPollingClient) {
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
    this.alternatorScheme = config.getScheme();
    this.alternatorPort = config.getPort();
    this.initialNodes = dedupePreservingOrder(hostsToUris(seedHosts));
    this.discoveredNodes = new AtomicReference<>();
    this.config = config;
    this.pollingHttpClient = pollingHttpClient;
    this.ownsPollingClient = ownsPollingClient;
    this.healthStore = new NodeHealthStore(config.getNodeHealthConfig(), initialNodes);
    try {
      this.validate();
    } catch (ValidationError e) {
      throw new RuntimeException(e);
    }
    this.discoveredNodes.set(initialNodes);
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
      // Make sure that `alternatorScheme` and `alternatorPort` are correct values
      this.hostToURI("1.1.1.1");
    } catch (MalformedURLException | URISyntaxException e) {
      throw new ValidationError("failed to validate configuration", e);
    }
  }

  private URI hostToURI(String host) throws URISyntaxException, MalformedURLException {
    URI uri = new URI(alternatorScheme, null, host, alternatorPort, null, null, null);
    // Make sure that URI to URL conversion works
    uri.toURL();
    return uri;
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
   * Returns the next node URI using the current query-plan selection.
   *
   * <p>This method is retained for source and binary compatibility with callers compiled against
   * versions that exposed direct node selection. New routing code should use {@link LazyQueryPlan}
   * so request retries get a per-request plan.
   *
   * @return a selected node URI
   * @deprecated Use {@link LazyQueryPlan} for request routing.
   */
  @Deprecated
  public URI nextAsURI() {
    markActivity();
    LazyQueryPlan queryPlan = new LazyQueryPlan(this);
    QueryPlanNodeFilter filter = newQueryPlanNodeFilter();
    URI node = filter.nextRouteCandidate(queryPlan, new ArrayDeque<>());
    if (node != null) {
      return node;
    }
    throw new IllegalStateException("No live nodes available");
  }

  /**
   * Returns the next node URI with the provided path and query.
   *
   * @param path URI path
   * @param query URI query
   * @return a selected node URI with the provided path and query
   * @deprecated Use {@link LazyQueryPlan} for request routing and compose request paths through the
   *     SDK.
   */
  @Deprecated
  public URI nextAsURI(String path, String query) {
    try {
      return withPathAndQuery(nextAsURI(), path, query);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  private URI withPathAndQuery(URI uri, String path, String query) throws URISyntaxException {
    return new URI(uri.getScheme(), null, uri.getHost(), uri.getPort(), path, query, null);
  }

  // Utility function for reading the entire contents of an input stream
  // (which we assume will be fairly short)
  private static String streamToString(InputStream stream) throws IOException {
    if (stream == null) {
      return "";
    }
    Scanner s = new Scanner(stream).useDelimiter("\\A");
    String result = s.hasNext() ? s.next() : "";
    stream.close();
    return result;
  }

  /**
   * Refreshes discovered nodes from {@code /localnodes} for the configured routing scope.
   *
   * <p>This updates the discovered-node candidate set only. Per-node health state is maintained
   * separately and synchronized by {@link #setDiscoveredNodes(List)}.
   *
   * @throws IOException reserved for discovery implementations that surface polling failures
   */
  void refreshDiscoveredNodes() throws IOException {
    RoutingScope scope = this.config.getRoutingScope();
    IOException lastException = null;
    while (scope != null) {
      try {
        List<URI> nodes = getNodesForScope(scope);
        if (!nodes.isEmpty()) {
          setDiscoveredNodes(nodes);
          logger.log(
              Level.FINE,
              "Updated discovered nodes to "
                  + discoveredNodes.get()
                  + " using "
                  + scope.getDescription());
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
    // No nodes found in any scope - keep the current routing list. Initial seed nodes remain
    // available as discovery fallback candidates, but are not injected into the routing ring.
    if (lastException != null) {
      logger.log(
          Level.WARNING,
          "All nodes unreachable in every routing scope, keeping existing discovered node list");
    } else {
      logger.log(Level.WARNING, "No nodes found in any routing scope, keeping existing node list");
    }
  }

  void updateLiveNodes() throws IOException {
    refreshDiscoveredNodes();
  }

  /**
   * Publishes the discovered node set and synchronizes node-health bookkeeping.
   *
   * <p>The stored list is sorted and deduplicated on every discovery update. Newly discovered nodes
   * are added to the health store as active. Nodes that disappear from one discovery response keep
   * their health state so a later rediscovery cannot silently resurrect a down node as active.
   *
   * @param nodes discovered node candidates for the configured routing scope
   */
  private void setDiscoveredNodes(List<URI> nodes) {
    List<URI> deduped = sortAndDedupeNodes(nodes);
    for (URI node : deduped) {
      healthStore.addNode(node);
    }
    discoveredNodes.set(deduped);
  }

  private List<URI> getNodesForScope(RoutingScope scope) throws IOException {
    String query = scope.getLocalNodesQuery();
    String requestQuery = query.isEmpty() ? null : query;

    DiscoveryAttempt liveAttempt =
        discoverNodes(scope, liveDiscoveryCandidates(), requestQuery, "live node");
    if (!liveAttempt.nodes.isEmpty()) {
      return liveAttempt.nodes;
    }

    DiscoveryAttempt seedAttempt =
        discoverNodes(
            scope, initialDiscoveryCandidates(liveAttempt.candidates), requestQuery, "seed node");
    if (!seedAttempt.nodes.isEmpty()) {
      return seedAttempt.nodes;
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
      RoutingScope scope, List<URI> candidates, String requestQuery, String candidateDescription) {
    IOException lastException = null;
    Set<URI> nodes = new LinkedHashSet<>();
    for (URI candidate : candidates) {
      try {
        List<URI> discoveredNodes =
            getNodes(withPathAndRawQuery(candidate, "/localnodes", requestQuery));
        reportNodeResult(candidate, NodeHealthObservation.PROBE_SUCCESS, false);
        if (!discoveredNodes.isEmpty()) {
          if (!(scope instanceof ClusterScope)) {
            return new DiscoveryAttempt(candidates, discoveredNodes, lastException);
          }
          nodes.addAll(discoveredNodes);
        }
      } catch (IOException e) {
        lastException = recordDiscoveryFailure(scope, candidate, candidateDescription, e);
      } catch (RuntimeException e) {
        lastException = recordDiscoveryFailure(scope, candidate, candidateDescription, e);
      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      }
    }

    return new DiscoveryAttempt(candidates, new ArrayList<>(nodes), lastException);
  }

  private IOException recordDiscoveryFailure(
      RoutingScope scope, URI candidate, String candidateDescription, Exception failure) {
    reportNodeResult(candidate, NodeHealthObservation.PROBE_FAILURE, false);
    logger.log(
        Level.WARNING,
        "Failed to contact "
            + candidateDescription
            + " "
            + candidate
            + " for "
            + scope.getDescription(),
        failure);
    if (failure instanceof IOException) {
      return (IOException) failure;
    }
    return new IOException(
        "runtime failure contacting " + candidateDescription + " " + candidate, failure);
  }

  private List<URI> liveDiscoveryCandidates() {
    return filterDownDiscoveryNodes(dedupePreservingOrder(getDiscoveredNodesInternal()));
  }

  private List<URI> initialDiscoveryCandidates(List<URI> alreadyTried) {
    Set<URI> candidates = new LinkedHashSet<>(initialNodes);
    candidates.removeAll(alreadyTried);
    return filterDownDiscoveryNodes(new ArrayList<>(candidates));
  }

  private static class DiscoveryAttempt {
    final List<URI> candidates;
    final List<URI> nodes;
    final IOException lastException;

    DiscoveryAttempt(List<URI> candidates, List<URI> nodes, IOException lastException) {
      this.candidates = candidates;
      this.nodes = nodes;
      this.lastException = lastException;
    }
  }

  private URI withPathAndRawQuery(URI uri, String path, String rawQuery) throws URISyntaxException {
    URI withoutQuery =
        new URI(uri.getScheme(), null, uri.getHost(), uri.getPort(), path, null, null);
    if (rawQuery == null || rawQuery.isEmpty()) {
      return withoutQuery;
    }
    try {
      return new URI(withoutQuery.toASCIIString() + "?" + rawQuery);
    } catch (URISyntaxException e) {
      return new URI(uri.getScheme(), null, uri.getHost(), uri.getPort(), path, rawQuery, null);
    }
  }

  private List<URI> getNodes(URI uri) throws IOException {
    HttpExecuteResponse response = executeGet(uri);

    try {
      int statusCode = response.httpResponse().statusCode();
      if (statusCode != HttpURLConnection.HTTP_OK) {
        response.responseBody().ifPresent(this::consumeAndClose);
        throw new HttpStatusException(uri, statusCode);
      }

      Optional<AbortableInputStream> bodyOpt = response.responseBody();
      if (!bodyOpt.isPresent()) {
        return Collections.emptyList();
      }

      String responseStr;
      try (AbortableInputStream body = bodyOpt.get()) {
        responseStr = streamToString(body);
      }

      return parseLocalNodesResponse(responseStr);
    } catch (HttpStatusException e) {
      throw e;
    } catch (IOException e) {
      response.responseBody().ifPresent(this::consumeAndClose);
      throw e;
    }
  }

  private int getHttpStatus(URI uri) throws IOException {
    HttpExecuteResponse response = executeGet(uri);
    try {
      int statusCode = response.httpResponse().statusCode();
      response.responseBody().ifPresent(this::consumeAndClose);
      return statusCode;
    } catch (RuntimeException e) {
      response.responseBody().ifPresent(this::consumeAndClose);
      throw e;
    }
  }

  private HttpExecuteResponse executeGet(URI uri) throws IOException {
    SdkHttpRequest sdkRequest =
        SdkHttpRequest.builder()
            .uri(uri)
            .method(SdkHttpMethod.GET)
            .putHeader("Host", uri.getHost() + ":" + uri.getPort())
            .putHeader("Connection", "keep-alive")
            .build();
    HttpExecuteRequest executeRequest = HttpExecuteRequest.builder().request(sdkRequest).build();
    ExecutableHttpRequest preparedRequest = pollingHttpClient.prepareRequest(executeRequest);
    return preparedRequest.call();
  }

  private List<URI> parseLocalNodesResponse(String responseStr) throws IOException {
    responseStr = responseStr != null ? responseStr.trim() : "";
    if (!responseStr.startsWith("[") || !responseStr.endsWith("]")) {
      throw new InvalidLocalNodesResponseException("invalid /localnodes JSON response");
    }

    String responseBody = responseStr.substring(1, responseStr.length() - 1).trim();
    if (responseBody.isEmpty()) {
      return Collections.emptyList();
    }

    String[] list = responseBody.split(",");
    List<URI> newHosts = new ArrayList<>();
    for (String host : list) {
      host = host.trim();
      if (host.length() < 2 || !host.startsWith("\"") || !host.endsWith("\"")) {
        throw new InvalidLocalNodesResponseException("invalid host in /localnodes JSON response");
      }
      host = host.substring(1, host.length() - 1);
      try {
        newHosts.add(this.hostToURI(host));
      } catch (URISyntaxException | MalformedURLException e) {
        logger.log(Level.WARNING, "Invalid host: " + host, e);
      }
    }
    return newHosts;
  }

  private static class HttpStatusException extends IOException {
    private final int statusCode;

    private HttpStatusException(URI uri, int statusCode) {
      super("non-200 response from " + uri + ": " + statusCode);
      this.statusCode = statusCode;
    }
  }

  private static class InvalidLocalNodesResponseException extends IOException {
    private InvalidLocalNodesResponseException(String message) {
      super(message);
    }
  }

  /**
   * Consumes and closes an AbortableInputStream to release the underlying connection back to the
   * pool.
   */
  private void consumeAndClose(AbortableInputStream stream) {
    try {
      // Read remaining bytes to ensure the connection can be reused
      byte[] buf = new byte[1024];
      while (stream.read(buf) != -1) {
        // discard
      }
      stream.close();
    } catch (IOException e) {
      try {
        stream.abort();
      } catch (Exception abortEx) {
        logger.log(Level.WARNING, "Failed to abort AbortableInputStream during cleanup", abortEx);
      }
    }
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
    markActivity();
    LazyQueryPlan queryPlan = new LazyQueryPlan(this);
    QueryPlanNodeFilter filter = newQueryPlanNodeFilter();
    FailedToCheck lastFailure = null;
    while (queryPlan.hasNext()) {
      URI selected = queryPlan.next();
      if (selected == null) {
        lastFailure = new FailedToCheck("query plan returned null node");
        continue;
      }
      if (!filter.shouldRouteTo(selected)) {
        continue;
      }

      URI uri;
      try {
        uri = withPathAndQuery(selected, "/localnodes", null);
      } catch (URISyntaxException e) {
        lastFailure = new FailedToCheck("Invalid URI selected by query plan: " + selected, e);
        continue;
      }

      try {
        return checkIfRackDatacenterFeatureIsSupported(selected, uri);
      } catch (FailedToCheck e) {
        lastFailure = e;
      }
    }
    throw lastFailure != null ? lastFailure : new FailedToCheck("No live nodes available");
  }

  private Boolean checkIfRackDatacenterFeatureIsSupported(URI node, URI uri) throws FailedToCheck {
    URI fakeRackUrl;
    try {
      fakeRackUrl =
          new URI(
              uri.getScheme(),
              null,
              uri.getHost(),
              uri.getPort(),
              uri.getPath(),
              "rack=fakeRack",
              null);
    } catch (URISyntaxException e) {
      // Should not ever happen
      throw new FailedToCheck("Invalid URI: " + uri, e);
    }
    try {
      List<URI> hostsWithFakeRack = getNodes(fakeRackUrl);
      List<URI> hostsWithoutRack = getNodes(uri);
      if (hostsWithoutRack.isEmpty()) {
        // This should not normally happen.
        // If list of nodes is empty, it is impossible to conclude if it supports rack/datacenter
        // filtering or not.
        throw new FailedToCheck(String.format("host %s returned empty list", uri));
      }
      reportNodeResult(node, NodeHealthObservation.PROBE_SUCCESS);
      // When rack filtering is not supported server returns same nodes.
      return hostsWithFakeRack.size() != hostsWithoutRack.size();
    } catch (IOException | RuntimeException e) {
      reportNodeResult(node, NodeHealthObservation.PROBE_FAILURE);
      throw new FailedToCheck("failed to read list of nodes from the node", e);
    }
  }

  /**
   * Returns active nodes eligible for normal routing.
   *
   * @return active node URIs
   * @since 2.0.6
   */
  public List<URI> getActiveNodes() {
    return Collections.unmodifiableList(new ArrayList<>(getActiveNodesInternal()));
  }

  /**
   * Returns quarantined nodes eligible for sampled verification traffic.
   *
   * @return quarantined node URIs
   * @since 2.0.6
   */
  public List<URI> getQuarantinedNodes() {
    return Collections.unmodifiableList(new ArrayList<>(getQuarantinedNodesInternal()));
  }

  /**
   * Returns down nodes excluded from normal routing.
   *
   * @return down node URIs
   * @since 2.0.6
   */
  public List<URI> getDownNodes() {
    return Collections.unmodifiableList(new ArrayList<>(getDownNodesInternal()));
  }

  /**
   * Returns a node health status snapshot.
   *
   * @param node node URI
   * @return status snapshot, or null when the node is unknown
   * @since 2.0.6
   */
  public NodeHealthStatus getNodeHealthStatus(URI node) {
    return healthStore.getNodeStatus(node);
  }

  /**
   * Reports a node request outcome to the health tracker.
   *
   * @param node node URI
   * @param observation observed request result
   * @since 2.0.6
   */
  public void reportNodeResult(URI node, NodeHealthObservation observation) {
    reportNodeResult(node, observation, true);
  }

  private void reportNodeResult(URI node, NodeHealthObservation observation, boolean markActivity) {
    if (markActivity) {
      markActivity();
    }
    healthStore.reportNodeResult(node, observation);
  }

  /** Runs one background probe cycle for down nodes with {@code GET /localnodes}. */
  List<URI> runDownNodeProbes() {
    return healthStore.probeDownNodes(
        getDownNodeProbeCandidates(),
        (node, status) -> {
          try {
            int statusCode = getHttpStatus(withPathAndQuery(node, "/localnodes", null));
            return statusCode == HttpURLConnection.HTTP_OK
                ? NodeHealthObservation.PROBE_SUCCESS
                : NodeHealthObservation.PROBE_FAILURE;
          } catch (IOException | RuntimeException | URISyntaxException e) {
            return NodeHealthObservation.PROBE_FAILURE;
          }
        });
  }

  private List<URI> getDownNodeProbeCandidates() {
    List<URI> candidates = dedupePreservingOrder(getDiscoveredNodesInternal());
    appendUniqueNodes(candidates, initialNodes);
    return candidates;
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
   * Returns nodes for a normal request query plan.
   *
   * <p>The returned list contains all known discovered candidates. Health filtering is
   * intentionally applied at the last routing moment by {@link QueryPlanNodeFilter} so plan
   * ordering remains stable while node health changes.
   *
   * @return query-plan node URIs
   * @since 2.0.6
   */
  public List<URI> getQueryPlanNodes() {
    return getDiscoveredNodesForAffinityQueryPlan();
  }

  /**
   * Returns nodes for a partition-key hash query plan.
   *
   * <p>The candidate order is the seeded affinity order over all known discovered nodes. Health
   * filtering is intentionally applied at the last routing moment by {@link QueryPlanNodeFilter}.
   *
   * @param hash partition-key hash
   * @return query-plan node URIs
   * @since 2.0.6
   */
  public List<URI> getQueryPlanNodesForHash(long hash) {
    return drainSeeded(getDiscoveredNodesForAffinityQueryPlan(), hash);
  }

  List<URI> getQueryPlanNodesWithPreferredNodes(List<URI> preferredNodes) {
    if (preferredNodes == null) {
      throw new IllegalArgumentException("preferredNodes cannot be null");
    }
    return orderPreferredNodesFirst(getDiscoveredNodesForAffinityQueryPlan(), preferredNodes);
  }

  /**
   * Returns the first known node for a partition-key hash without consuming quarantine sampling.
   *
   * <p>This is used by batch-write key-affinity vote aggregation, where each item contributes its
   * preferred coordinator but the batch as a whole gets one query plan. Health filtering is applied
   * later by {@link QueryPlanNodeFilter}.
   *
   * @param hash partition-key hash
   * @return the preferred node, or null when no candidates exist
   */
  public URI getPreferredQueryPlanNodeForHash(long hash) {
    List<URI> candidates = drainSeeded(getDiscoveredNodesForAffinityQueryPlan(), hash);
    return candidates.isEmpty() ? null : candidates.get(0);
  }

  /**
   * Returns discovered scoped nodes in the canonical order used to seed key-affinity plans.
   *
   * @return sorted discovered nodes for affinity hashing
   */
  List<URI> getDiscoveredNodesForAffinityQueryPlan() {
    return sortAndDedupeNodes(getDiscoveredNodesInternal());
  }

  private NodeHealthState getQueryPlanNodeState(URI node) {
    NodeHealthStatus status = healthStore.getNodeStatus(node);
    return status != null ? status.getState() : NodeHealthState.ACTIVE;
  }

  private boolean shouldSampleQuarantinedNodeForRouting() {
    return shouldSampleQuarantinedNode(getActiveNodesInternal().isEmpty());
  }

  /** Creates a per-request node-health filter for query-plan candidates. */
  public QueryPlanNodeFilter newQueryPlanNodeFilter() {
    return new QueryPlanNodeFilter();
  }

  /**
   * Per-request final health gate for query-plan candidates.
   *
   * <p>{@link LazyQueryPlan} keeps all known nodes in candidate order. This filter decides, at the
   * last routing moment, whether a candidate can receive the request attempt. If no active nodes
   * remain, the filter fails open and allows all known candidates without mutating their stored
   * health state.
   */
  public final class QueryPlanNodeFilter {
    private Boolean sampleQuarantinedNode;
    private boolean usedQuarantinedNode;

    private QueryPlanNodeFilter() {}

    /**
     * Returns true when a query attempt may be routed to the candidate.
     *
     * <p>Active nodes are always allowed. When no active nodes exist, all known nodes are allowed
     * as an emergency fallback. Otherwise, down nodes are skipped, and at most one quarantined node
     * is allowed when the quarantine sampling policy admits it for this request.
     *
     * @param node candidate node
     * @return true if the request may be routed to this node
     */
    public boolean shouldRouteTo(URI node) {
      if (node == null) {
        return false;
      }
      NodeHealthState state = getQueryPlanNodeState(node);
      if (activeNodesEmpty()) {
        return true;
      }
      if (state == NodeHealthState.ACTIVE) {
        return true;
      }
      if (state != NodeHealthState.QUARANTINED || usedQuarantinedNode) {
        return false;
      }
      if (!shouldPreferQuarantinedNode()) {
        return false;
      }
      usedQuarantinedNode = true;
      return true;
    }

    /**
     * Returns the next node this request should route to, preserving skipped active candidates for
     * later attempts.
     *
     * @param candidates query-plan candidates
     * @param skippedCandidates active candidates delayed while checking quarantine sampling
     * @return the next routable node, or null when none is available
     */
    public URI nextRouteCandidate(Iterator<URI> candidates, Deque<URI> skippedCandidates) {
      Deque<URI> delayedCandidates =
          skippedCandidates != null ? skippedCandidates : new ArrayDeque<>();
      URI skippedCandidate = pollSkippedRouteCandidate(delayedCandidates);
      if (skippedCandidate != null || candidates == null) {
        return skippedCandidate;
      }

      boolean hasQuarantinedCandidate = hasQuarantinedCandidate();
      while (candidates.hasNext()) {
        URI candidate = candidates.next();
        if (!shouldRouteTo(candidate)) {
          continue;
        }
        if (!hasQuarantinedCandidate || isQuarantinedNode(candidate)) {
          return candidate;
        }
        delayedCandidates.addLast(candidate);
      }
      return pollSkippedRouteCandidate(delayedCandidates);
    }

    private URI pollSkippedRouteCandidate(Deque<URI> skippedCandidates) {
      while (skippedCandidates != null && !skippedCandidates.isEmpty()) {
        URI candidate = skippedCandidates.removeFirst();
        if (shouldRouteTo(candidate)) {
          return candidate;
        }
      }
      return null;
    }

    private boolean shouldPreferQuarantinedNode() {
      if (activeNodesEmpty()) {
        return false;
      }
      if (sampleQuarantinedNode == null) {
        sampleQuarantinedNode = shouldSampleQuarantinedNodeForRouting();
      }
      return sampleQuarantinedNode;
    }

    private boolean hasQuarantinedCandidate() {
      return !activeNodesEmpty() && !getQuarantinedNodesInternal().isEmpty();
    }

    private boolean isQuarantinedNode(URI node) {
      return node != null && getQueryPlanNodeState(node) == NodeHealthState.QUARANTINED;
    }

    private boolean activeNodesEmpty() {
      return getActiveNodesInternal().isEmpty();
    }
  }

  private static List<URI> orderPreferredNodesFirst(List<URI> sortedNodes, List<URI> preferred) {
    List<URI> ordered = new ArrayList<>(sortedNodes.size());
    Map<URI, URI> availableNodes = new HashMap<>();
    for (URI node : sortedNodes) {
      availableNodes.putIfAbsent(NodeHealthStore.canonicalNodeKey(node), node);
    }
    Set<URI> orderedNodes = new HashSet<>();
    for (URI preferredNode : preferred) {
      URI key = NodeHealthStore.canonicalNodeKey(preferredNode);
      URI node = availableNodes.get(key);
      if (node != null && orderedNodes.add(key)) {
        ordered.add(node);
      }
    }
    for (URI node : sortedNodes) {
      if (orderedNodes.add(NodeHealthStore.canonicalNodeKey(node))) {
        ordered.add(node);
      }
    }
    return ordered;
  }

  /**
   * Returns the internal discovered nodes list directly. This is intended for use by {@link
   * LazyQueryPlan} to avoid copying the list on every access.
   *
   * <p>Note: The returned list should not be modified. It may be replaced atomically at any time by
   * the background refresh thread. Discovery updates publish sorted lists, while the initial seed
   * list preserves configured seed order until the first successful update.
   *
   * <p>This method is protected to allow test mocks to override it.
   *
   * <p>The default implementation intentionally delegates to {@link #getLiveNodesInternal()} so
   * existing subclasses that override the deprecated hook continue to feed query planning.
   *
   * @return the current discovered nodes list (not a copy)
   */
  protected List<URI> getDiscoveredNodesInternal() {
    return getLiveNodesInternal();
  }

  /**
   * Returns the internal discovered nodes list directly.
   *
   * <p>This method is retained for source and binary compatibility with subclasses compiled against
   * versions where the raw discovered-node hook used this name.
   *
   * @return the current discovered nodes list (not a copy)
   * @deprecated Use {@link #getDiscoveredNodesInternal()} instead.
   */
  @Deprecated
  protected List<URI> getLiveNodesInternal() {
    return discoveredNodes.get();
  }

  protected List<URI> getActiveNodesInternal() {
    List<URI> activeNodes = new ArrayList<>();
    for (URI node : getDiscoveredNodesInternal()) {
      NodeHealthStatus status = healthStore.getNodeStatus(node);
      if (status == null || status.getState() == NodeHealthState.ACTIVE) {
        activeNodes.add(node);
      }
    }
    return sortAndDedupeNodes(activeNodes);
  }

  private List<URI> getQuarantinedNodesInternal() {
    return getDiscoveredNodesByState(NodeHealthState.QUARANTINED);
  }

  private List<URI> getDownNodesInternal() {
    return getDiscoveredNodesByState(NodeHealthState.DOWN);
  }

  private List<URI> getDiscoveredNodesByState(NodeHealthState state) {
    List<URI> nodes = new ArrayList<>();
    for (URI node : getDiscoveredNodesInternal()) {
      NodeHealthStatus status = healthStore.getNodeStatus(node);
      if (status != null && status.getState() == state) {
        nodes.add(node);
      }
    }
    return sortAndDedupeNodes(nodes);
  }

  private List<URI> filterDownDiscoveryNodes(List<URI> nodes) {
    List<URI> filtered = new ArrayList<>();
    for (URI node : nodes) {
      if (getQueryPlanNodeState(node) != NodeHealthState.DOWN) {
        filtered.add(node);
      }
    }
    return filtered;
  }

  private boolean shouldSampleQuarantinedNode(boolean activeNodesEmpty) {
    List<URI> quarantinedNodes = getQuarantinedNodesInternal();
    if (quarantinedNodes.isEmpty()) {
      return false;
    }
    if (activeNodesEmpty) {
      return true;
    }

    return consumeQuarantineSamplingSlot();
  }

  private boolean consumeQuarantineSamplingSlot() {
    long attempt = quarantineSamplingCounter.getAndIncrement() + 1;
    return attempt % config.getNodeHealthConfig().getQuarantineTrafficInterval() == 0;
  }

  static URI firstNodeWithSeed(List<URI> nodes, long seed) {
    List<URI> candidates = sortAndDedupeNodes(nodes);
    if (candidates.isEmpty()) {
      return null;
    }
    return candidates.get(new GoRand(seed).intn(candidates.size()));
  }

  static List<URI> drainSeeded(List<URI> nodes, long seed) {
    List<URI> remainingNodes = sortAndDedupeNodes(nodes);
    List<URI> out = new ArrayList<>();
    GoRand rand = new GoRand(seed);
    while (!remainingNodes.isEmpty()) {
      int idx = rand.intn(remainingNodes.size());
      URI node = remainingNodes.get(idx);
      int last = remainingNodes.size() - 1;
      remainingNodes.set(idx, remainingNodes.get(last));
      remainingNodes.remove(last);
      out.add(node);
    }
    return out;
  }

  static List<URI> sortAndDedupeNodes(List<URI> nodes) {
    List<URI> sorted = dedupePreservingOrder(nodes);
    sorted.sort(Comparator.comparing(URI::toString));
    return sorted;
  }

  static List<URI> dedupePreservingOrder(List<URI> nodes) {
    List<URI> deduped = new ArrayList<>();
    appendUniqueNodes(deduped, nodes);
    return deduped;
  }

  static void appendUniqueNodes(List<URI> out, List<URI> nodes) {
    Set<URI> seen = nodeKeys(out);
    for (URI node : nodes) {
      URI key = NodeHealthStore.canonicalNodeKey(node);
      if (node != null && seen.add(key)) {
        out.add(node);
      }
    }
  }

  private static Set<URI> nodeKeys(List<URI> nodes) {
    Set<URI> keys = new HashSet<>();
    for (URI node : nodes) {
      keys.add(NodeHealthStore.canonicalNodeKey(node));
    }
    return keys;
  }

  /**
   * Returns a snapshot of the current discovered nodes list.
   *
   * <p>The list is the raw discovered candidate set, not a health-filtered routing list. Discovery
   * updates publish sorted nodes, while the initial seed list preserves configured seed order until
   * the first successful update.
   *
   * @return an unmodifiable list of the current discovered node URIs
   * @since 2.1.0
   */
  public List<URI> getDiscoveredNodes() {
    return Collections.unmodifiableList(new ArrayList<>(discoveredNodes.get()));
  }

  /**
   * Returns a snapshot of discovered nodes currently active for normal routing.
   *
   * <p>Nodes that are quarantined or down remain visible through {@link #getDiscoveredNodes()} but
   * are excluded from this live-node view.
   *
   * @return an unmodifiable list of active discovered node URIs in stored discovered-node order
   * @since 2.0.0
   */
  public List<URI> getLiveNodes() {
    List<URI> live = new ArrayList<>();
    for (URI node : getDiscoveredNodesInternal()) {
      NodeHealthStatus status = healthStore.getNodeStatus(node);
      if (status == null || status.getState() == NodeHealthState.ACTIVE) {
        live.add(node);
      }
    }
    return Collections.unmodifiableList(dedupePreservingOrder(live));
  }
}
