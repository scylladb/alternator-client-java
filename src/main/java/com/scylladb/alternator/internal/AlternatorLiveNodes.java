package com.scylladb.alternator.internal;

import com.scylladb.alternator.AlternatorConfig;
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
import java.util.concurrent.atomic.AtomicInteger;
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
 * Maintains and automatically updates a list of known live Alternator nodes. Live Alternator nodes
 * should answer alternatorScheme (http or https) requests on port alternatorPort. One of these
 * livenodes will be used, at round-robin order, for every connection. The list of live nodes starts
 * with one or more known nodes, but then a thread periodically replaces this list by an up-to-date
 * list retrieved from making a "/localnodes" requests to one of these nodes.
 *
 * @author dmitry.kropachev
 */
public class AlternatorLiveNodes extends Thread {
  private final String alternatorScheme;
  private final int alternatorPort;
  private final AtomicReference<List<URI>> liveNodes;
  private final List<URI> initialNodes;
  private final AtomicInteger nextLiveNodeIndex;
  private final AlternatorConfig config;
  private final AtomicBoolean running = new AtomicBoolean(false);
  private final AtomicBoolean shutdownRequested = new AtomicBoolean(false);
  private final SdkHttpClient pollingHttpClient;
  private final boolean ownsPollingClient;
  private final AtomicLong lastActivityTime = new AtomicLong(0);

  /**
   * Set once {@link #validateConfig()} has proven the scheme/port pair valid. After that {@link
   * #hostToURI(String)} skips the expensive toURL() validation on each refresh.
   */
  private boolean schemeAndPortValidated;

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
          logger.log(Level.SEVERE, "AlternatorLiveNodes failed to sync nodes list", e);
        }
        try {
          Thread.sleep(getRefreshInterval());
        } catch (InterruptedException e) {
          logger.log(Level.INFO, "AlternatorLiveNodes thread interrupted and stopping");
          Thread.currentThread().interrupt(); // Restore interrupted status
          return;
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
    if (ownsPollingClient && pollingHttpClient != null) {
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
    this(
        config.getSeedHosts().isEmpty()
            ? AlternatorConfig.builder()
                .withSeedNode(seedUri)
                .withRoutingScope(config.getRoutingScope())
                .withCompressionAlgorithm(config.getCompressionAlgorithm())
                .withMinCompressionSizeBytes(config.getMinCompressionSizeBytes())
                .withOptimizeHeaders(config.isOptimizeHeaders())
                .withHeadersWhitelist(config.getHeadersWhitelist())
                .build()
            : config);
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
    this.liveNodes = new AtomicReference<>();
    this.nextLiveNodeIndex = new AtomicInteger(0);
    this.config = config;
    this.pollingHttpClient = pollingHttpClient;
    this.ownsPollingClient = ownsPollingClient;
    // Validate the scheme/port pair first so the per-host URI builds below — and every
    // subsequent /localnodes refresh — can skip the toURL() round-trip.
    try {
      this.validateConfig();
    } catch (ValidationError e) {
      throw new RuntimeException(e);
    }
    // Build initial node URIs through the same helper used by /localnodes refreshes.
    List<URI> seedUris = new ArrayList<>(seedHosts.size());
    for (String host : seedHosts) {
      try {
        seedUris.add(hostToURI(host));
      } catch (URISyntaxException | MalformedURLException e) {
        throw new RuntimeException("Invalid host: " + host, e);
      }
    }
    this.initialNodes = seedUris;
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
      // Make sure that `alternatorScheme` and `alternatorPort` are correct values; once this
      // succeeds, subsequent hostToURI() calls can skip the toURL() check (the only failure
      // mode there is the scheme/port combination, which is fixed for the lifetime of this
      // instance).
      URI probe = new URI(alternatorScheme, null, "1.1.1.1", alternatorPort, null, null, null);
      probe.toURL();
      schemeAndPortValidated = true;
    } catch (MalformedURLException | URISyntaxException e) {
      throw new ValidationError("failed to validate configuration", e);
    }
  }

  private URI hostToURI(String host) throws URISyntaxException, MalformedURLException {
    URI uri = new URI(alternatorScheme, null, host, alternatorPort, null, null, null);
    // Skip the toURL() round-trip once we've already proved the scheme/port valid in
    // validateConfig(). Most bad host strings still fail URI construction above, but some (e.g.
    // a host starting with '/') silently produce a URI whose port is -1 because the host is
    // reinterpreted as a path. Reject those here.
    if (uri.getPort() != alternatorPort) {
      throw new URISyntaxException(host, "host string produced a URI with unexpected port");
    }
    if (!schemeAndPortValidated) {
      uri.toURL();
    }
    return uri;
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
    // Math.floorMod handles the AtomicInteger wrap-around case where getAndIncrement returns
    // Integer.MIN_VALUE. Math.abs(Integer.MIN_VALUE) == Integer.MIN_VALUE (still negative),
    // which would throw IndexOutOfBoundsException once every ~2^31 calls.
    return nodes.get(Math.floorMod(nextLiveNodeIndex.getAndIncrement(), nodes.size()));
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
      return new URI(uri.getScheme(), null, uri.getHost(), uri.getPort(), path, query, null);
    } catch (URISyntaxException e) {
      // Should never happen, nextAsURI content is already validated
      throw new RuntimeException(e);
    }
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

  void updateLiveNodes() throws IOException {
    // Mark activity so the background poller stays at the active (1s) refresh interval. The
    // previous implementation routed every refresh through nextAsURI(), which implicitly
    // bumped lastActivityTime; this version selects candidates directly off the live list,
    // so we have to mark activity explicitly or the poller will fall back to the idle
    // interval (60s) and convergence after a node death is far too slow to matter.
    markActivity();
    RoutingScope scope = this.config.getRoutingScope();
    IOException lastException = null;
    // Nodes that returned IOException during this refresh cycle. They must not be re-injected
    // into the live list when we publish a new one — that's the bug behind issue #YCSB-rolling-
    // upgrade where a dead seed was put back into rotation on every refresh because of
    // mergeWithInitialNodes(). Refresh is best-effort: a node that succeeds once is enough.
    Set<URI> deadInThisCycle = new HashSet<>();
    while (scope != null) {
      String query = scope.getLocalNodesQuery();
      // Iterate over all known live nodes for this scope. The old behavior tried exactly one
      // node per scope (issue: a refresh that picked the dead seed gave up immediately, even
      // though N-1 healthy peers could have answered).
      List<URI> candidates = new ArrayList<>(liveNodes.get());
      boolean scopeHandled = false;
      // Track per-scope failures separately: a node that returns IOException at one scope
      // is still retried at the next scope (matches the existing scope-fallback test
      // expectations — scopes differ by query params, not transport reachability, and the
      // empty-list and IOException paths must traverse the same number of scope levels).
      Set<URI> deadInThisScope = new HashSet<>();
      for (URI base : candidates) {
        if (deadInThisScope.contains(base)) {
          continue;
        }
        URI uri;
        try {
          uri =
              new URI(
                  base.getScheme(),
                  null,
                  base.getHost(),
                  base.getPort(),
                  "/localnodes",
                  query.isEmpty() ? null : query,
                  null);
        } catch (URISyntaxException e) {
          throw new AssertionError("live-node URI " + base + " is already validated", e);
        }
        try {
          List<URI> nodes = getNodes(uri);
          if (!nodes.isEmpty()) {
            liveNodes.set(mergePostRefresh(nodes, base, deadInThisCycle));
            logger.log(
                Level.FINE, "Updated hosts to " + liveNodes + " using " + scope.getDescription());
            return;
          }
          // Empty response: this scope simply has no nodes here; stop polling other hosts
          // (they'd all return the same empty list) and fall through to the next scope.
          scopeHandled = true;
          break;
        } catch (IOException e) {
          logger.log(
              Level.WARNING, "Failed to contact node " + uri + " for " + scope.getDescription(), e);
          deadInThisScope.add(base);
          deadInThisCycle.add(base);
          lastException = e;
          // Try the next live node within this same scope before falling back.
        }
      }
      // Suppress "unused" warning when no candidate triggered the empty-list path explicitly.
      if (!scopeHandled) {
        // nothing to do — either all candidates threw IOException, or there were no candidates
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
    // No scope produced a usable list. Prune nodes we confirmed dead in this cycle. If that
    // empties the list, fall back to the original seed set so a future refresh can rediscover
    // the cluster — but do NOT silently merge dead seeds back into a list that still has
    // live nodes, that's exactly what kept routing traffic at downed coordinators.
    if (lastException != null) {
      List<URI> remaining = new ArrayList<>(liveNodes.get());
      remaining.removeAll(deadInThisCycle);
      if (remaining.isEmpty()) {
        liveNodes.set(new ArrayList<>(initialNodes));
        logger.log(
            Level.WARNING,
            "All known nodes unreachable in every routing scope, restoring seed list "
                + "as last-resort recovery candidates");
      } else {
        liveNodes.set(remaining);
        logger.log(
            Level.WARNING,
            "All routing scopes failed to refresh; pruned "
                + deadInThisCycle.size()
                + " unreachable node(s), continuing with remaining live nodes");
      }
    } else {
      logger.log(Level.WARNING, "No nodes found in any routing scope, keeping existing node list");
    }
  }

  /**
   * Builds the new live-node list after a successful /localnodes response.
   *
   * <p>The discovered list is treated as authoritative: a healthy peer answering /localnodes knows
   * which nodes are currently live and the ones it omits are presumed down. The node that just
   * answered ({@code source}) is added unconditionally — it just proved itself alive. Nodes that
   * failed earlier in this same refresh cycle are excluded even if they appear in the discovered
   * list (covers the case where the discovery endpoint reports a stale list that still includes the
   * dead seed).
   *
   * <p>This deliberately replaces the previous "always merge in the original seed URIs" behavior:
   * that was a safety net for the case where every discovered node dies, but it also kept dead
   * seeds permanently in rotation when the peer's view was newer than the binding's. The all-die
   * safety net is handled by the seed-restore branch in {@link #updateLiveNodes()} instead.
   */
  List<URI> mergePostRefresh(List<URI> discovered, URI source, Set<URI> deadInThisCycle) {
    LinkedHashSet<URI> result = new LinkedHashSet<>(discovered);
    result.removeAll(deadInThisCycle);
    if (source != null) {
      result.add(source);
    }
    return new ArrayList<>(result);
  }

  private List<URI> getNodes(URI uri) throws IOException {
    SdkHttpRequest sdkRequest =
        SdkHttpRequest.builder()
            .uri(uri)
            .method(SdkHttpMethod.GET)
            .putHeader("Host", uri.getHost() + ":" + uri.getPort())
            .putHeader("Connection", "keep-alive")
            .build();
    HttpExecuteRequest executeRequest = HttpExecuteRequest.builder().request(sdkRequest).build();
    ExecutableHttpRequest preparedRequest = pollingHttpClient.prepareRequest(executeRequest);
    HttpExecuteResponse response = preparedRequest.call();

    try {
      int statusCode = response.httpResponse().statusCode();
      if (statusCode != HttpURLConnection.HTTP_OK) {
        // Consume and close the response body to release the connection
        response.responseBody().ifPresent(this::consumeAndClose);
        return Collections.emptyList();
      }

      Optional<AbortableInputStream> bodyOpt = response.responseBody();
      if (!bodyOpt.isPresent()) {
        return Collections.emptyList();
      }

      String responseStr;
      try (AbortableInputStream body = bodyOpt.get()) {
        responseStr = streamToString(body);
      }

      // /localnodes responds with a JSON array of host strings, e.g.
      //   ["127.0.0.2","127.0.0.3","127.0.0.1"]
      // We parse it by hand to avoid pulling in a JSON library — the format is restricted
      // enough that a streaming scan over the bytes is straightforward and tolerant of
      // whitespace, an empty array, or a missing trailing newline.
      return parseLocalNodes(responseStr);
    } catch (IOException e) {
      // Ensure the response body is consumed on error
      response.responseBody().ifPresent(this::consumeAndClose);
      throw e;
    }
  }

  /**
   * Parses a JSON array of host strings into a list of URIs. Tolerates surrounding whitespace, an
   * empty array, and missing trailing newline. Skips entries that fail URI construction with a
   * warning rather than failing the whole refresh.
   */
  List<URI> parseLocalNodes(String json) {
    if (json == null) {
      return Collections.emptyList();
    }
    int i = 0;
    int n = json.length();
    // Skip leading whitespace
    while (i < n && Character.isWhitespace(json.charAt(i))) {
      i++;
    }
    if (i >= n || json.charAt(i) != '[') {
      logger.log(Level.WARNING, "Unexpected /localnodes response (not a JSON array): " + json);
      return Collections.emptyList();
    }
    i++; // consume '['
    List<URI> result = new ArrayList<>();
    StringBuilder host = new StringBuilder(32);
    while (i < n) {
      char c = json.charAt(i);
      if (Character.isWhitespace(c) || c == ',') {
        i++;
        continue;
      }
      if (c == ']') {
        break;
      }
      if (c != '"') {
        logger.log(
            Level.WARNING,
            "Unexpected character in /localnodes response at index " + i + ": " + json);
        break;
      }
      i++; // consume opening quote
      host.setLength(0);
      while (i < n) {
        char ch = json.charAt(i);
        if (ch == '\\' && i + 1 < n) {
          host.append(json.charAt(i + 1));
          i += 2;
          continue;
        }
        if (ch == '"') {
          i++; // consume closing quote
          break;
        }
        host.append(ch);
        i++;
      }
      String parsed = host.toString();
      if (parsed.isEmpty()) {
        continue;
      }
      try {
        result.add(hostToURI(parsed));
      } catch (URISyntaxException | MalformedURLException e) {
        logger.log(Level.WARNING, "Invalid host in /localnodes response: " + parsed, e);
      }
    }
    return result;
  }

  /**
   * Thread-local drain buffer reused across {@link #consumeAndClose} calls so we don't allocate a
   * new 1 KiB array every time we have to discard a response body.
   */
  private static final ThreadLocal<byte[]> DRAIN_BUFFER =
      ThreadLocal.withInitial(() -> new byte[1024]);

  /**
   * Consumes and closes an AbortableInputStream to release the underlying connection back to the
   * pool.
   */
  private void consumeAndClose(AbortableInputStream stream) {
    try {
      byte[] buf = DRAIN_BUFFER.get();
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
      URI uri = nextAsURI("/localnodes", query);
      List<URI> nodes = getNodes(uri);
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
    // No defensive copy: every list ever stored in the AtomicReference is constructed
    // internally and never mutated after publication. unmodifiableList prevents callers
    // from accidentally mutating that internal list.
    return Collections.unmodifiableList(liveNodes.get());
  }
}
