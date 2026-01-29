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
import java.net.ProtocolException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

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
  private final HttpClient httpClient;

  private static Logger logger = Logger.getLogger(AlternatorLiveNodes.class.getName());

  /** {@inheritDoc} */
  @Override
  public void run() {
    logger.log(Level.INFO, "AlternatorLiveNodes thread started");
    try {
      for (; ; ) {
        try {
          updateLiveNodes();
        } catch (IOException e) {
          logger.log(Level.SEVERE, "AlternatorLiveNodes failed to sync nodes list", e);
        }
        try {
          Thread.sleep(5000);
        } catch (InterruptedException e) {
          logger.log(Level.INFO, "AlternatorLiveNodes thread interrupted and stopping");
          return;
        }
      }
    } finally {
      logger.log(Level.INFO, "AlternatorLiveNodes thread stopped");
    }
  }

  /**
   * Constructor for AlternatorLiveNodes.
   *
   * @param liveNode a {@link java.net.URI} object
   * @param datacenter a {@link java.lang.String} object
   * @param rack a {@link java.lang.String} object
   * @deprecated Use {@link #AlternatorLiveNodes(AlternatorConfig)} instead.
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
   * @since 1.0.5
   * @deprecated Use {@link #AlternatorLiveNodes(AlternatorConfig)} instead.
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
   * @since 1.0.5
   * @deprecated Use {@link #AlternatorLiveNodes(AlternatorConfig)} with config containing seed
   *     node.
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
   * @deprecated Use {@link #AlternatorLiveNodes(AlternatorConfig)} instead.
   */
  @Deprecated
  public AlternatorLiveNodes(
      List<URI> liveNodes, String scheme, int port, String datacenter, String rack) {
    this(buildConfigFromUris(liveNodes, datacenter, rack, null));
  }

  /**
   * Constructor for AlternatorLiveNodes with RoutingScope.
   *
   * @param liveNodes a {@link java.util.List} object of URIs
   * @param scheme a {@link java.lang.String} object (ignored, extracted from URIs)
   * @param port a int (ignored, extracted from URIs)
   * @param routingScope the routing scope for node targeting
   * @since 1.0.5
   * @deprecated Use {@link #AlternatorLiveNodes(AlternatorConfig)} instead.
   */
  @Deprecated
  public AlternatorLiveNodes(
      List<URI> liveNodes, String scheme, int port, RoutingScope routingScope) {
    this(buildConfigFromUris(liveNodes, null, null, routingScope));
  }

  /**
   * Constructor for AlternatorLiveNodes with AlternatorConfig containing seed nodes, scheme, port,
   * and routing settings.
   *
   * @param liveNodes a {@link java.util.List} object of URIs
   * @param scheme a {@link java.lang.String} object (ignored, extracted from URIs)
   * @param port a int (ignored, extracted from URIs)
   * @param config the Alternator configuration containing routing scope and other settings
   * @since 1.0.5
   * @deprecated Use {@link #AlternatorLiveNodes(AlternatorConfig)} with config containing seed
   *     nodes.
   */
  @Deprecated
  public AlternatorLiveNodes(
      List<URI> liveNodes, String scheme, int port, AlternatorConfig config) {
    this(
        config.getSeedHosts().isEmpty()
            ? buildConfigFromUris(liveNodes, null, null, config.getRoutingScope())
            : config);
  }

  /**
   * Helper method to build AlternatorConfig from a list of URIs.
   *
   * @param uris the list of URIs
   * @param datacenter optional datacenter
   * @param rack optional rack
   * @param routingScope optional routing scope
   * @return the built config
   */
  private static AlternatorConfig buildConfigFromUris(
      List<URI> uris, String datacenter, String rack, RoutingScope routingScope) {
    if (uris == null || uris.isEmpty()) {
      throw new RuntimeException("uris cannot be null or empty");
    }
    URI first = uris.get(0);
    List<String> hosts = new ArrayList<>();
    for (URI uri : uris) {
      hosts.add(uri.getHost());
    }
    RoutingScope effectiveScope =
        routingScope != null ? routingScope : deriveRoutingScope(datacenter, rack);
    return AlternatorConfig.builder()
        .withSeedHosts(hosts)
        .withScheme(first.getScheme())
        .withPort(first.getPort())
        .withRoutingScope(effectiveScope)
        .build();
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
   * <p>The config must contain seed hosts (via {@link AlternatorConfig.Builder#withSeedHost},
   * {@link AlternatorConfig.Builder#withSeedHosts}, or {@link
   * AlternatorConfig.Builder#withSeedNode}). The scheme and port must also be configured.
   *
   * @param config the Alternator configuration containing seed hosts, scheme, port, routing scope,
   *     and other settings
   * @throws RuntimeException if config is null or contains no seed hosts
   * @since 1.0.5
   */
  public AlternatorLiveNodes(AlternatorConfig config) {
    if (config == null) {
      throw new RuntimeException("config cannot be null");
    }
    List<String> seedHosts = config.getSeedHosts();
    if (seedHosts == null || seedHosts.isEmpty()) {
      throw new RuntimeException("config must contain at least one seed host");
    }
    this.alternatorScheme = config.getScheme();
    this.alternatorPort = config.getPort();
    // Build URIs from hosts, scheme, and port
    List<URI> seedUris = new ArrayList<>();
    for (String host : seedHosts) {
      try {
        seedUris.add(new URI(alternatorScheme, null, host, alternatorPort, null, null, null));
      } catch (URISyntaxException e) {
        throw new RuntimeException("Invalid host: " + host, e);
      }
    }
    this.initialNodes = seedUris;
    this.liveNodes = new AtomicReference<>();
    this.nextLiveNodeIndex = new AtomicInteger(0);
    this.config = config;
    try {
      this.validate();
    } catch (ValidationError e) {
      throw new RuntimeException(e);
    }
    this.liveNodes.set(initialNodes);
    this.httpClient = prepareHttpClient();
  }

  /**
   * Derives a RoutingScope from legacy datacenter/rack configuration.
   *
   * @param datacenter the datacenter name
   * @param rack the rack name
   * @return the derived routing scope, or ClusterScope if neither is set
   */
  private static RoutingScope deriveRoutingScopeFromLegacy(String datacenter, String rack) {
    String dc = datacenter != null ? datacenter : "";
    String r = rack != null ? rack : "";
    if (dc.isEmpty() && r.isEmpty()) {
      return ClusterScope.create();
    }
    if (dc.isEmpty()) {
      // Rack without datacenter is not valid, treat as cluster-wide
      return ClusterScope.create();
    }
    if (r.isEmpty()) {
      return DatacenterScope.of(dc, ClusterScope.create());
    }
    return RackScope.of(dc, r, DatacenterScope.of(dc, ClusterScope.create()));
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

  /**
   * nextAsURI.
   *
   * @return a {@link java.net.URI} object
   */
  public URI nextAsURI() {
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
      return new URI(uri.getScheme(), null, uri.getHost(), uri.getPort(), path, query, null);
    } catch (URISyntaxException e) {
      // Should never happen, nextAsURI content is already validated
      throw new RuntimeException(e);
    }
  }

  // Utility function for reading the entire contents of an input stream
  // (which we assume will be fairly short)
  private static String streamToString(HttpEntity body) throws IOException {
    if (body == null) {
      return "";
    }
    InputStream stream = body.getContent();
    if (stream == null) {
      return "";
    }
    Scanner s = new Scanner(stream).useDelimiter("\\A");
    String result = s.hasNext() ? s.next() : "";
    stream.close();
    return result;
  }

  private void updateLiveNodes() throws IOException {
    RoutingScope scope = this.config.getRoutingScope();
    while (scope != null) {
      String query = scope.getLocalNodesQuery();
      URI uri = nextAsURI("/localnodes", query.isEmpty() ? null : query);
      List<URI> nodes = getNodes(uri);
      if (!nodes.isEmpty()) {
        liveNodes.set(nodes);
        logger.log(
            Level.FINE, "Updated hosts to " + liveNodes + " using " + scope.getDescription());
        return;
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
    // No nodes found in any scope - keep the current list
    logger.log(Level.WARNING, "No nodes found in any routing scope, keeping existing node list");
  }

  private List<URI> getNodes(URI uri) throws IOException {
    // Note that despite this being called HttpURLConnection, it actually
    // supports HTTPS as well.
    HttpResponse httpResponse;
    try {
      httpResponse = httpClient.execute(new HttpGet(uri));
      if (httpResponse.getStatusLine().getStatusCode() != HttpURLConnection.HTTP_OK) {
        return Collections.emptyList();
      }
    } catch (ProtocolException e) {
      // It can happen only of conn is already connected or "GET" is not a valid method
      // Both cases not true, os it should happen
      throw new RuntimeException(e);
    }
    String response = streamToString(httpResponse.getEntity());
    // response looks like: ["127.0.0.2","127.0.0.3","127.0.0.1"]
    response = response.trim();
    response = response.substring(1, response.length() - 1);
    String[] list = response.split(",");
    List<URI> newHosts = new ArrayList<>();
    for (String host : list) {
      if (host.isEmpty()) {
        continue;
      }
      host = host.trim();
      host = host.substring(1, host.length() - 1);
      try {
        newHosts.add(this.hostToURI(host));
      } catch (URISyntaxException | MalformedURLException e) {
        logger.log(Level.WARNING, "Invalid host: " + host, e);
      }
    }
    return newHosts;
  }

  private static HttpClient prepareHttpClient() {
    RegistryBuilder<ConnectionSocketFactory> socketFactoryRegistryBuilder =
        RegistryBuilder.<ConnectionSocketFactory>create()
            .register("http", PlainConnectionSocketFactory.getSocketFactory())
            .register("https", SSLConnectionSocketFactory.getSocketFactory());

    TrustManager[] trustAllCertificates =
        new TrustManager[] {
          new X509TrustManager() {
            public void checkClientTrusted(X509Certificate[] chain, String authType) {}

            public void checkServerTrusted(X509Certificate[] chain, String authType) {}

            public X509Certificate[] getAcceptedIssuers() {
              return new X509Certificate[0];
            }
          }
        };
    try {
      SSLContext sslContext = SSLContext.getInstance("SSL");
      sslContext.init(null, trustAllCertificates, new java.security.SecureRandom());
      socketFactoryRegistryBuilder.register(
          "https", new SSLConnectionSocketFactory(sslContext, NoopHostnameVerifier.INSTANCE));
    } catch (NoSuchAlgorithmException | KeyManagementException e) {
      throw new RuntimeException(e);
    }

    PoolingHttpClientConnectionManager httpConnectionManager =
        new PoolingHttpClientConnectionManager(socketFactoryRegistryBuilder.build());
    httpConnectionManager.setMaxTotal(200);
    httpConnectionManager.setDefaultMaxPerRoute(1);
    return HttpClients.custom().setConnectionManager(httpConnectionManager).build();
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
   * @since 1.0.5
   */
  public RoutingScope getRoutingScope() {
    return config.getRoutingScope();
  }

  /**
   * Creates a new LazyQueryPlan for iterating over the current live nodes. The plan provides
   * pseudo-random iteration order with a random seed for load distribution.
   *
   * <p>This is useful for implementing retry logic where you want to try different nodes in
   * sequence until a request succeeds.
   *
   * @return a new {@link LazyQueryPlan} containing the current live nodes
   * @since 1.0.5
   */
  public LazyQueryPlan newQueryPlan() {
    return new LazyQueryPlan(liveNodes.get(), Collections.<URI>emptyList());
  }

  /**
   * Creates a new LazyQueryPlan for iterating over the current live nodes with a specific seed for
   * reproducible ordering.
   *
   * @param seed the seed for pseudo-random ordering
   * @return a new {@link LazyQueryPlan} containing the current live nodes
   * @since 1.0.5
   */
  public LazyQueryPlan newQueryPlan(long seed) {
    return new LazyQueryPlan(liveNodes.get(), Collections.<URI>emptyList(), seed);
  }

  /**
   * Creates a new LazyQueryPlan with both active (live) and quarantined nodes. Active nodes are
   * tried first, followed by quarantined nodes. This is useful for implementing retry logic with
   * fallback to potentially recovering nodes.
   *
   * @param quarantinedNodes a list of quarantined nodes to try after active nodes
   * @return a new {@link LazyQueryPlan} containing live nodes followed by quarantined nodes
   * @since 1.0.5
   */
  public LazyQueryPlan newQueryPlan(List<URI> quarantinedNodes) {
    return new LazyQueryPlan(liveNodes.get(), quarantinedNodes);
  }

  /**
   * Returns a snapshot of the current live nodes list.
   *
   * @return an unmodifiable list of the current live node URIs
   * @since 1.0.5
   */
  public List<URI> getLiveNodes() {
    return Collections.unmodifiableList(new ArrayList<>(liveNodes.get()));
  }
}
