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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.scylladb.alternator.AlternatorConfig;
import com.scylladb.alternator.TlsConfig;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsExchange;
import com.sun.net.httpserver.HttpsParameters;
import com.sun.net.httpserver.HttpsServer;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.ExtendedSSLSession;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SNIHostName;
import javax.net.ssl.SNIServerName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSession;
import org.junit.Assume;
import org.junit.Test;
import software.amazon.awssdk.http.SdkHttpClient;

/** Real TLS fallback tests using a certificate valid only for the logical DNS hostname. */
public class AlternatorLiveNodesTlsDnsFallbackTest {
  private static final char[] KEYSTORE_PASSWORD = "changeit".toCharArray();
  // Public test-only key material for a loopback server; never used outside this test.
  private static final String KEYSTORE_BASE64 =
      "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"
          + "BBSzS/pH6sWmlZgkwx6kxmCPv+WcZgICJxA=";

  private static final String IPV6_KEYSTORE_BASE64 =
      "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";

  @Test(timeout = 15000)
  public void testApacheHttpsFallbackPreservesLogicalHostnameAndSni() throws Exception {
    assertHttpsFallback(
        (tlsConfig, badAddress, goodAddress) ->
            ApacheSyncClientFactory.createPollingClient(
                tlsConfig, hostname -> new InetAddress[] {badAddress, goodAddress}));
  }

  @Test(timeout = 15000)
  public void testApacheHttpsFallbackNormalizesTrailingDotOnlyForTlsIdentity() throws Exception {
    assertHttpsFallback(
        (tlsConfig, badAddress, goodAddress) ->
            ApacheSyncClientFactory.createPollingClient(
                tlsConfig, hostname -> new InetAddress[] {badAddress, goodAddress}),
        "logical.test.",
        "logical.test");
  }

  @Test(timeout = 15000)
  public void testCrtHttpsFallbackPreservesLogicalHostnameAndSni() throws Exception {
    assertHttpsFallback(
        (tlsConfig, badAddress, goodAddress) ->
            CrtSyncClientFactory.createPollingClient(
                TlsConfig.systemDefault(),
                hostname -> Arrays.asList(badAddress, goodAddress),
                TlsContextFactory.createSslContext(tlsConfig).getSocketFactory()));
  }

  @Test(timeout = 15000)
  public void testCrtHttpsFallbackNormalizesTrailingDotOnlyForTlsIdentity() throws Exception {
    assertHttpsFallback(
        (tlsConfig, badAddress, goodAddress) ->
            CrtSyncClientFactory.createPollingClient(
                TlsConfig.systemDefault(),
                hostname -> Arrays.asList(badAddress, goodAddress),
                TlsContextFactory.createSslContext(tlsConfig).getSocketFactory()),
        "logical.test.",
        "logical.test");
  }

  @Test(timeout = 15000)
  public void testApacheHttpsFallbackUsesBracketedIpv6AuthorityAndIpCertificateIdentity()
      throws Exception {
    assertIpv6HttpsFallback(
        (tlsConfig, ipv6Loopback, resolvedHost) ->
            ApacheSyncClientFactory.createPollingClient(
                tlsConfig,
                hostname -> {
                  resolvedHost.set(hostname);
                  return new InetAddress[] {ipv6Loopback};
                }));
  }

  @Test(timeout = 15000)
  public void testCrtHttpsFallbackUsesBracketedIpv6AuthorityAndIpCertificateIdentity()
      throws Exception {
    assertIpv6HttpsFallback(
        (tlsConfig, ipv6Loopback, resolvedHost) ->
            CrtSyncClientFactory.createPollingClient(
                TlsConfig.systemDefault(),
                hostname -> {
                  resolvedHost.set(hostname);
                  return Arrays.asList(ipv6Loopback);
                },
                TlsContextFactory.createSslContext(tlsConfig).getSocketFactory()));
  }

  private static void assertIpv6HttpsFallback(Ipv6PollingClientFactory clientFactory)
      throws Exception {
    InetAddress ipv6Loopback = InetAddress.getByName("::1");
    KeyStore keyStore = loadKeyStore(IPV6_KEYSTORE_BASE64);
    X509Certificate certificate = (X509Certificate) keyStore.getCertificate("ipv6");
    assertEquals(1, certificate.getSubjectAlternativeNames().size());
    assertEquals(7, certificate.getSubjectAlternativeNames().iterator().next().get(0));

    AtomicInteger requests = new AtomicInteger();
    AtomicReference<String> host = new AtomicReference<>();
    AtomicReference<String> sni = new AtomicReference<>();
    HttpsServer server;
    try {
      server =
          startServer(
              serverContext(keyStore),
              ipv6Loopback,
              0,
              200,
              "[\"learned.test\"]",
              requests,
              host,
              sni);
    } catch (IOException exception) {
      Assume.assumeNoException("IPv6 loopback is unavailable", exception);
      return;
    }

    Path caCertificate = Files.createTempFile("ipv6-loopback-test-ca", ".cer");
    Files.write(caCertificate, certificate.getEncoded());
    AtomicReference<String> resolvedHost = new AtomicReference<>();
    SdkHttpClient client = null;
    try {
      TlsConfig certificateTrust =
          TlsConfig.builder()
              .withCaCertPath(caCertificate)
              .withTrustSystemCaCerts(false)
              .withVerifyHostname(true)
              .build();
      client = clientFactory.create(certificateTrust, ipv6Loopback, resolvedHost);
      int port = server.getAddress().getPort();
      AlternatorLiveNodes liveNodes =
          new AlternatorLiveNodes(
              AlternatorConfig.builder()
                  .withSeedHost("::1")
                  .withScheme("https")
                  .withPort(port)
                  .build(),
              client);

      liveNodes.updateLiveNodes();

      assertEquals("learned.test", liveNodes.nextAsURI().getHost());
      assertNull("an IPv6 literal must bypass the DNS resolver callback", resolvedHost.get());
      assertEquals(1, requests.get());
      assertEquals("[::1]:" + port, host.get());
      assertNull("IP literals must not be sent as DNS SNI names", sni.get());
    } finally {
      if (client != null) {
        client.close();
      }
      server.stop(0);
      Files.deleteIfExists(caCertificate);
    }
  }

  @Test(timeout = 15000)
  public void testCrtHttpsFallbackRejectsCertificateForDifferentLogicalHostname() throws Exception {
    KeyStore keyStore = loadKeyStore();
    Certificate certificate = keyStore.getCertificate("logical");
    Path caCertificate = Files.createTempFile("logical-test-ca", ".cer");
    Files.write(caCertificate, certificate.getEncoded());
    InetAddress address = InetAddress.getByName("127.0.0.1");
    AtomicInteger requests = new AtomicInteger();
    HttpsServer server = null;
    SdkHttpClient client = null;
    try {
      server =
          startServer(
              serverContext(keyStore),
              address,
              0,
              200,
              "[\"learned.test\"]",
              requests,
              new AtomicReference<>(),
              new AtomicReference<>());
      TlsConfig certificateTrust =
          TlsConfig.builder()
              .withCaCertPath(caCertificate)
              .withTrustSystemCaCerts(false)
              .withVerifyHostname(true)
              .build();
      client =
          CrtSyncClientFactory.createPollingClient(
              TlsConfig.systemDefault(),
              hostname -> Arrays.asList(address),
              TlsContextFactory.createSslContext(certificateTrust).getSocketFactory());
      AlternatorLiveNodes liveNodes =
          new AlternatorLiveNodes(
              AlternatorConfig.builder()
                  .withSeedHost("different.test")
                  .withScheme("https")
                  .withPort(server.getAddress().getPort())
                  .build(),
              client);

      liveNodes.updateLiveNodes();

      assertEquals("different.test", liveNodes.nextAsURI().getHost());
      assertEquals("TLS hostname validation must fail before an HTTP request", 0, requests.get());
    } finally {
      if (client != null) {
        client.close();
      }
      if (server != null) {
        server.stop(0);
      }
      Files.deleteIfExists(caCertificate);
    }
  }

  @Test(timeout = 15000)
  public void testCrtHttpsFallbackHonorsTrustAll() throws Exception {
    InetAddress address = InetAddress.getByName("127.0.0.1");
    AtomicInteger requests = new AtomicInteger();
    AtomicReference<String> host = new AtomicReference<>();
    AtomicReference<String> sni = new AtomicReference<>();
    HttpsServer server = null;
    SdkHttpClient client = null;
    try {
      server =
          startServer(
              serverContext(loadKeyStore()),
              address,
              0,
              200,
              "[\"learned.test\"]",
              requests,
              host,
              sni);
      client =
          CrtSyncClientFactory.createPollingClient(
              TlsConfig.trustAll(), hostname -> Arrays.asList(address));
      int port = server.getAddress().getPort();
      AlternatorLiveNodes liveNodes =
          new AlternatorLiveNodes(
              AlternatorConfig.builder()
                  .withSeedHost("different.test")
                  .withScheme("https")
                  .withPort(port)
                  .build(),
              client);

      liveNodes.updateLiveNodes();

      assertEquals("learned.test", liveNodes.nextAsURI().getHost());
      assertEquals(1, requests.get());
      assertEquals("different.test:" + port, host.get());
      assertEquals("different.test", sni.get());
    } finally {
      if (client != null) {
        client.close();
      }
      if (server != null) {
        server.stop(0);
      }
    }
  }

  private static void assertHttpsFallback(PollingClientFactory clientFactory) throws Exception {
    assertHttpsFallback(clientFactory, "logical.test", "logical.test");
  }

  private static void assertHttpsFallback(
      PollingClientFactory clientFactory, String logicalHost, String expectedTlsHost)
      throws Exception {
    KeyStore keyStore = loadKeyStore();
    Path caCertificate = Files.createTempFile("logical-test-ca", ".cer");
    Certificate certificate = keyStore.getCertificate("logical");
    X509Certificate serverCertificate = (X509Certificate) certificate;
    assertEquals("CN=logical.test", serverCertificate.getSubjectX500Principal().getName());
    assertEquals(1, serverCertificate.getSubjectAlternativeNames().size());
    assertEquals(
        "logical.test", serverCertificate.getSubjectAlternativeNames().iterator().next().get(1));
    Files.write(caCertificate, certificate.getEncoded());

    SSLContext serverContext = serverContext(keyStore);
    InetAddress goodAddress = InetAddress.getByName("127.0.0.1");
    InetAddress badAddress = InetAddress.getByName("127.0.0.2");
    AtomicInteger goodRequests = new AtomicInteger();
    AtomicInteger badRequests = new AtomicInteger();
    AtomicReference<String> goodHost = new AtomicReference<>();
    AtomicReference<String> badHost = new AtomicReference<>();
    AtomicReference<String> goodSni = new AtomicReference<>();
    AtomicReference<String> badSni = new AtomicReference<>();
    HttpsServer goodServer = null;
    HttpsServer badServer = null;
    SdkHttpClient client = null;
    try {
      goodServer =
          startServer(
              serverContext,
              goodAddress,
              0,
              200,
              "[\"learned.test\"]",
              goodRequests,
              goodHost,
              goodSni);
      int port = goodServer.getAddress().getPort();
      badServer =
          startServer(
              serverContext,
              badAddress,
              port,
              503,
              "temporarily unavailable",
              badRequests,
              badHost,
              badSni);
      TlsConfig tlsConfig =
          TlsConfig.builder()
              .withCaCertPath(caCertificate)
              .withTrustSystemCaCerts(false)
              .withVerifyHostname(true)
              .build();
      client = clientFactory.create(tlsConfig, badAddress, goodAddress);
      AlternatorConfig config =
          AlternatorConfig.builder()
              .withSeedHost(logicalHost)
              .withScheme("https")
              .withPort(port)
              .build();
      AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, client);

      liveNodes.updateLiveNodes();

      assertEquals("learned.test", liveNodes.nextAsURI().getHost());
      assertEquals(1, badRequests.get());
      assertEquals(1, goodRequests.get());
      assertEquals(logicalHost + ":" + port, badHost.get());
      assertEquals(logicalHost + ":" + port, goodHost.get());
      assertEquals(expectedTlsHost, badSni.get());
      assertEquals(expectedTlsHost, goodSni.get());
    } finally {
      if (client != null) {
        client.close();
      }
      if (badServer != null) {
        badServer.stop(0);
      }
      if (goodServer != null) {
        goodServer.stop(0);
      }
      Files.deleteIfExists(caCertificate);
    }
  }

  private static KeyStore loadKeyStore() throws Exception {
    return loadKeyStore(KEYSTORE_BASE64);
  }

  private static KeyStore loadKeyStore(String encodedKeyStore) throws Exception {
    KeyStore keyStore = KeyStore.getInstance("PKCS12");
    byte[] encoded = Base64.getDecoder().decode(encodedKeyStore);
    try (ByteArrayInputStream input = new ByteArrayInputStream(encoded)) {
      keyStore.load(input, KEYSTORE_PASSWORD);
    }
    return keyStore;
  }

  private static SSLContext serverContext(KeyStore keyStore) throws Exception {
    KeyManagerFactory keyManagerFactory =
        KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    keyManagerFactory.init(keyStore, KEYSTORE_PASSWORD);
    SSLContext context = SSLContext.getInstance("TLS");
    context.init(keyManagerFactory.getKeyManagers(), null, new SecureRandom());
    return context;
  }

  private static HttpsServer startServer(
      SSLContext context,
      InetAddress address,
      int port,
      int status,
      String responseBody,
      AtomicInteger requestCount,
      AtomicReference<String> host,
      AtomicReference<String> sni)
      throws IOException {
    HttpsServer server = HttpsServer.create(new InetSocketAddress(address, port), 0);
    server.setHttpsConfigurator(
        new HttpsConfigurator(context) {
          @Override
          public void configure(HttpsParameters parameters) {
            SSLParameters sslParameters = context.getDefaultSSLParameters();
            parameters.setSSLParameters(sslParameters);
          }
        });
    server.createContext(
        "/localnodes",
        exchange -> {
          HttpsExchange httpsExchange = (HttpsExchange) exchange;
          requestCount.incrementAndGet();
          host.set(exchange.getRequestHeaders().getFirst("Host"));
          sni.set(requestedServerName(httpsExchange.getSSLSession()));
          byte[] body = responseBody.getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(status, body.length);
          try (OutputStream output = exchange.getResponseBody()) {
            output.write(body);
          }
        });
    server.start();
    return server;
  }

  private static String requestedServerName(SSLSession session) {
    if (!(session instanceof ExtendedSSLSession)) {
      return null;
    }
    List<SNIServerName> names = ((ExtendedSSLSession) session).getRequestedServerNames();
    for (SNIServerName name : names) {
      if (name instanceof SNIHostName) {
        return ((SNIHostName) name).getAsciiName();
      }
    }
    return null;
  }

  private interface PollingClientFactory {
    SdkHttpClient create(TlsConfig tlsConfig, InetAddress badAddress, InetAddress goodAddress)
        throws Exception;
  }

  private interface Ipv6PollingClientFactory {
    SdkHttpClient create(
        TlsConfig tlsConfig, InetAddress ipv6Loopback, AtomicReference<String> resolvedHost)
        throws Exception;
  }
}
