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

import com.scylladb.alternator.AlternatorConfig;
import com.scylladb.alternator.TlsConfig;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsExchange;
import com.sun.net.httpserver.HttpsParameters;
import com.sun.net.httpserver.HttpsServer;
import java.io.ByteArrayInputStream;
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
import org.junit.Test;
import software.amazon.awssdk.http.SdkHttpClient;

/** Real TLS fallback tests using a certificate valid only for the logical DNS hostname. */
public class AlternatorLiveNodesTlsDnsFallbackTest {
  private static final char[] KEYSTORE_PASSWORD = "changeit".toCharArray();
  // Public test-only key material for a loopback server; never used outside this test.
  private static final String KEYSTORE_BASE64 =
      "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"
          + "BBSzS/pH6sWmlZgkwx6kxmCPv+WcZgICJxA=";

  @Test(timeout = 15000)
  public void testApacheHttpsFallbackPreservesLogicalHostnameAndSni() throws Exception {
    assertHttpsFallback(
        (tlsConfig, badAddress, goodAddress) ->
            ApacheSyncClientFactory.createPollingClient(
                tlsConfig, hostname -> new InetAddress[] {badAddress, goodAddress}));
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
              .withSeedHost("logical.test")
              .withScheme("https")
              .withPort(port)
              .build();
      AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config, client);

      liveNodes.updateLiveNodes();

      assertEquals("learned.test", liveNodes.nextAsURI().getHost());
      assertEquals(1, badRequests.get());
      assertEquals(1, goodRequests.get());
      assertEquals("logical.test:" + port, badHost.get());
      assertEquals("logical.test:" + port, goodHost.get());
      assertEquals("logical.test", badSni.get());
      assertEquals("logical.test", goodSni.get());
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
    KeyStore keyStore = KeyStore.getInstance("PKCS12");
    byte[] encoded = Base64.getDecoder().decode(KEYSTORE_BASE64);
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
      throws Exception {
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
}
