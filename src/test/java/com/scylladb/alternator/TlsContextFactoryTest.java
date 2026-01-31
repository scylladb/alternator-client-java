package com.scylladb.alternator;

import static org.junit.Assert.*;

import com.scylladb.alternator.internal.TlsContextFactory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import javax.net.ssl.SSLContext;
import org.junit.Test;

/**
 * Unit tests for TlsContextFactory.
 *
 * @author dmitry.kropachev
 */
public class TlsContextFactoryTest {

  @Test
  public void testCreateSslContextWithTrustAll() {
    TlsConfig config = TlsConfig.trustAll();
    SSLContext sslContext = TlsContextFactory.createSslContext(config);

    assertNotNull("SSLContext should not be null", sslContext);
    assertEquals("Protocol should be TLS", "TLS", sslContext.getProtocol().substring(0, 3));
  }

  @Test
  public void testCreateSslContextWithSystemDefault() {
    TlsConfig config = TlsConfig.systemDefault();
    SSLContext sslContext = TlsContextFactory.createSslContext(config);

    assertNotNull("SSLContext should not be null", sslContext);
  }

  @Test
  public void testCreateSslContextWithNullConfig() {
    // Should default to trust-all
    SSLContext sslContext = TlsContextFactory.createSslContext((TlsConfig) null);

    assertNotNull("SSLContext should not be null", sslContext);
  }

  @Test
  public void testCreateSslContextWithSessionCacheConfig() {
    TlsSessionCacheConfig sessionConfig =
        TlsSessionCacheConfig.builder()
            .withSessionCacheSize(200)
            .withSessionTimeoutSeconds(3600)
            .build();

    TlsConfig config =
        TlsConfig.builder()
            .withTrustAllCertificates(true)
            .withSessionCacheConfig(sessionConfig)
            .build();

    SSLContext sslContext = TlsContextFactory.createSslContext(config);

    assertNotNull("SSLContext should not be null", sslContext);
    // Verify session cache settings are applied
    assertEquals(
        "Session cache size should be configured",
        200,
        sslContext.getClientSessionContext().getSessionCacheSize());
    assertEquals(
        "Session timeout should be configured",
        3600,
        sslContext.getClientSessionContext().getSessionTimeout());
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testLegacyCreateSslContextWithTlsSessionCacheConfig() {
    TlsSessionCacheConfig config =
        TlsSessionCacheConfig.builder()
            .withSessionCacheSize(100)
            .withSessionTimeoutSeconds(1800)
            .build();

    SSLContext sslContext = TlsContextFactory.createSslContext(config);

    assertNotNull("SSLContext should not be null", sslContext);
    assertEquals(
        "Session cache size should be configured",
        100,
        sslContext.getClientSessionContext().getSessionCacheSize());
  }

  @Test
  public void testCreateSslContextWithCustomCaCert() throws IOException {
    // Create a temporary self-signed certificate for testing
    Path tempCert = createTempCertificate();
    try {
      TlsConfig config =
          TlsConfig.builder().withCaCertPath(tempCert).withTrustSystemCaCerts(false).build();

      SSLContext sslContext = TlsContextFactory.createSslContext(config);

      assertNotNull("SSLContext should not be null", sslContext);
    } finally {
      Files.deleteIfExists(tempCert);
    }
  }

  @Test
  public void testCreateSslContextWithCustomCaCertAndSystemCas() throws IOException {
    // Create a temporary self-signed certificate for testing
    Path tempCert = createTempCertificate();
    try {
      TlsConfig config =
          TlsConfig.builder().withCaCertPath(tempCert).withTrustSystemCaCerts(true).build();

      SSLContext sslContext = TlsContextFactory.createSslContext(config);

      assertNotNull("SSLContext should not be null", sslContext);
    } finally {
      Files.deleteIfExists(tempCert);
    }
  }

  @Test(expected = RuntimeException.class)
  public void testCreateSslContextWithInvalidCaCertPath() throws IOException {
    Path invalidPath = Files.createTempFile("invalid", ".pem");
    Files.write(invalidPath, "invalid certificate data".getBytes());

    try {
      TlsConfig config =
          TlsConfig.builder().withCaCertPath(invalidPath).withTrustSystemCaCerts(false).build();

      TlsContextFactory.createSslContext(config);
    } finally {
      Files.deleteIfExists(invalidPath);
    }
  }

  @Test(expected = RuntimeException.class)
  public void testCreateSslContextWithNonExistentCaCertPath() {
    Path nonExistentPath = Path.of("/non/existent/path/ca.pem");

    TlsConfig config =
        TlsConfig.builder().withCaCertPath(nonExistentPath).withTrustSystemCaCerts(false).build();

    TlsContextFactory.createSslContext(config);
  }

  @Test
  public void testCreateSslContextWithSessionCacheDisabled() {
    TlsConfig config =
        TlsConfig.builder()
            .withTrustAllCertificates(true)
            .withSessionCacheConfig(TlsSessionCacheConfig.disabled())
            .build();

    SSLContext sslContext = TlsContextFactory.createSslContext(config);

    assertNotNull("SSLContext should not be null", sslContext);
    assertEquals(
        "Session cache size should be 0 when disabled",
        0,
        sslContext.getClientSessionContext().getSessionCacheSize());
  }

  /**
   * Creates a temporary self-signed certificate for testing.
   *
   * <p>This certificate was generated with: {@code openssl req -x509 -nodes -days 3650 -newkey
   * rsa:2048 -keyout key.pem -out cert.pem -subj "/CN=test-ca"}
   *
   * @return the path to the temporary certificate file
   * @throws IOException if the file cannot be created
   */
  private Path createTempCertificate() throws IOException {
    // This is a self-signed test certificate (PEM format)
    // Generated for testing purposes only - DO NOT use in production
    String testCert =
        "-----BEGIN CERTIFICATE-----\n"
            + "MIIDBTCCAe2gAwIBAgIUSSxkkPZdq+pTKeALhEEpIrfWjeEwDQYJKoZIhvcNAQEL\n"
            + "BQAwEjEQMA4GA1UEAwwHdGVzdC1jYTAeFw0yNjAxMzEwMTM0MjNaFw0zNjAxMjkw\n"
            + "MTM0MjNaMBIxEDAOBgNVBAMMB3Rlc3QtY2EwggEiMA0GCSqGSIb3DQEBAQUAA4IB\n"
            + "DwAwggEKAoIBAQC7fotfXqSlJk0uM0RE/fbzV6bXsli9dezhC3sOP1XIS7DVnua7\n"
            + "xYL6TnvWR2Y64x68REBXSl6iNYqaIxtlJdf5NwL1tqPI6qLA6y3o/hGXIqRZekke\n"
            + "fv21BODJ/rHu9MrkBIkI+vFDazAUXz3agHV1Qnre3CRDPvHPmNVOac9exU1+qOXh\n"
            + "FBYZOzKKrLkaTVxMjGOl8tzASTbrkBHnbUV1kzIraIvsM+S8AZDZTukCZ+370a3O\n"
            + "MDx8CTv1kONVrpXaVnL9QLGFkpLx1jT1ApeZsMb7twgn/oEFJl8E7BXbG1T+lhza\n"
            + "KF/wf9KXS+fC/xv1G7/ywftScdzP/0IGQEw1AgMBAAGjUzBRMB0GA1UdDgQWBBQI\n"
            + "n37WLBvpKEKBa3LcMuSbyJYeHDAfBgNVHSMEGDAWgBQIn37WLBvpKEKBa3LcMuSb\n"
            + "yJYeHDAPBgNVHRMBAf8EBTADAQH/MA0GCSqGSIb3DQEBCwUAA4IBAQBGG3rmBhh0\n"
            + "eHWas+EdD3lifaJt36LdkBAkwE/HkZjCMLlZwyk09uBJCTb+5M5D8cFh17LqXdUz\n"
            + "1xql2yok+r3UojkTGxeShIZZ7Lwkg6nCAygsvu9QZkv2bz382MHhvwoPoGqSpOen\n"
            + "O/WPAZCpDMCfymhvE2C2yX9EUZNj+z3HRLgsyBqqAvx5NS7/whB3rZlLWuH+NMJp\n"
            + "5At2UHnrXSFomBdJOkpvxQFGm8yHES79Yopc4VHA9lD8hQ1yF+Q23b64q161Vemn\n"
            + "bA2+T05xQusR5P6tWOxwqQ1SXXaQYBh7czcms441WZV26Y0SRJTzne8iaNeCLTOJ\n"
            + "11uOp5H+Pi7g\n"
            + "-----END CERTIFICATE-----\n";

    Path tempFile = Files.createTempFile("test-ca-", ".pem");
    Files.write(tempFile, testCert.getBytes());
    return tempFile;
  }
}
