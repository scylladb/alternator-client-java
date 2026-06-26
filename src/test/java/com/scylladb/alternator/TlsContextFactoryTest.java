package com.scylladb.alternator;

import static org.junit.Assert.*;

import com.scylladb.alternator.internal.TlsContextFactory;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
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

  @Test
  public void testCreateSslContextWithClientCertificate() throws IOException {
    Path clientCert = createTempClientCertificate();
    Path clientKey = createTempClientPrivateKey();
    try {
      TlsConfig config =
          TlsConfig.builder()
              .withTrustAllCertificates(true)
              .withClientCertificate(clientCert, clientKey)
              .build();

      SSLContext sslContext = TlsContextFactory.createSslContext(config);

      assertNotNull("SSLContext should not be null", sslContext);
    } finally {
      Files.deleteIfExists(clientCert);
      Files.deleteIfExists(clientKey);
    }
  }

  @Test
  public void testCreateSslContextWithInvalidClientPrivateKeyFails() throws IOException {
    Path clientCert = createTempClientCertificate();
    Path clientKey = Files.createTempFile("invalid-client-key-", ".pem");
    try {
      Files.write(clientKey, "not a private key".getBytes(StandardCharsets.US_ASCII));
      TlsConfig config =
          TlsConfig.builder()
              .withTrustAllCertificates(true)
              .withClientCertificate(clientCert, clientKey)
              .build();

      TlsContextFactory.createSslContext(config);
      fail("Should fail with invalid client private key");
    } catch (RuntimeException e) {
      String message = e.toString();
      if (e.getCause() != null) {
        message += " " + e.getCause();
      }
      assertTrue("Error should mention private key", message.toLowerCase().contains("private key"));
    } finally {
      Files.deleteIfExists(clientCert);
      Files.deleteIfExists(clientKey);
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

  private Path createTempClientCertificate() throws IOException {
    String testCert =
        "-----BEGIN CERTIFICATE-----\n"
            + "MIIDDTCCAfWgAwIBAgIUQ9vAVQDGKT4oOuaelYW6Vo+LedUwDQYJKoZIhvcNAQEL\n"
            + "BQAwFjEUMBIGA1UEAwwLdGVzdC1jbGllbnQwHhcNMjYwNjI2MTEyNTM3WhcNMzYw\n"
            + "NjIzMTEyNTM3WjAWMRQwEgYDVQQDDAt0ZXN0LWNsaWVudDCCASIwDQYJKoZIhvcN\n"
            + "AQEBBQADggEPADCCAQoCggEBAK/2xNeIpdnAPFnmMv4A2+++8QJyBXcUV05fY/Jv\n"
            + "PZSURWE84YyWT53XiY2qLnI+mpO8OXJNf5SXy7r6E5UYV7T6aM6s5LpxRyvhNosv\n"
            + "d7K/ZXimfMKqXXMKeYRuw2FdAGEAjF3vP6FAayEKgtL77/7TeWSZmvOnhVnDSZZo\n"
            + "uY/AEHnqNBkeLtEyDJmZX4Ed0ffnzG7qNyTIs9/tv+/UvH6WmGerbjDQa5ygfZyV\n"
            + "r+F+GSxgm41deHVBWcU2E5onmqtHpxdVH8JVnDS0BbAOi7AqZBm0198NX0EaHJS9\n"
            + "pfApQO/hubtwc6tQcvT+YYnvltkfBa/WRVzE9glS687aiy0CAwEAAaNTMFEwHQYD\n"
            + "VR0OBBYEFKbGjhXTEZvHZHZZRfaDjR8qfvSgMB8GA1UdIwQYMBaAFKbGjhXTEZvH\n"
            + "ZHZZRfaDjR8qfvSgMA8GA1UdEwEB/wQFMAMBAf8wDQYJKoZIhvcNAQELBQADggEB\n"
            + "AA7+bjX6LqzuByr64mMZBOLrIBtUNHbxrFRcxpJ4vU95XTC4fJS9WGlnJecNqSkE\n"
            + "QFx3uN6YUCcRuz7fWnZT2it+0AyyWgjbj+lOZklekTQz8+KklWSfOACYomEfDXoD\n"
            + "W/rjqrUh7LD13THen2xaYFctzqxRachrpr1HCLsotsm9ORfaIOKlTyemGpyZsNje\n"
            + "qx1LPB/1TRrkIcIyHjyd16baTvuhDlbyGmZFpXNqpRoVsg6Q0knNGIggqVWYZQHi\n"
            + "kWnyK1TM5SE/Gosh9MRY4Hz44yQYOr8chmgRh/5tro+FWx8KKVW6U8IEnslN1QAu\n"
            + "IdiE7tP9gWke/Fesa1mNRHY=\n"
            + "-----END CERTIFICATE-----\n";

    Path tempFile = Files.createTempFile("test-client-cert-", ".pem");
    Files.write(tempFile, testCert.getBytes(StandardCharsets.US_ASCII));
    return tempFile;
  }

  private Path createTempClientPrivateKey() throws IOException {
    String testKey =
        "-----BEGIN PRIVATE KEY-----\n"
            + "MIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQCv9sTXiKXZwDxZ\n"
            + "5jL+ANvvvvECcgV3FFdOX2Pybz2UlEVhPOGMlk+d14mNqi5yPpqTvDlyTX+Ul8u6\n"
            + "+hOVGFe0+mjOrOS6cUcr4TaLL3eyv2V4pnzCql1zCnmEbsNhXQBhAIxd7z+hQGsh\n"
            + "CoLS++/+03lkmZrzp4VZw0mWaLmPwBB56jQZHi7RMgyZmV+BHdH358xu6jckyLPf\n"
            + "7b/v1Lx+lphnq24w0GucoH2cla/hfhksYJuNXXh1QVnFNhOaJ5qrR6cXVR/CVZw0\n"
            + "tAWwDouwKmQZtNffDV9BGhyUvaXwKUDv4bm7cHOrUHL0/mGJ75bZHwWv1kVcxPYJ\n"
            + "UuvO2ostAgMBAAECggEAP60pJmYIvnfWXyHyqgBdlrCePqhMWf8+aNoULRMcUbwm\n"
            + "Lz380QdD4HazDFTdYfJNtdCTaU2qMXeM/iipFXctpgxICSJ/0whTHQnu1wdiZYjl\n"
            + "i3eUBk3oa00LFGWQxpcFIBU2tndxq0TIf7hyBy+sdabJcyIy9KFWnFkTNB7Jp77b\n"
            + "i4JhRAk7JRG0tWZKRGCYyRh6Zi7ywlZEnXtT0RuAHcnfR1KtCREeA7ISkHqvCo0c\n"
            + "l3HxwmlLNfLYrbD20HTlmvpGOBgNtFTM1TB0EQREafjmXETgHXq+8FGo+1vcrfxA\n"
            + "zthgGFsJ7iaGU3K816SoNxNTTDX7hTIFKxPJAUnE8QKBgQDOXg378gOialyMur/z\n"
            + "ZjlW/98mhiFcwgzVulsSuKwE7FvYATq4Ad03HW3h0gmf+9BDDGDXnG4WC5tGPLYN\n"
            + "61RqxVepKtt67n3hcyzZDOHAUz9p53xF91Kyn0d1dyUUoGXkqp1TcVKal2UaElup\n"
            + "53akCWpJVQxjLFPAxMBrz6nX8wKBgQDaSMmO6eTjNP9y7UgMAY88Nqu8PyAPyFHV\n"
            + "utaq1gROHhp8gxbP4Hkb1Wwj0PeUWkzTWwnmLVnMuWAktks15Z7VZjgsAwhTkCgG\n"
            + "/HOZ8n0W2z3J0xvlV5JMfFaW6bsF3wQu24Mw9RDpxlMC8MlQoWB34zosD2L+hcmn\n"
            + "Sb5H++D4XwKBgQCoXRvTnVNRwqzXM9U+4vuM+xw39d5qKvcFuBBtabUOHzefNwGM\n"
            + "9hhgyuXHAvFPUMZMrWClB77YxYdc+lMdcA1jPrWSEqEV3lVdBfZk7pmPq1tlL7K3\n"
            + "8lvJ1yEZuKbL+UCoGnpYhW/7J+EYMDoQmAK3Oec5BOYiUxvRfbPvQXEz+QKBgQCv\n"
            + "qUuq2sb7oTbBQfpszwR5rHVftF0U1lwk54rBSCGGy+r8sHG3MCnGIGY6HHxgwpp4\n"
            + "rBa3SV+uxK9+W8UCxpqfmPczU+1rceMEXDybcuz/a8e5l04nreVp79Wu9MEw5Fv1\n"
            + "aWmWCGFn/9Xl0+fuHzAGyrGRq4A622eAXHPoceaFeQKBgQCtrlbPMYRcsQ3XFWqr\n"
            + "+j1m170TDDZA2f9Xmabw1gDlXJzl56FjCHw1pwEar951Zz4YxXlAxBFrsSSAoejY\n"
            + "2k767/Yt4BXgICmLWwzWaLjrXDlTzF0/MAlkrkU/8DbxWJr9ipfk3jV+MRq5lyub\n"
            + "XnY1EPIr829qOLS66QxTxZUX3w==\n"
            + "-----END PRIVATE KEY-----\n";

    Path tempFile = Files.createTempFile("test-client-key-", ".pem");
    Files.write(tempFile, testKey.getBytes(StandardCharsets.US_ASCII));
    return tempFile;
  }
}
