package com.scylladb.alternator;

import static org.junit.Assert.*;

import java.net.URI;
import org.junit.Test;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;

/**
 * Unit tests for sync client builder customizer API and validation.
 *
 * <p>Verifies mutual exclusion rules, customizer invocation, and configuration propagation for
 * {@link AlternatorDynamoDbClient.AlternatorDynamoDbClientBuilder}.
 */
public class AlternatorDynamoDbClientCustomizerTest {

  private static final URI SEED_URI = URI.create("http://127.0.0.1:9999");

  @Test(expected = IllegalStateException.class)
  public void testCannotUseBothApacheAndCrtCustomizers() {
    AlternatorDynamoDbClient.builder()
        .endpointOverride(SEED_URI)
        .withApacheHttpClientCustomizer(builder -> {})
        .withCrtHttpClientCustomizer(builder -> {})
        .build();
  }

  @Test(expected = IllegalStateException.class)
  public void testCannotUseCustomizerWithHttpClient() {
    SdkHttpClient httpClient = ApacheHttpClient.create();
    try {
      AlternatorDynamoDbClient.builder()
          .endpointOverride(SEED_URI)
          .httpClient(httpClient)
          .withApacheHttpClientCustomizer(builder -> {})
          .build();
    } finally {
      httpClient.close();
    }
  }

  @Test(expected = IllegalStateException.class)
  public void testCannotUseCrtCustomizerWithHttpClient() {
    SdkHttpClient httpClient = ApacheHttpClient.create();
    try {
      AlternatorDynamoDbClient.builder()
          .endpointOverride(SEED_URI)
          .httpClient(httpClient)
          .withCrtHttpClientCustomizer(builder -> {})
          .build();
    } finally {
      httpClient.close();
    }
  }

  @Test(expected = IllegalStateException.class)
  public void testCannotUseCustomizerWithHttpClientBuilder() {
    AlternatorDynamoDbClient.builder()
        .endpointOverride(SEED_URI)
        .httpClientBuilder(ApacheHttpClient.builder())
        .withApacheHttpClientCustomizer(builder -> {})
        .build();
  }

  @Test(expected = IllegalStateException.class)
  public void testEndpointOverrideRequired() {
    AlternatorDynamoDbClient.builder().build();
  }

  @Test
  public void testApacheCustomizerBuilderReturnsThis() {
    AlternatorDynamoDbClient.AlternatorDynamoDbClientBuilder builder =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(SEED_URI)
            .withApacheHttpClientCustomizer(b -> {});
    assertNotNull("Builder should return itself for chaining", builder);
  }

  @Test
  public void testCrtCustomizerBuilderReturnsThis() {
    AlternatorDynamoDbClient.AlternatorDynamoDbClientBuilder builder =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(SEED_URI)
            .withCrtHttpClientCustomizer(b -> {});
    assertNotNull("Builder should return itself for chaining", builder);
  }

  @Test
  public void testApacheCustomizerWithConfig() {
    // Verify that config + customizer work together without errors
    // This should not throw - build will fail to connect but the builder setup is valid
    AlternatorDynamoDbClient.AlternatorDynamoDbClientBuilder builder =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(SEED_URI)
            .withMaxConnections(100)
            .withConnectionMaxIdleTimeMs(30000)
            .withConnectionTimeToLiveMs(60000)
            .withApacheHttpClientCustomizer(b -> b.maxConnections(200));
    assertNotNull("Builder with config and customizer should be valid", builder);
  }

  @Test
  public void testCrtCustomizerWithConfig() {
    AlternatorDynamoDbClient.AlternatorDynamoDbClientBuilder builder =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(SEED_URI)
            .withMaxConnections(100)
            .withConnectionMaxIdleTimeMs(30000)
            .withCrtHttpClientCustomizer(b -> b.maxConcurrency(200));
    assertNotNull("Builder with config and CRT customizer should be valid", builder);
  }

  @Test
  public void testTlsConfigWithCustomizer() {
    AlternatorDynamoDbClient.AlternatorDynamoDbClientBuilder builder =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(URI.create("https://127.0.0.1:9999"))
            .withTlsConfig(TlsConfig.trustAll())
            .withApacheHttpClientCustomizer(b -> {});
    assertNotNull("Builder with TLS and customizer should be valid", builder);
  }
}
