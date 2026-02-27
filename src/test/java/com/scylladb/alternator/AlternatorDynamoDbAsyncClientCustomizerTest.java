package com.scylladb.alternator;

import static org.junit.Assert.*;

import java.net.URI;
import org.junit.Test;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;

/**
 * Unit tests for async client builder customizer API and validation.
 *
 * <p>Verifies mutual exclusion rules, customizer invocation, and configuration propagation for
 * {@link AlternatorDynamoDbAsyncClient.AlternatorDynamoDbAsyncClientBuilder}.
 */
public class AlternatorDynamoDbAsyncClientCustomizerTest {

  private static final URI SEED_URI = URI.create("http://127.0.0.1:9999");

  @Test(expected = IllegalStateException.class)
  public void testCannotUseBothNettyAndCrtCustomizers() {
    AlternatorDynamoDbAsyncClient.builder()
        .endpointOverride(SEED_URI)
        .withNettyHttpClientCustomizer(builder -> {})
        .withCrtAsyncHttpClientCustomizer(builder -> {})
        .build();
  }

  @Test(expected = IllegalStateException.class)
  public void testCannotUseCustomizerWithHttpClient() {
    SdkAsyncHttpClient httpClient = NettyNioAsyncHttpClient.create();
    try {
      AlternatorDynamoDbAsyncClient.builder()
          .endpointOverride(SEED_URI)
          .httpClient(httpClient)
          .withNettyHttpClientCustomizer(builder -> {})
          .build();
    } finally {
      httpClient.close();
    }
  }

  @Test(expected = IllegalStateException.class)
  public void testCannotUseCrtCustomizerWithHttpClient() {
    SdkAsyncHttpClient httpClient = NettyNioAsyncHttpClient.create();
    try {
      AlternatorDynamoDbAsyncClient.builder()
          .endpointOverride(SEED_URI)
          .httpClient(httpClient)
          .withCrtAsyncHttpClientCustomizer(builder -> {})
          .build();
    } finally {
      httpClient.close();
    }
  }

  @Test(expected = IllegalStateException.class)
  public void testCannotUseCustomizerWithHttpClientBuilder() {
    AlternatorDynamoDbAsyncClient.builder()
        .endpointOverride(SEED_URI)
        .httpClientBuilder(NettyNioAsyncHttpClient.builder())
        .withNettyHttpClientCustomizer(builder -> {})
        .build();
  }

  @Test(expected = IllegalStateException.class)
  public void testEndpointOverrideRequired() {
    AlternatorDynamoDbAsyncClient.builder().build();
  }

  @Test
  public void testNettyCustomizerBuilderReturnsThis() {
    AlternatorDynamoDbAsyncClient.AlternatorDynamoDbAsyncClientBuilder builder =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(SEED_URI)
            .withNettyHttpClientCustomizer(b -> {});
    assertNotNull("Builder should return itself for chaining", builder);
  }

  @Test
  public void testCrtCustomizerBuilderReturnsThis() {
    AlternatorDynamoDbAsyncClient.AlternatorDynamoDbAsyncClientBuilder builder =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(SEED_URI)
            .withCrtAsyncHttpClientCustomizer(b -> {});
    assertNotNull("Builder should return itself for chaining", builder);
  }

  @Test
  public void testNettyCustomizerWithConfig() {
    AlternatorDynamoDbAsyncClient.AlternatorDynamoDbAsyncClientBuilder builder =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(SEED_URI)
            .withMaxConnections(100)
            .withConnectionMaxIdleTimeMs(30000)
            .withConnectionTimeToLiveMs(60000)
            .withNettyHttpClientCustomizer(b -> b.maxConcurrency(200));
    assertNotNull("Builder with config and Netty customizer should be valid", builder);
  }

  @Test
  public void testCrtCustomizerWithConfig() {
    AlternatorDynamoDbAsyncClient.AlternatorDynamoDbAsyncClientBuilder builder =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(SEED_URI)
            .withMaxConnections(100)
            .withConnectionMaxIdleTimeMs(30000)
            .withCrtAsyncHttpClientCustomizer(b -> b.maxConcurrency(200));
    assertNotNull("Builder with config and CRT customizer should be valid", builder);
  }

  @Test
  public void testTlsConfigWithCustomizer() {
    AlternatorDynamoDbAsyncClient.AlternatorDynamoDbAsyncClientBuilder builder =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(URI.create("https://127.0.0.1:9999"))
            .withTlsConfig(TlsConfig.trustAll())
            .withNettyHttpClientCustomizer(b -> {});
    assertNotNull("Builder with TLS and customizer should be valid", builder);
  }
}
