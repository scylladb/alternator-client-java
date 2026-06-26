package com.scylladb.alternator;

import static org.junit.Assert.*;

import com.scylladb.alternator.internal.AsyncClientDetector;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.Arrays;
import java.util.function.UnaryOperator;
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
    builder.validateAndDetectAsyncClientType();
  }

  @Test
  public void testCrtCustomizerWithConfig() {
    AlternatorDynamoDbAsyncClient.AlternatorDynamoDbAsyncClientBuilder builder =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(SEED_URI)
            .withMaxConnections(100)
            .withConnectionMaxIdleTimeMs(30000)
            .withCrtAsyncHttpClientCustomizer(b -> b.maxConcurrency(200));
    builder.validateAndDetectAsyncClientType();
  }

  @Test(expected = IllegalStateException.class)
  public void testApacheHttpClientTypeOnAsyncBuilder() {
    AlternatorDynamoDbAsyncClient.builder()
        .endpointOverride(SEED_URI)
        .withHttpClientType(HttpClientType.APACHE)
        .build();
  }

  @Test(expected = IllegalStateException.class)
  public void testHttpClientTypeNettyConflictsWithCrtCustomizer() {
    AlternatorDynamoDbAsyncClient.builder()
        .endpointOverride(SEED_URI)
        .withHttpClientType(HttpClientType.NETTY)
        .withCrtAsyncHttpClientCustomizer(b -> {})
        .build();
  }

  @Test(expected = IllegalStateException.class)
  public void testHttpClientTypeCrtConflictsWithNettyCustomizer() {
    AlternatorDynamoDbAsyncClient.builder()
        .endpointOverride(SEED_URI)
        .withHttpClientType(HttpClientType.CRT)
        .withNettyHttpClientCustomizer(b -> {})
        .build();
  }

  @Test(expected = IllegalStateException.class)
  public void testHttpClientTypeConflictsWithHttpClient() {
    SdkAsyncHttpClient httpClient = NettyNioAsyncHttpClient.create();
    try {
      AlternatorDynamoDbAsyncClient.builder()
          .endpointOverride(SEED_URI)
          .httpClient(httpClient)
          .withHttpClientType(HttpClientType.NETTY)
          .build();
    } finally {
      httpClient.close();
    }
  }

  @Test
  public void testHttpClientTypeBuilderReturnsThis() {
    AlternatorDynamoDbAsyncClient.AlternatorDynamoDbAsyncClientBuilder builder =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(SEED_URI)
            .withHttpClientType(HttpClientType.NETTY);
    assertNotNull("Builder should return itself for chaining", builder);
  }

  @Test
  public void testUserAgentBuilderMethodsReturnThis() {
    AlternatorDynamoDbAsyncClient.AlternatorDynamoDbAsyncClientBuilder builder =
        AlternatorDynamoDbAsyncClient.builder().endpointOverride(SEED_URI);

    assertSame(builder, builder.withUserAgent("custom/1"));
    assertSame(builder, builder.withUserAgent(userAgent -> userAgent + " app/1"));
    assertSame(builder, builder.withoutUserAgent());
  }

  @Test
  public void testSeedHostsBuilderPreservesExplicitSeeds() {
    AlternatorDynamoDbAsyncClientWrapper wrapper =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(SEED_URI)
            .withSeedHosts(Arrays.asList("127.0.0.1", "127.0.0.2"))
            .buildWithAlternatorAPI();
    try {
      AlternatorConfig config = wrapper.getAlternatorConfig();
      assertEquals(Arrays.asList("127.0.0.1", "127.0.0.2"), config.getSeedHosts());
      assertEquals(SEED_URI.getScheme(), config.getScheme());
      assertEquals(SEED_URI.getPort(), config.getPort());
    } finally {
      wrapper.close();
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWithUserAgentRejectsNullString() {
    AlternatorDynamoDbAsyncClient.builder().endpointOverride(SEED_URI).withUserAgent((String) null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWithUserAgentRejectsBlankString() {
    AlternatorDynamoDbAsyncClient.builder().endpointOverride(SEED_URI).withUserAgent(" ");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWithUserAgentRejectsNullTransformer() {
    AlternatorDynamoDbAsyncClient.builder()
        .endpointOverride(SEED_URI)
        .withUserAgent((UnaryOperator<String>) null);
  }

  @Test
  public void testHttpClientTypeAutoWithNettyCustomizerPassesValidation() {
    AlternatorDynamoDbAsyncClient.AlternatorDynamoDbAsyncClientBuilder builder =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(SEED_URI)
            .withHttpClientType(HttpClientType.AUTO)
            .withNettyHttpClientCustomizer(b -> {});
    builder.validateAndDetectAsyncClientType();
  }

  @Test
  public void testHttpClientTypeNettyWithMatchingCustomizerPassesValidation() {
    AlternatorDynamoDbAsyncClient.AlternatorDynamoDbAsyncClientBuilder builder =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(SEED_URI)
            .withHttpClientType(HttpClientType.NETTY)
            .withNettyHttpClientCustomizer(b -> b.maxConcurrency(100));
    builder.validateAndDetectAsyncClientType();
  }

  @Test
  public void testHttpClientTypeCrtWithMatchingCustomizerPassesValidation() {
    AlternatorDynamoDbAsyncClient.AlternatorDynamoDbAsyncClientBuilder builder =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(SEED_URI)
            .withHttpClientType(HttpClientType.CRT)
            .withCrtAsyncHttpClientCustomizer(b -> b.maxConcurrency(200));
    builder.validateAndDetectAsyncClientType();
  }

  @Test
  public void testHttpClientTypeAutoAlonePassesValidation() {
    AlternatorDynamoDbAsyncClient.AlternatorDynamoDbAsyncClientBuilder builder =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(SEED_URI)
            .withHttpClientType(HttpClientType.AUTO);
    builder.validateAndDetectAsyncClientType();
  }

  @Test(expected = IllegalStateException.class)
  public void testHttpClientTypeAutoConflictsWithHttpClient() {
    SdkAsyncHttpClient httpClient = NettyNioAsyncHttpClient.create();
    try {
      AlternatorDynamoDbAsyncClient.builder()
          .endpointOverride(SEED_URI)
          .httpClient(httpClient)
          .withHttpClientType(HttpClientType.AUTO)
          .build();
    } finally {
      httpClient.close();
    }
  }

  @Test(expected = NullPointerException.class)
  public void testHttpClientTypeRejectsNull() {
    AlternatorDynamoDbAsyncClient.builder().endpointOverride(SEED_URI).withHttpClientType(null);
  }

  @Test
  public void testHttpClientTypeNettyAlonePassesValidation() {
    var builder =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(SEED_URI)
            .withHttpClientType(HttpClientType.NETTY);
    builder.validateAndDetectAsyncClientType();
  }

  @Test
  public void testHttpClientTypeCrtAlonePassesValidation() {
    var builder =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(SEED_URI)
            .withHttpClientType(HttpClientType.CRT);
    builder.validateAndDetectAsyncClientType();
  }

  @Test
  public void testHttpClientTypeAutoWithCrtAsyncCustomizerPassesValidation() {
    var builder =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(SEED_URI)
            .withHttpClientType(HttpClientType.AUTO)
            .withCrtAsyncHttpClientCustomizer(b -> {});
    builder.validateAndDetectAsyncClientType();
  }

  @Test
  public void testDetectAsyncClientTypeWithNettyCustomizerOnly() {
    assertEquals(
        AsyncClientDetector.AsyncClientType.NETTY,
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(SEED_URI)
            .withNettyHttpClientCustomizer(b -> {})
            .validateAndDetectAsyncClientType());
  }

  @Test
  public void testDetectAsyncClientTypeWithCrtCustomizerOnly() {
    assertEquals(
        AsyncClientDetector.AsyncClientType.CRT,
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(SEED_URI)
            .withCrtAsyncHttpClientCustomizer(b -> {})
            .validateAndDetectAsyncClientType());
  }

  @Test
  public void testDetectAsyncClientTypeWithExplicitNetty() {
    assertEquals(
        AsyncClientDetector.AsyncClientType.NETTY,
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(SEED_URI)
            .withHttpClientType(HttpClientType.NETTY)
            .validateAndDetectAsyncClientType());
  }

  @Test
  public void testDetectAsyncClientTypeWithExplicitCrt() {
    assertEquals(
        AsyncClientDetector.AsyncClientType.CRT,
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(SEED_URI)
            .withHttpClientType(HttpClientType.CRT)
            .validateAndDetectAsyncClientType());
  }

  @Test
  public void testHttpClientTypeAutoResolveSameAsNull() {
    AsyncClientDetector.AsyncClientType autoResult =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(SEED_URI)
            .withHttpClientType(HttpClientType.AUTO)
            .validateAndDetectAsyncClientType();
    AsyncClientDetector.AsyncClientType nullResult =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(SEED_URI)
            .validateAndDetectAsyncClientType();
    assertEquals(
        "AUTO and null (default) should resolve to the same client type", nullResult, autoResult);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testHttpClientTypeCrtWithAlternatorConfigPassesValidation() {
    AlternatorConfig config = AlternatorConfig.builder().withMaxConnections(100).build();
    var builder =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(SEED_URI)
            .withAlternatorConfig(config)
            .withHttpClientType(HttpClientType.CRT);
    builder.validateAndDetectAsyncClientType();
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testHttpClientTypeNettyWithAlternatorConfigPassesValidation() {
    AlternatorConfig config = AlternatorConfig.builder().withMaxConnections(100).build();
    var builder =
        AlternatorDynamoDbAsyncClient.builder()
            .endpointOverride(SEED_URI)
            .withHttpClientType(HttpClientType.NETTY)
            .withAlternatorConfig(config);
    builder.validateAndDetectAsyncClientType();
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testWithAlternatorConfigDisablesUserAgentTransformer() throws Exception {
    AlternatorConfig config =
        AlternatorConfig.builder().withOptimizeHeaders(true).withUserAgentEnabled(false).build();
    var builder = AlternatorDynamoDbAsyncClient.builder().withAlternatorConfig(config);

    assertNull(userAgentTransformer(builder).apply("aws-sdk-java/2.x"));
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testWithAlternatorConfigEnablesDefaultUserAgentTransformer() throws Exception {
    AlternatorConfig config = AlternatorConfig.builder().withUserAgentEnabled(true).build();
    var builder =
        AlternatorDynamoDbAsyncClient.builder().withoutUserAgent().withAlternatorConfig(config);

    assertEquals(
        AlternatorUserAgent.userAgentToken(),
        userAgentTransformer(builder).apply("aws-sdk-java/2.x"));
  }

  @Test(expected = IllegalStateException.class)
  public void testHttpClientTypeConflictsWithHttpClientBuilder() {
    AlternatorDynamoDbAsyncClient.builder()
        .endpointOverride(SEED_URI)
        .httpClientBuilder(NettyNioAsyncHttpClient.builder())
        .withHttpClientType(HttpClientType.NETTY)
        .build();
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

  @SuppressWarnings("unchecked")
  private UnaryOperator<String> userAgentTransformer(
      AlternatorDynamoDbAsyncClient.AlternatorDynamoDbAsyncClientBuilder builder) throws Exception {
    Field field = builder.getClass().getDeclaredField("userAgentTransformer");
    field.setAccessible(true);
    return (UnaryOperator<String>) field.get(builder);
  }
}
