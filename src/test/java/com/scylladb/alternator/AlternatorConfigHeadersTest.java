package com.scylladb.alternator;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.junit.Test;

/**
 * Unit tests for AlternatorConfig headers optimization configuration.
 *
 * @author dmitry.kropachev
 */
public class AlternatorConfigHeadersTest {

  @Test
  public void testDefaultHeadersOptimizationSettings() {
    AlternatorConfig config = AlternatorConfig.builder().build();

    assertFalse(config.isOptimizeHeaders());
    // Without compression, Content-Encoding is not required
    assertEquals(config.getRequiredHeaders(), config.getHeadersWhitelist());
  }

  @Test
  public void testHeadersOptimizationEnabled() {
    AlternatorConfig config = AlternatorConfig.builder().withOptimizeHeaders(true).build();

    assertTrue(config.isOptimizeHeaders());
    // Without compression, Content-Encoding is not required
    assertEquals(config.getRequiredHeaders(), config.getHeadersWhitelist());
  }

  @Test
  public void testCustomHeadersWhitelist() {
    // Get required headers first, then add custom ones
    Set<String> requiredHeaders = AlternatorConfig.builder().getRequiredHeaders();
    Set<String> customHeaders = new HashSet<>(requiredHeaders);
    customHeaders.add("X-Custom");

    AlternatorConfig config =
        AlternatorConfig.builder()
            .withOptimizeHeaders(true)
            .withHeadersWhitelist(customHeaders)
            .build();

    assertTrue(config.isOptimizeHeaders());
    assertEquals(customHeaders, config.getHeadersWhitelist());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNullHeadersWhitelistThrowsException() {
    AlternatorConfig.builder().withHeadersWhitelist(null).build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyHeadersWhitelistThrowsException() {
    AlternatorConfig.builder().withHeadersWhitelist(Arrays.asList()).build();
  }

  @Test
  public void testHeadersWhitelistIsImmutable() {
    // Get required headers first, then add custom ones
    Set<String> requiredHeaders = AlternatorConfig.builder().getRequiredHeaders();
    Set<String> customHeaders = new HashSet<>(requiredHeaders);

    AlternatorConfig config =
        AlternatorConfig.builder()
            .withOptimizeHeaders(true)
            .withHeadersWhitelist(customHeaders)
            .build();

    // Modifying the original set should not affect the config
    customHeaders.add("X-Modified");
    assertFalse(config.getHeadersWhitelist().contains("X-Modified"));
  }

  @Test
  public void testHeadersOptimizationWithDatacenterAndRack() {
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withDatacenter("us-east")
            .withRack("rack1")
            .withOptimizeHeaders(true)
            .build();

    assertEquals("us-east", config.getDatacenter());
    assertEquals("rack1", config.getRack());
    assertTrue(config.isOptimizeHeaders());
    // Without compression, Content-Encoding is not required
    assertEquals(config.getRequiredHeaders(), config.getHeadersWhitelist());
  }

  @Test
  public void testHeadersOptimizationWithCompression() {
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withCompressionAlgorithm(RequestCompressionAlgorithm.GZIP)
            .withOptimizeHeaders(true)
            .build();

    assertEquals(RequestCompressionAlgorithm.GZIP, config.getCompressionAlgorithm());
    assertTrue(config.isOptimizeHeaders());
    // With compression, Content-Encoding should be included
    assertTrue(config.getHeadersWhitelist().contains("Content-Encoding"));
    assertTrue(config.getRequiredHeaders().contains("Content-Encoding"));
  }

  @Test
  public void testBackwardCompatibilityWithoutHeadersOptimization() {
    // Existing code without headers settings should still work
    AlternatorConfig config =
        AlternatorConfig.builder().withDatacenter("dc1").withRack("rack1").build();

    assertEquals("dc1", config.getDatacenter());
    assertEquals("rack1", config.getRack());
    // Headers optimization defaults to disabled
    assertFalse(config.isOptimizeHeaders());
    // Default whitelist is based on current config (no compression, with auth)
    assertEquals(config.getRequiredHeaders(), config.getHeadersWhitelist());
  }

  @Test
  public void testExplicitlyDisableHeadersOptimization() {
    AlternatorConfig config = AlternatorConfig.builder().withOptimizeHeaders(false).build();

    assertFalse(config.isOptimizeHeaders());
  }

  @Test
  public void testFullHeadersWhitelistContents() {
    // Config with compression=true, auth=true should have all headers
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withCompressionAlgorithm(RequestCompressionAlgorithm.GZIP)
            .authenticationEnabled(true)
            .build();
    Set<String> headers = config.getRequiredHeaders();

    assertTrue(headers.contains("Host"));
    assertTrue(headers.contains("X-Amz-Target"));
    assertTrue(headers.contains("Content-Type"));
    assertTrue(headers.contains("Content-Length"));
    assertTrue(headers.contains("Accept-Encoding"));
    assertTrue(headers.contains("Content-Encoding"));
    assertTrue(headers.contains("Authorization"));
    assertTrue(headers.contains("X-Amz-Date"));
    assertEquals(8, headers.size());
  }

  @Test
  public void testRequiredHeadersIsImmutable() {
    AlternatorConfig config = AlternatorConfig.builder().build();
    try {
      config.getRequiredHeaders().add("X-Should-Fail");
      fail("getRequiredHeaders() should return immutable set");
    } catch (UnsupportedOperationException e) {
      // Expected
    }
  }

  @Test
  public void testAuthenticationEnabledByDefault() {
    AlternatorConfig config = AlternatorConfig.builder().build();

    assertTrue(config.isAuthenticationEnabled());
  }

  @Test
  public void testAuthenticationDisabled() {
    AlternatorConfig config =
        AlternatorConfig.builder().authenticationEnabled(false).build();

    assertFalse(config.isAuthenticationEnabled());
  }

  @Test
  public void testAuthenticationDisabledUsesNoAuthWhitelist() {
    AlternatorConfig config =
        AlternatorConfig.builder()
            .authenticationEnabled(false)
            .withOptimizeHeaders(true)
            .build();

    Set<String> whitelist = config.getHeadersWhitelist();

    // Should use the computed whitelist based on config (no compression, no auth)
    assertTrue(whitelist.contains("Host"));
    assertTrue(whitelist.contains("X-Amz-Target"));
    assertTrue(whitelist.contains("Content-Type"));
    assertTrue(whitelist.contains("Content-Length"));
    assertFalse(whitelist.contains("Authorization"));
    assertFalse(whitelist.contains("X-Amz-Date"));
    // Without compression enabled, Content-Encoding is not included
    assertFalse(whitelist.contains("Content-Encoding"));
    assertEquals(config.getRequiredHeaders(), whitelist);
  }

  @Test
  public void testAuthenticationDisabledWithCustomWhitelist() {
    // Get required headers for no-auth config
    Set<String> requiredHeaders =
        AlternatorConfig.builder().authenticationEnabled(false).getRequiredHeaders();
    Set<String> customHeaders = new HashSet<>(requiredHeaders);
    customHeaders.add("X-Custom");

    AlternatorConfig config =
        AlternatorConfig.builder()
            .authenticationEnabled(false)
            .withOptimizeHeaders(true)
            .withHeadersWhitelist(customHeaders)
            .build();

    // Custom whitelist should override the default
    assertEquals(customHeaders, config.getHeadersWhitelist());
  }

  @Test
  public void testHeadersWhitelistNoAuthContents() {
    // Config with compression=true, auth=false
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withCompressionAlgorithm(RequestCompressionAlgorithm.GZIP)
            .authenticationEnabled(false)
            .build();
    Set<String> noAuthHeaders = config.getRequiredHeaders();

    assertTrue(noAuthHeaders.contains("Host"));
    assertTrue(noAuthHeaders.contains("X-Amz-Target"));
    assertTrue(noAuthHeaders.contains("Content-Type"));
    assertTrue(noAuthHeaders.contains("Content-Length"));
    assertTrue(noAuthHeaders.contains("Accept-Encoding"));
    assertTrue(noAuthHeaders.contains("Content-Encoding"));
    assertFalse(noAuthHeaders.contains("Authorization"));
    assertFalse(noAuthHeaders.contains("X-Amz-Date"));
    assertEquals(6, noAuthHeaders.size());
  }

  @Test
  public void testComputeRequiredHeadersVariations() {
    // Test the internal helper via built configs
    AlternatorConfig noCompressionNoAuth =
        AlternatorConfig.builder()
            .withCompressionAlgorithm(RequestCompressionAlgorithm.NONE)
            .authenticationEnabled(false)
            .build();
    assertEquals(5, noCompressionNoAuth.getRequiredHeaders().size());
    assertFalse(noCompressionNoAuth.getRequiredHeaders().contains("Content-Encoding"));
    assertFalse(noCompressionNoAuth.getRequiredHeaders().contains("Authorization"));

    AlternatorConfig withCompressionNoAuth =
        AlternatorConfig.builder()
            .withCompressionAlgorithm(RequestCompressionAlgorithm.GZIP)
            .authenticationEnabled(false)
            .build();
    assertEquals(6, withCompressionNoAuth.getRequiredHeaders().size());
    assertTrue(withCompressionNoAuth.getRequiredHeaders().contains("Content-Encoding"));
    assertFalse(withCompressionNoAuth.getRequiredHeaders().contains("Authorization"));

    AlternatorConfig noCompressionWithAuth =
        AlternatorConfig.builder()
            .withCompressionAlgorithm(RequestCompressionAlgorithm.NONE)
            .authenticationEnabled(true)
            .build();
    assertEquals(7, noCompressionWithAuth.getRequiredHeaders().size());
    assertFalse(noCompressionWithAuth.getRequiredHeaders().contains("Content-Encoding"));
    assertTrue(noCompressionWithAuth.getRequiredHeaders().contains("Authorization"));

    AlternatorConfig withCompressionWithAuth =
        AlternatorConfig.builder()
            .withCompressionAlgorithm(RequestCompressionAlgorithm.GZIP)
            .authenticationEnabled(true)
            .build();
    assertEquals(8, withCompressionWithAuth.getRequiredHeaders().size());
    assertTrue(withCompressionWithAuth.getRequiredHeaders().contains("Content-Encoding"));
    assertTrue(withCompressionWithAuth.getRequiredHeaders().contains("Authorization"));
  }

  @Test
  public void testBuilderGetRequiredHeaders() {
    // Test builder's getRequiredHeaders method
    AlternatorConfig.Builder builder = AlternatorConfig.builder();
    Set<String> defaultRequired = builder.getRequiredHeaders();
    // Default is no compression, with auth
    assertEquals(7, defaultRequired.size());
    assertFalse(defaultRequired.contains("Content-Encoding"));
    assertTrue(defaultRequired.contains("Authorization"));

    // Enable compression
    builder.withCompressionAlgorithm(RequestCompressionAlgorithm.GZIP);
    Set<String> withCompression = builder.getRequiredHeaders();
    assertEquals(8, withCompression.size());
    assertTrue(withCompression.contains("Content-Encoding"));

    // Disable auth
    builder.authenticationEnabled(false);
    Set<String> noAuth = builder.getRequiredHeaders();
    assertEquals(6, noAuth.size());
    assertTrue(noAuth.contains("Content-Encoding"));
    assertFalse(noAuth.contains("Authorization"));
  }

  @Test
  public void testConfigGetRequiredHeaders() {
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withCompressionAlgorithm(RequestCompressionAlgorithm.GZIP)
            .authenticationEnabled(true)
            .build();

    Set<String> required = config.getRequiredHeaders();
    assertEquals(8, required.size());
    assertTrue(required.contains("Content-Encoding"));
    assertTrue(required.contains("Authorization"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCustomWhitelistMissingRequiredHeadersThrows() {
    // This should throw because we're missing required headers
    Set<String> incompleteHeaders = new HashSet<>(Arrays.asList("Host", "Authorization"));

    AlternatorConfig.builder()
        .withOptimizeHeaders(true)
        .withHeadersWhitelist(incompleteHeaders)
        .build();
  }

  @Test
  public void testCustomWhitelistWithAllRequiredHeadersSucceeds() {
    Set<String> requiredHeaders = AlternatorConfig.builder().getRequiredHeaders();

    // Adding all required headers should work
    AlternatorConfig config =
        AlternatorConfig.builder()
            .withOptimizeHeaders(true)
            .withHeadersWhitelist(requiredHeaders)
            .build();

    assertEquals(requiredHeaders, config.getHeadersWhitelist());
  }
}
