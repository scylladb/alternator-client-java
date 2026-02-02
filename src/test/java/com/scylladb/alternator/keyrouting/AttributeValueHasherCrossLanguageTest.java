package com.scylladb.alternator.keyrouting;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * Cross-language compatibility tests for AttributeValueHasher.
 *
 * <p>These tests use exact expected hash values from the cross-language specification to ensure
 * compatibility with implementations in other languages (e.g., Go).
 *
 * <p>Only S (String), N (Number), and B (Binary) types are tested as these are the only partition
 * key types supported by ScyllaDB Alternator.
 *
 * <p>Test vectors from: https://github.com/scylladb/alternator-load-balancing/issues/165
 *
 * @author dmitry.kropachev
 */
public class AttributeValueHasherCrossLanguageTest {

  // ========== Null Input Test ==========

  @Test
  public void testNullInput() {
    assertEquals("Null input should return 0", 0L, AttributeValueHasher.hash(null));
  }

  // ========== String Type Tests ==========

  @Test
  public void testStringHello() {
    AttributeValue value = AttributeValue.builder().s("hello").build();
    assertEquals(8815023923555918238L, AttributeValueHasher.hash(value));
  }

  @Test
  public void testStringEmpty() {
    AttributeValue value = AttributeValue.builder().s("").build();
    assertEquals(8849112093580131862L, AttributeValueHasher.hash(value));
  }

  @Test
  public void testStringUser123() {
    AttributeValue value = AttributeValue.builder().s("user_123").build();
    assertEquals(-4025731529809423594L, AttributeValueHasher.hash(value));
  }

  @Test
  public void testStringUnicode() {
    // "こんにちは" (Japanese "hello")
    AttributeValue value = AttributeValue.builder().s("こんにちは").build();
    assertEquals(-8746014667889746860L, AttributeValueHasher.hash(value));
  }

  // ========== Number Type Tests ==========

  @Test
  public void testNumber42() {
    AttributeValue value = AttributeValue.builder().n("42").build();
    assertEquals(-5061732451827723051L, AttributeValueHasher.hash(value));
  }

  @Test
  public void testNumberNegative12345() {
    AttributeValue value = AttributeValue.builder().n("-12345").build();
    assertEquals(2496798676881075539L, AttributeValueHasher.hash(value));
  }

  @Test
  public void testNumberPi() {
    AttributeValue value = AttributeValue.builder().n("3.14159").build();
    assertEquals(2139945193071104172L, AttributeValueHasher.hash(value));
  }

  @Test
  public void testNumberScientificNotation() {
    AttributeValue value = AttributeValue.builder().n("1.23E10").build();
    assertEquals(-8571981415737439826L, AttributeValueHasher.hash(value));
  }

  // ========== Binary Type Tests ==========

  @Test
  public void testBinary010203() {
    AttributeValue value =
        AttributeValue.builder().b(SdkBytes.fromByteArray(new byte[] {0x01, 0x02, 0x03})).build();
    assertEquals(5026299041734804437L, AttributeValueHasher.hash(value));
  }

  @Test
  public void testBinaryEmpty() {
    AttributeValue value =
        AttributeValue.builder().b(SdkBytes.fromByteArray(new byte[] {})).build();
    assertEquals(8244620721157455449L, AttributeValueHasher.hash(value));
  }

  @Test
  public void testBinaryFF0080() {
    AttributeValue value =
        AttributeValue.builder()
            .b(SdkBytes.fromByteArray(new byte[] {(byte) 0xFF, 0x00, (byte) 0x80}))
            .build();
    assertEquals(14533934253577680L, AttributeValueHasher.hash(value));
  }

  // ========== Type Collision Prevention Tests ==========

  @Test
  public void testTypeCollisionString12345() {
    AttributeValue value = AttributeValue.builder().s("12345").build();
    assertEquals(-6122888897254035317L, AttributeValueHasher.hash(value));
  }

  @Test
  public void testTypeCollisionNumber12345() {
    AttributeValue value = AttributeValue.builder().n("12345").build();
    assertEquals(-3190731486301745196L, AttributeValueHasher.hash(value));
  }

  @Test
  public void testTypeCollisionBinary12345() {
    // "12345" as UTF-8 bytes
    AttributeValue value =
        AttributeValue.builder()
            .b(SdkBytes.fromByteArray("12345".getBytes(java.nio.charset.StandardCharsets.UTF_8)))
            .build();
    assertEquals(-3752463870508600385L, AttributeValueHasher.hash(value));
  }
}
