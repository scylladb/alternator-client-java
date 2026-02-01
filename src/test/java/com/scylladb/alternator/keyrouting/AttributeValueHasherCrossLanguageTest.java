package com.scylladb.alternator.keyrouting;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * Cross-language compatibility tests for AttributeValueHasher.
 *
 * <p>These tests use exact expected hash values from the cross-language specification to ensure
 * compatibility with implementations in other languages (e.g., Go).
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

  // ========== Boolean Type Tests ==========

  @Test
  public void testBooleanTrue() {
    AttributeValue value = AttributeValue.builder().bool(true).build();
    assertEquals(8486936384116756332L, AttributeValueHasher.hash(value));
  }

  @Test
  public void testBooleanFalse() {
    AttributeValue value = AttributeValue.builder().bool(false).build();
    assertEquals(-4126391008895418907L, AttributeValueHasher.hash(value));
  }

  // ========== NULL Type Tests ==========

  @Test
  public void testNullTrue() {
    AttributeValue value = AttributeValue.builder().nul(true).build();
    assertEquals(-561667943985901489L, AttributeValueHasher.hash(value));
  }

  // ========== String Set Tests ==========

  @Test
  public void testStringSetABC() {
    AttributeValue value = AttributeValue.builder().ss(Arrays.asList("a", "b", "c")).build();
    assertEquals(7306159961466191513L, AttributeValueHasher.hash(value));
  }

  @Test
  public void testStringSetABCDifferentOrder() {
    // Same set but different input order - should produce same hash
    AttributeValue value = AttributeValue.builder().ss(Arrays.asList("c", "a", "b")).build();
    assertEquals(7306159961466191513L, AttributeValueHasher.hash(value));
  }

  @Test
  public void testStringSetEmpty() {
    AttributeValue value = AttributeValue.builder().ss(Arrays.<String>asList()).build();
    assertEquals(1389283912212466035L, AttributeValueHasher.hash(value));
  }

  // ========== Number Set Tests ==========

  @Test
  public void testNumberSet123() {
    AttributeValue value = AttributeValue.builder().ns(Arrays.asList("1", "2", "3")).build();
    assertEquals(7671176432463372843L, AttributeValueHasher.hash(value));
  }

  @Test
  public void testNumberSet123DifferentOrder() {
    // Same set but different input order - should produce same hash
    AttributeValue value = AttributeValue.builder().ns(Arrays.asList("3", "1", "2")).build();
    assertEquals(7671176432463372843L, AttributeValueHasher.hash(value));
  }

  // ========== Binary Set Tests ==========

  @Test
  public void testBinarySet0102() {
    AttributeValue value =
        AttributeValue.builder()
            .bs(
                Arrays.asList(
                    SdkBytes.fromByteArray(new byte[] {0x01}),
                    SdkBytes.fromByteArray(new byte[] {0x02})))
            .build();
    assertEquals(1665953200922610785L, AttributeValueHasher.hash(value));
  }

  // ========== List Tests ==========

  @Test
  public void testListSaN1() {
    // L [S("a"), N("1")]
    AttributeValue value =
        AttributeValue.builder()
            .l(
                Arrays.asList(
                    AttributeValue.builder().s("a").build(),
                    AttributeValue.builder().n("1").build()))
            .build();
    assertEquals(2820707766025454319L, AttributeValueHasher.hash(value));
  }

  @Test
  public void testListEmpty() {
    AttributeValue value = AttributeValue.builder().l(Arrays.<AttributeValue>asList()).build();
    assertEquals(-9218108584195748763L, AttributeValueHasher.hash(value));
  }

  // ========== Map Tests ==========

  @Test
  public void testMapAgeNameJohn() {
    // M {"age": N("30"), "name": S("John")}
    Map<String, AttributeValue> map = new HashMap<>();
    map.put("age", AttributeValue.builder().n("30").build());
    map.put("name", AttributeValue.builder().s("John").build());
    AttributeValue value = AttributeValue.builder().m(map).build();
    assertEquals(-902430298826217654L, AttributeValueHasher.hash(value));
  }

  @Test
  public void testMapEmpty() {
    AttributeValue value =
        AttributeValue.builder().m(new HashMap<String, AttributeValue>()).build();
    assertEquals(3924702969362948632L, AttributeValueHasher.hash(value));
  }

  @Test
  public void testMapNestedList() {
    // M {"key": L([S("nested")])}
    Map<String, AttributeValue> map = new HashMap<>();
    map.put(
        "key",
        AttributeValue.builder()
            .l(Arrays.asList(AttributeValue.builder().s("nested").build()))
            .build());
    AttributeValue value = AttributeValue.builder().m(map).build();
    assertEquals(-5371960927743395604L, AttributeValueHasher.hash(value));
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

  // ========== Boundary Collision Prevention Tests ==========

  @Test
  public void testBoundaryStringSetABC() {
    // SS ["a", "bc"]
    AttributeValue value = AttributeValue.builder().ss(Arrays.asList("a", "bc")).build();
    assertEquals(1290520225009436005L, AttributeValueHasher.hash(value));
  }

  @Test
  public void testBoundaryStringSetABC2() {
    // SS ["ab", "c"]
    AttributeValue value = AttributeValue.builder().ss(Arrays.asList("ab", "c")).build();
    assertEquals(-5535761315402902992L, AttributeValueHasher.hash(value));
  }

  @Test
  public void testBoundaryListABC() {
    // L [S("a"), S("bc")]
    AttributeValue value =
        AttributeValue.builder()
            .l(
                Arrays.asList(
                    AttributeValue.builder().s("a").build(),
                    AttributeValue.builder().s("bc").build()))
            .build();
    assertEquals(-8510235581865967010L, AttributeValueHasher.hash(value));
  }

  @Test
  public void testBoundaryListABC2() {
    // L [S("ab"), S("c")]
    AttributeValue value =
        AttributeValue.builder()
            .l(
                Arrays.asList(
                    AttributeValue.builder().s("ab").build(),
                    AttributeValue.builder().s("c").build()))
            .build();
    assertEquals(1154309738056842165L, AttributeValueHasher.hash(value));
  }
}
