package com.scylladb.alternator.keyrouting;

import static org.junit.Assert.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import software.amazon.awssdk.services.dynamodb.model.*;

/**
 * Unit tests for KeyAffinityRequestClassifier.
 *
 * @author dmitry.kropachev
 */
public class KeyAffinityRequestClassifierTest {

  // ========== shouldApply tests for NONE mode ==========

  @Test
  public void testNoneModeNeverApplies() {
    UpdateItemRequest request =
        UpdateItemRequest.builder()
            .tableName("test")
            .key(createKey("pk", "value"))
            .updateExpression("SET x = :v")
            .build();

    assertFalse(KeyAffinityRequestClassifier.shouldApply(KeyRouteAffinity.NONE, request));
  }

  // ========== shouldApply tests for RMW mode - UpdateItem ==========

  @Test
  public void testRmwModeUpdateItemWithUpdateExpression() {
    // UpdateItem with updateExpression SHOULD trigger RMW because UpdateExpression operations
    // are LWT-based in Alternator
    UpdateItemRequest request =
        UpdateItemRequest.builder()
            .tableName("test")
            .key(createKey("pk", "value"))
            .updateExpression("SET x = :v")
            .build();

    assertTrue(KeyAffinityRequestClassifier.shouldApply(KeyRouteAffinity.RMW, request));
  }

  @Test
  public void testRmwModeUpdateItemWithConditionExpression() {
    UpdateItemRequest request =
        UpdateItemRequest.builder()
            .tableName("test")
            .key(createKey("pk", "value"))
            .conditionExpression("attribute_exists(x)")
            .build();

    assertTrue(KeyAffinityRequestClassifier.shouldApply(KeyRouteAffinity.RMW, request));
  }

  @Test
  public void testRmwModeUpdateItemWithExpected() {
    UpdateItemRequest request =
        UpdateItemRequest.builder()
            .tableName("test")
            .key(createKey("pk", "value"))
            .expected(
                Collections.singletonMap(
                    "x", ExpectedAttributeValue.builder().exists(true).build()))
            .build();

    assertTrue(KeyAffinityRequestClassifier.shouldApply(KeyRouteAffinity.RMW, request));
  }

  @Test
  public void testRmwModeUpdateItemWithReturnValues() {
    UpdateItemRequest request =
        UpdateItemRequest.builder()
            .tableName("test")
            .key(createKey("pk", "value"))
            .returnValues(ReturnValue.ALL_OLD)
            .build();

    assertTrue(KeyAffinityRequestClassifier.shouldApply(KeyRouteAffinity.RMW, request));
  }

  @Test
  public void testRmwModeUpdateItemWithReturnValuesNone() {
    UpdateItemRequest request =
        UpdateItemRequest.builder()
            .tableName("test")
            .key(createKey("pk", "value"))
            .returnValues(ReturnValue.NONE)
            .build();

    assertFalse(KeyAffinityRequestClassifier.shouldApply(KeyRouteAffinity.RMW, request));
  }

  @Test
  public void testRmwModeUpdateItemWithReturnValuesUpdatedOld() {
    UpdateItemRequest request =
        UpdateItemRequest.builder()
            .tableName("test")
            .key(createKey("pk", "value"))
            .returnValues(ReturnValue.UPDATED_OLD)
            .build();

    assertTrue(KeyAffinityRequestClassifier.shouldApply(KeyRouteAffinity.RMW, request));
  }

  @Test
  public void testRmwModeUpdateItemWithReturnValuesAllNew() {
    UpdateItemRequest request =
        UpdateItemRequest.builder()
            .tableName("test")
            .key(createKey("pk", "value"))
            .returnValues(ReturnValue.ALL_NEW)
            .build();

    assertTrue(KeyAffinityRequestClassifier.shouldApply(KeyRouteAffinity.RMW, request));
  }

  @Test
  public void testRmwModeUpdateItemWithReturnValuesUpdatedNew() {
    // UPDATED_NEW does NOT trigger RMW - can be computed from update alone without full read
    UpdateItemRequest request =
        UpdateItemRequest.builder()
            .tableName("test")
            .key(createKey("pk", "value"))
            .returnValues(ReturnValue.UPDATED_NEW)
            .build();

    assertFalse(KeyAffinityRequestClassifier.shouldApply(KeyRouteAffinity.RMW, request));
  }

  @Test
  public void testRmwModeUpdateItemWithAddAction() {
    // ADD action requires reading current value for atomic increment
    Map<String, AttributeValueUpdate> updates = new HashMap<>();
    updates.put(
        "counter",
        AttributeValueUpdate.builder()
            .action(AttributeAction.ADD)
            .value(AttributeValue.builder().n("1").build())
            .build());

    UpdateItemRequest request =
        UpdateItemRequest.builder()
            .tableName("test")
            .key(createKey("pk", "value"))
            .attributeUpdates(updates)
            .build();

    assertTrue(KeyAffinityRequestClassifier.shouldApply(KeyRouteAffinity.RMW, request));
  }

  @Test
  public void testRmwModeUpdateItemWithDeleteActionWithValue() {
    // DELETE action with value requires checking set membership
    Map<String, AttributeValueUpdate> updates = new HashMap<>();
    updates.put(
        "tags",
        AttributeValueUpdate.builder()
            .action(AttributeAction.DELETE)
            .value(AttributeValue.builder().ss("tag1").build())
            .build());

    UpdateItemRequest request =
        UpdateItemRequest.builder()
            .tableName("test")
            .key(createKey("pk", "value"))
            .attributeUpdates(updates)
            .build();

    assertTrue(KeyAffinityRequestClassifier.shouldApply(KeyRouteAffinity.RMW, request));
  }

  @Test
  public void testRmwModeUpdateItemWithDeleteActionWithoutValue() {
    // DELETE action without value (deleting entire attribute) does NOT need to read current value
    Map<String, AttributeValueUpdate> updates = new HashMap<>();
    updates.put("tags", AttributeValueUpdate.builder().action(AttributeAction.DELETE).build());

    UpdateItemRequest request =
        UpdateItemRequest.builder()
            .tableName("test")
            .key(createKey("pk", "value"))
            .attributeUpdates(updates)
            .build();

    assertFalse(KeyAffinityRequestClassifier.shouldApply(KeyRouteAffinity.RMW, request));
  }

  @Test
  public void testRmwModeUpdateItemWithPutAction() {
    // PUT action (simple attribute set) does NOT trigger RMW
    Map<String, AttributeValueUpdate> updates = new HashMap<>();
    updates.put(
        "name",
        AttributeValueUpdate.builder()
            .action(AttributeAction.PUT)
            .value(AttributeValue.builder().s("new name").build())
            .build());

    UpdateItemRequest request =
        UpdateItemRequest.builder()
            .tableName("test")
            .key(createKey("pk", "value"))
            .attributeUpdates(updates)
            .build();

    assertFalse(KeyAffinityRequestClassifier.shouldApply(KeyRouteAffinity.RMW, request));
  }

  @Test
  public void testRmwModeUpdateItemSimple() {
    // UpdateItem without any conditions or return values
    UpdateItemRequest request =
        UpdateItemRequest.builder().tableName("test").key(createKey("pk", "value")).build();

    assertFalse(KeyAffinityRequestClassifier.shouldApply(KeyRouteAffinity.RMW, request));
  }

  // ========== shouldApply tests for RMW mode - PutItem ==========

  @Test
  public void testRmwModePutItemWithConditionExpression() {
    PutItemRequest request =
        PutItemRequest.builder()
            .tableName("test")
            .item(createItem("pk", "value"))
            .conditionExpression("attribute_not_exists(pk)")
            .build();

    assertTrue(KeyAffinityRequestClassifier.shouldApply(KeyRouteAffinity.RMW, request));
  }

  @Test
  public void testRmwModePutItemWithExpected() {
    PutItemRequest request =
        PutItemRequest.builder()
            .tableName("test")
            .item(createItem("pk", "value"))
            .expected(
                Collections.singletonMap(
                    "pk", ExpectedAttributeValue.builder().exists(false).build()))
            .build();

    assertTrue(KeyAffinityRequestClassifier.shouldApply(KeyRouteAffinity.RMW, request));
  }

  @Test
  public void testRmwModePutItemWithReturnValues() {
    PutItemRequest request =
        PutItemRequest.builder()
            .tableName("test")
            .item(createItem("pk", "value"))
            .returnValues(ReturnValue.ALL_OLD)
            .build();

    assertTrue(KeyAffinityRequestClassifier.shouldApply(KeyRouteAffinity.RMW, request));
  }

  @Test
  public void testRmwModePutItemSimple() {
    PutItemRequest request =
        PutItemRequest.builder().tableName("test").item(createItem("pk", "value")).build();

    assertFalse(KeyAffinityRequestClassifier.shouldApply(KeyRouteAffinity.RMW, request));
  }

  // ========== shouldApply tests for RMW mode - DeleteItem ==========

  @Test
  public void testRmwModeDeleteItemWithConditionExpression() {
    DeleteItemRequest request =
        DeleteItemRequest.builder()
            .tableName("test")
            .key(createKey("pk", "value"))
            .conditionExpression("attribute_exists(pk)")
            .build();

    assertTrue(KeyAffinityRequestClassifier.shouldApply(KeyRouteAffinity.RMW, request));
  }

  @Test
  public void testRmwModeDeleteItemWithExpected() {
    DeleteItemRequest request =
        DeleteItemRequest.builder()
            .tableName("test")
            .key(createKey("pk", "value"))
            .expected(
                Collections.singletonMap(
                    "pk", ExpectedAttributeValue.builder().exists(true).build()))
            .build();

    assertTrue(KeyAffinityRequestClassifier.shouldApply(KeyRouteAffinity.RMW, request));
  }

  @Test
  public void testRmwModeDeleteItemWithReturnValues() {
    DeleteItemRequest request =
        DeleteItemRequest.builder()
            .tableName("test")
            .key(createKey("pk", "value"))
            .returnValues(ReturnValue.ALL_OLD)
            .build();

    assertTrue(KeyAffinityRequestClassifier.shouldApply(KeyRouteAffinity.RMW, request));
  }

  @Test
  public void testRmwModeDeleteItemSimple() {
    DeleteItemRequest request =
        DeleteItemRequest.builder().tableName("test").key(createKey("pk", "value")).build();

    assertFalse(KeyAffinityRequestClassifier.shouldApply(KeyRouteAffinity.RMW, request));
  }

  // ========== shouldApply tests for RMW mode - BatchWriteItem ==========

  @Test
  public void testRmwModeBatchWriteItem() {
    // BatchWriteItem is not supported for key route affinity because it can contain
    // items for multiple tables with different partition keys
    BatchWriteItemRequest request = BatchWriteItemRequest.builder().build();
    assertFalse(KeyAffinityRequestClassifier.shouldApply(KeyRouteAffinity.RMW, request));
  }

  // ========== shouldApply tests for ANY_WRITE mode ==========

  @Test
  public void testAnyWriteModeUpdateItem() {
    UpdateItemRequest request =
        UpdateItemRequest.builder().tableName("test").key(createKey("pk", "value")).build();

    assertTrue(KeyAffinityRequestClassifier.shouldApply(KeyRouteAffinity.ANY_WRITE, request));
  }

  @Test
  public void testAnyWriteModePutItem() {
    PutItemRequest request =
        PutItemRequest.builder().tableName("test").item(createItem("pk", "value")).build();

    assertTrue(KeyAffinityRequestClassifier.shouldApply(KeyRouteAffinity.ANY_WRITE, request));
  }

  @Test
  public void testAnyWriteModeDeleteItem() {
    DeleteItemRequest request =
        DeleteItemRequest.builder().tableName("test").key(createKey("pk", "value")).build();

    assertTrue(KeyAffinityRequestClassifier.shouldApply(KeyRouteAffinity.ANY_WRITE, request));
  }

  @Test
  public void testAnyWriteModeBatchWriteItem() {
    // BatchWriteItem is not supported for key route affinity because it can contain
    // items for multiple tables with different partition keys
    BatchWriteItemRequest request = BatchWriteItemRequest.builder().build();
    assertFalse(KeyAffinityRequestClassifier.shouldApply(KeyRouteAffinity.ANY_WRITE, request));
  }

  // ========== shouldApply tests for read operations ==========

  @Test
  public void testRmwModeDoesNotApplyToGetItem() {
    GetItemRequest request =
        GetItemRequest.builder().tableName("test").key(createKey("pk", "value")).build();

    assertFalse(KeyAffinityRequestClassifier.shouldApply(KeyRouteAffinity.RMW, request));
  }

  @Test
  public void testAnyWriteModeDoesNotApplyToGetItem() {
    GetItemRequest request =
        GetItemRequest.builder().tableName("test").key(createKey("pk", "value")).build();

    assertFalse(KeyAffinityRequestClassifier.shouldApply(KeyRouteAffinity.ANY_WRITE, request));
  }

  @Test
  public void testDoesNotApplyToQuery() {
    QueryRequest request = QueryRequest.builder().tableName("test").build();

    assertFalse(KeyAffinityRequestClassifier.shouldApply(KeyRouteAffinity.RMW, request));
    assertFalse(KeyAffinityRequestClassifier.shouldApply(KeyRouteAffinity.ANY_WRITE, request));
  }

  // ========== extractTableName tests ==========

  @Test
  public void testExtractTableNameFromUpdateItem() {
    UpdateItemRequest request = UpdateItemRequest.builder().tableName("users").build();
    assertEquals("users", KeyAffinityRequestClassifier.extractTableName(request));
  }

  @Test
  public void testExtractTableNameFromPutItem() {
    PutItemRequest request = PutItemRequest.builder().tableName("orders").build();
    assertEquals("orders", KeyAffinityRequestClassifier.extractTableName(request));
  }

  @Test
  public void testExtractTableNameFromDeleteItem() {
    DeleteItemRequest request = DeleteItemRequest.builder().tableName("sessions").build();
    assertEquals("sessions", KeyAffinityRequestClassifier.extractTableName(request));
  }

  @Test
  public void testExtractTableNameFromGetItem() {
    GetItemRequest request = GetItemRequest.builder().tableName("products").build();
    assertEquals("products", KeyAffinityRequestClassifier.extractTableName(request));
  }

  @Test
  public void testExtractTableNameFromQuery() {
    QueryRequest request = QueryRequest.builder().tableName("events").build();
    assertEquals("events", KeyAffinityRequestClassifier.extractTableName(request));
  }

  @Test
  public void testExtractTableNameFromBatchWriteItem() {
    BatchWriteItemRequest request = BatchWriteItemRequest.builder().build();
    assertNull(KeyAffinityRequestClassifier.extractTableName(request));
  }

  // ========== extractPartitionKey tests ==========

  @Test
  public void testExtractPartitionKeyFromUpdateItem() {
    Map<String, AttributeValue> key = createKey("user_id", "u123");
    UpdateItemRequest request = UpdateItemRequest.builder().tableName("users").key(key).build();

    AttributeValue pk = KeyAffinityRequestClassifier.extractPartitionKey(request, "user_id");
    assertNotNull(pk);
    assertEquals("u123", pk.s());
  }

  @Test
  public void testExtractPartitionKeyFromPutItem() {
    Map<String, AttributeValue> item = createItem("order_id", "o456");
    PutItemRequest request = PutItemRequest.builder().tableName("orders").item(item).build();

    AttributeValue pk = KeyAffinityRequestClassifier.extractPartitionKey(request, "order_id");
    assertNotNull(pk);
    assertEquals("o456", pk.s());
  }

  @Test
  public void testExtractPartitionKeyFromDeleteItem() {
    Map<String, AttributeValue> key = createKey("session_id", "s789");
    DeleteItemRequest request = DeleteItemRequest.builder().tableName("sessions").key(key).build();

    AttributeValue pk = KeyAffinityRequestClassifier.extractPartitionKey(request, "session_id");
    assertNotNull(pk);
    assertEquals("s789", pk.s());
  }

  @Test
  public void testExtractPartitionKeyFromGetItem() {
    Map<String, AttributeValue> key = createKey("product_id", "p012");
    GetItemRequest request = GetItemRequest.builder().tableName("products").key(key).build();

    AttributeValue pk = KeyAffinityRequestClassifier.extractPartitionKey(request, "product_id");
    assertNotNull(pk);
    assertEquals("p012", pk.s());
  }

  @Test
  public void testExtractPartitionKeyWithWrongKeyName() {
    Map<String, AttributeValue> key = createKey("user_id", "u123");
    UpdateItemRequest request = UpdateItemRequest.builder().tableName("users").key(key).build();

    AttributeValue pk = KeyAffinityRequestClassifier.extractPartitionKey(request, "wrong_key");
    assertNull(pk);
  }

  @Test
  public void testExtractPartitionKeyWithNullKeyName() {
    Map<String, AttributeValue> key = createKey("user_id", "u123");
    UpdateItemRequest request = UpdateItemRequest.builder().tableName("users").key(key).build();

    AttributeValue pk = KeyAffinityRequestClassifier.extractPartitionKey(request, null);
    assertNull(pk);
  }

  // ========== Helper methods ==========

  private Map<String, AttributeValue> createKey(String keyName, String keyValue) {
    Map<String, AttributeValue> key = new HashMap<>();
    key.put(keyName, AttributeValue.builder().s(keyValue).build());
    return key;
  }

  private Map<String, AttributeValue> createItem(String keyName, String keyValue) {
    Map<String, AttributeValue> item = new HashMap<>();
    item.put(keyName, AttributeValue.builder().s(keyValue).build());
    item.put("data", AttributeValue.builder().s("some data").build());
    return item;
  }
}
