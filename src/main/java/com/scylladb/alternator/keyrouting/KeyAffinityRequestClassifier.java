package com.scylladb.alternator.keyrouting;

import java.util.Map;
import software.amazon.awssdk.core.SdkRequest;
import software.amazon.awssdk.services.dynamodb.model.AttributeAction;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

/**
 * Determines if a DynamoDB request qualifies for key route affinity.
 *
 * @author dmitry.kropachev
 * @since 1.0.7
 */
public final class KeyAffinityRequestClassifier {

  private KeyAffinityRequestClassifier() {}

  /**
   * Returns true if the request should use route affinity based on mode.
   *
   * @param mode the route affinity mode
   * @param request the DynamoDB request
   * @return true if route affinity should be applied
   */
  public static boolean shouldApply(KeyRouteAffinity mode, SdkRequest request) {
    if (mode == KeyRouteAffinity.NONE) {
      return false;
    }

    if (request instanceof UpdateItemRequest) {
      return shouldApplyUpdateItem(mode, (UpdateItemRequest) request);
    }
    if (request instanceof PutItemRequest) {
      return shouldApplyPutItem(mode, (PutItemRequest) request);
    }
    if (request instanceof DeleteItemRequest) {
      return shouldApplyDeleteItem(mode, (DeleteItemRequest) request);
    }
    // Note: BatchWriteItemRequest is intentionally not supported for key route affinity.
    // It can contain items for multiple tables with different partition keys, making it
    // impossible to route deterministically to a single node. These requests will use
    // the default round-robin routing.

    // Read operations and unsupported operations don't qualify
    return false;
  }

  private static boolean shouldApplyUpdateItem(KeyRouteAffinity mode, UpdateItemRequest request) {
    if (mode == KeyRouteAffinity.ANY_WRITE) {
      return true;
    }
    // RMW mode: trigger for conditional operations or read-before-write

    // UpdateExpression operations are LWT-based in Alternator
    if (request.updateExpression() != null && !request.updateExpression().isEmpty()) {
      return true;
    }
    if (request.conditionExpression() != null && !request.conditionExpression().isEmpty()) {
      return true;
    }
    if (request.hasExpected() && !request.expected().isEmpty()) {
      return true;
    }
    // Check return values that require reading the item's current state:
    // - ALL_OLD: returns entire item before update (requires read)
    // - UPDATED_OLD: returns only updated attributes before update (requires read)
    // - ALL_NEW: returns entire item after update, including non-updated attributes (requires read)
    // - UPDATED_NEW: returns only the updated attributes after update, which can be computed
    //   directly from the update expression without reading the current item state
    ReturnValue rv = request.returnValues();
    if (rv == ReturnValue.ALL_OLD || rv == ReturnValue.UPDATED_OLD || rv == ReturnValue.ALL_NEW) {
      return true;
    }
    // Check for ADD action or DELETE action with value in legacy AttributeUpdates
    if (request.hasAttributeUpdates()) {
      for (AttributeValueUpdate update : request.attributeUpdates().values()) {
        if (update.action() == AttributeAction.ADD) {
          return true;
        }
        if (update.action() == AttributeAction.DELETE && update.value() != null) {
          return true;
        }
      }
    }
    return false;
  }

  private static boolean shouldApplyPutItem(KeyRouteAffinity mode, PutItemRequest request) {
    if (mode == KeyRouteAffinity.ANY_WRITE) {
      return true;
    }
    // RMW mode: check for conditions
    if (request.conditionExpression() != null && !request.conditionExpression().isEmpty()) {
      return true;
    }
    if (request.hasExpected() && !request.expected().isEmpty()) {
      return true;
    }
    if (request.returnValues() != null && request.returnValues() != ReturnValue.NONE) {
      return true;
    }
    return false;
  }

  private static boolean shouldApplyDeleteItem(KeyRouteAffinity mode, DeleteItemRequest request) {
    if (mode == KeyRouteAffinity.ANY_WRITE) {
      return true;
    }
    // RMW mode: check for conditions
    if (request.conditionExpression() != null && !request.conditionExpression().isEmpty()) {
      return true;
    }
    if (request.hasExpected() && !request.expected().isEmpty()) {
      return true;
    }
    if (request.returnValues() != null && request.returnValues() != ReturnValue.NONE) {
      return true;
    }
    return false;
  }

  /**
   * Extracts the table name from a DynamoDB request.
   *
   * <p>Note: This method handles more request types than {@link #shouldApply} for completeness and
   * potential future use. For example, QueryRequest is handled here even though shouldApply returns
   * false for Query operations, because:
   *
   * <ul>
   *   <li>Query operations don't benefit from key affinity (they read from replicas)
   *   <li>Query may return results from multiple partition keys, making single-node routing
   *       meaningless
   *   <li>Having extractTableName support Query enables future extensions or diagnostic use
   * </ul>
   *
   * @param request the DynamoDB request
   * @return the table name, or null if not applicable
   */
  public static String extractTableName(SdkRequest request) {
    if (request instanceof UpdateItemRequest) {
      return ((UpdateItemRequest) request).tableName();
    }
    if (request instanceof PutItemRequest) {
      return ((PutItemRequest) request).tableName();
    }
    if (request instanceof DeleteItemRequest) {
      return ((DeleteItemRequest) request).tableName();
    }
    if (request instanceof GetItemRequest) {
      return ((GetItemRequest) request).tableName();
    }
    // Note: QueryRequest is included for completeness, though shouldApply returns false for Query.
    // See method Javadoc for rationale.
    if (request instanceof QueryRequest) {
      return ((QueryRequest) request).tableName();
    }
    // BatchWriteItem can contain items for multiple tables.
    // Key route affinity for BatchWriteItem is not implemented - would require either:
    // 1. Single-table batches only (limiting functionality)
    // 2. Splitting batches by partition key (complex, changes semantics)
    return null;
  }

  /**
   * Extracts the partition key value from a DynamoDB request.
   *
   * @param request the DynamoDB request
   * @param pkAttributeName the partition key attribute name
   * @return the partition key value, or null if not found
   */
  public static AttributeValue extractPartitionKey(SdkRequest request, String pkAttributeName) {
    Map<String, AttributeValue> key = null;

    if (request instanceof UpdateItemRequest) {
      key = ((UpdateItemRequest) request).key();
    } else if (request instanceof PutItemRequest) {
      key = ((PutItemRequest) request).item();
    } else if (request instanceof DeleteItemRequest) {
      key = ((DeleteItemRequest) request).key();
    } else if (request instanceof GetItemRequest) {
      key = ((GetItemRequest) request).key();
    }

    if (key != null && pkAttributeName != null) {
      return key.get(pkAttributeName);
    }
    return null;
  }
}
