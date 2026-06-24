package com.scylladb.alternator.keyrouting;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import com.scylladb.alternator.internal.AlternatorLiveNodes;
import com.scylladb.alternator.internal.LazyQueryPlan;
import com.scylladb.alternator.keyrouting.KeyAffinityRequestClassifier.BatchWriteRoutingTarget;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteRequest;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

/** Cross-language BatchWriteItem route-affinity vectors shared with Rust and Go. */
public class BatchWriteItemKeyRouteAffinityCrossLanguageTest {

  @Test
  public void testBatchWriteItemKeyRouteAffinityCrossLanguageVectors() throws URISyntaxException {
    for (BatchWriteVector vector : vectors()) {
      List<BatchWriteRoutingTarget> targets =
          KeyAffinityRequestClassifier.extractBatchWriteRoutingTargets(vector.request);
      assertFalse(vector.name, targets.isEmpty());

      BatchWriteRoutingTarget target = targets.get(0);
      assertEquals(vector.name, vector.tableName, target.tableName());
      assertEquals(vector.name, vector.operation, target.operation());
      assertEquals(vector.name, vector.canonical, target.canonicalAttributes());

      String pkName = vector.pkInfo.get(target.tableName());
      AttributeValue pkValue = target.partitionKeyValue(pkName);
      assertEquals(vector.name, vector.pkLabel, attributeLabel(pkValue));

      long hash = batchWriteHash(vector.request, vector.pkInfo);
      assertEquals(vector.name, vector.hashSigned, hash);
      assertEquals(vector.name, vector.hashUnsigned, Long.toUnsignedString(hash));
      assertEquals(vector.name, vector.firstSixNodes, nodeSequence(hash, 6));
    }
  }

  private static List<BatchWriteVector> vectors() {
    return Arrays.asList(
        new BatchWriteVector(
            "same_table_write_order",
            batchWrite(
                table("orders", put(attrs("pk", s("order456"))), put(attrs("pk", s("order123"))))),
            pkInfo("orders", "pk"),
            "orders",
            "PutRequest",
            "S:order123",
            "{\"pk\":{\"S\":\"order123\"}}",
            -2126891002421145093L,
            "16319853071288406523",
            Arrays.asList("node9", "node2", "node10", "node8", "node7", "node5")),
        new BatchWriteVector(
            "multi_table_order",
            batchWrite(
                table("sessions", delete(attrs("pk", s("session123")))),
                table(
                    "orders",
                    put(attrs("data", s("value"), "pk", s("order456"))),
                    put(attrs("pk", s("order123"), "data", s("value"))))),
            pkInfo("orders", "pk", "sessions", "pk"),
            "orders",
            "PutRequest",
            "S:order123",
            "{\"data\":{\"S\":\"value\"},\"pk\":{\"S\":\"order123\"}}",
            -2126891002421145093L,
            "16319853071288406523",
            Arrays.asList("node9", "node2", "node10", "node8", "node7", "node5")),
        new BatchWriteVector(
            "delete_put_same_attributes",
            batchWrite(
                table("orders", put(attrs("pk", s("same"))), delete(attrs("pk", s("same"))))),
            pkInfo("orders", "pk"),
            "orders",
            "DeleteRequest",
            "S:same",
            "{\"pk\":{\"S\":\"same\"}}",
            -4879317772220196571L,
            "13567426301489355045",
            Arrays.asList("node1", "node3", "node10", "node7", "node4", "node5")),
        new BatchWriteVector(
            "number_partition_key",
            batchWrite(table("accounts", put(attrs("pk", n("7"))), put(attrs("pk", n("42"))))),
            pkInfo("accounts", "pk"),
            "accounts",
            "PutRequest",
            "N:42",
            "{\"pk\":{\"N\":\"42\"}}",
            -5061732451827723051L,
            "13385011621881828565",
            Arrays.asList("node3", "node7", "node1", "node10", "node2", "node5")),
        new BatchWriteVector(
            "binary_partition_key",
            batchWrite(
                table(
                    "blobs",
                    put(attrs("pk", b(0x01, 0x02, 0x03))),
                    delete(attrs("pk", b(0x00, 0xff))))),
            pkInfo("blobs", "pk"),
            "blobs",
            "DeleteRequest",
            "B:00ff",
            "{\"pk\":{\"B\":{\"__bytes__\":\"00ff\"}}}",
            -4376945693382523102L,
            "14069798380327028514",
            Arrays.asList("node7", "node2", "node5", "node1", "node6", "node9")));
  }

  @SafeVarargs
  private static BatchWriteItemRequest batchWrite(Map.Entry<String, List<WriteRequest>>... tables) {
    Map<String, List<WriteRequest>> requestItems = new LinkedHashMap<>();
    for (Map.Entry<String, List<WriteRequest>> table : tables) {
      requestItems.put(table.getKey(), table.getValue());
    }
    return BatchWriteItemRequest.builder().requestItems(requestItems).build();
  }

  private static Map.Entry<String, List<WriteRequest>> table(
      String tableName, WriteRequest... writes) {
    return new AbstractMap.SimpleImmutableEntry<>(tableName, Arrays.asList(writes));
  }

  private static WriteRequest put(Map<String, AttributeValue> item) {
    return WriteRequest.builder().putRequest(PutRequest.builder().item(item).build()).build();
  }

  private static WriteRequest delete(Map<String, AttributeValue> key) {
    return WriteRequest.builder().deleteRequest(DeleteRequest.builder().key(key).build()).build();
  }

  private static Map<String, AttributeValue> attrs(Object... keyValues) {
    Map<String, AttributeValue> attrs = new LinkedHashMap<>();
    for (int i = 0; i < keyValues.length; i += 2) {
      attrs.put((String) keyValues[i], (AttributeValue) keyValues[i + 1]);
    }
    return attrs;
  }

  private static Map<String, String> pkInfo(String tableName, String pkName, String... rest) {
    Map<String, String> pkInfo = new LinkedHashMap<>();
    pkInfo.put(tableName, pkName);
    for (int i = 0; i < rest.length; i += 2) {
      pkInfo.put(rest[i], rest[i + 1]);
    }
    return pkInfo;
  }

  private static AttributeValue s(String value) {
    return AttributeValue.builder().s(value).build();
  }

  private static AttributeValue n(String value) {
    return AttributeValue.builder().n(value).build();
  }

  private static AttributeValue b(int... values) {
    byte[] bytes = new byte[values.length];
    for (int i = 0; i < values.length; i++) {
      bytes[i] = (byte) values[i];
    }
    return AttributeValue.builder().b(SdkBytes.fromByteArray(bytes)).build();
  }

  private static long batchWriteHash(BatchWriteItemRequest request, Map<String, String> pkInfo) {
    for (BatchWriteRoutingTarget target :
        KeyAffinityRequestClassifier.extractBatchWriteRoutingTargets(request)) {
      String pkName = pkInfo.get(target.tableName());
      if (pkName == null) {
        throw new AssertionError("missing partition key info for table " + target.tableName());
      }

      AttributeValue pkValue = target.partitionKeyValue(pkName);
      if (pkValue == null) {
        continue;
      }

      try {
        return AttributeValueHasher.hash(pkValue);
      } catch (IllegalArgumentException e) {
        // Try the next deterministic candidate.
      }
    }
    throw new AssertionError("batch write request does not have a usable partition key value");
  }

  private static String attributeLabel(AttributeValue value) {
    if (value.s() != null) {
      return "S:" + value.s();
    }
    if (value.n() != null) {
      return "N:" + value.n();
    }
    if (value.b() != null) {
      return "B:" + hex(value.b().asByteArray());
    }
    return String.valueOf(value);
  }

  private static List<String> nodeSequence(long seed, int count) throws URISyntaxException {
    List<URI> nodes = new ArrayList<>();
    for (int i = 1; i <= 10; i++) {
      nodes.add(new URI("http", null, "node" + i + ".example.com", 8000, null, null, null));
    }

    LazyQueryPlan plan =
        new LazyQueryPlan(new AlternatorLiveNodes(nodes, "http", 8000, "", ""), seed);
    List<String> result = new ArrayList<>();
    for (int i = 0; i < count && plan.hasNext(); i++) {
      result.add(nodeShortName(plan.next()));
    }
    return result;
  }

  private static String nodeShortName(URI uri) {
    String host = uri.getHost();
    return host.substring(0, host.indexOf(".example.com"));
  }

  private static String hex(byte[] bytes) {
    final char[] hex = "0123456789abcdef".toCharArray();
    StringBuilder builder = new StringBuilder(bytes.length * 2);
    for (byte value : bytes) {
      builder.append(hex[(value >> 4) & 0x0f]);
      builder.append(hex[value & 0x0f]);
    }
    return builder.toString();
  }

  private static final class BatchWriteVector {
    private final String name;
    private final BatchWriteItemRequest request;
    private final Map<String, String> pkInfo;
    private final String tableName;
    private final String operation;
    private final String pkLabel;
    private final String canonical;
    private final long hashSigned;
    private final String hashUnsigned;
    private final List<String> firstSixNodes;

    private BatchWriteVector(
        String name,
        BatchWriteItemRequest request,
        Map<String, String> pkInfo,
        String tableName,
        String operation,
        String pkLabel,
        String canonical,
        long hashSigned,
        String hashUnsigned,
        List<String> firstSixNodes) {
      this.name = name;
      this.request = request;
      this.pkInfo = pkInfo;
      this.tableName = tableName;
      this.operation = operation;
      this.pkLabel = pkLabel;
      this.canonical = canonical;
      this.hashSigned = hashSigned;
      this.hashUnsigned = hashUnsigned;
      this.firstSixNodes = firstSixNodes;
    }
  }
}
