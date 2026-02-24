package com.scylladb.alternator.demo;

import com.scylladb.alternator.AlternatorDynamoDbClient;
import com.scylladb.alternator.keyrouting.KeyRouteAffinity;
import com.scylladb.alternator.keyrouting.KeyRouteAffinityConfig;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

/**
 * Demo6: Conditional-write workload with concurrent writers and readers.
 *
 * <p>Mimics a production pattern where records are inserted with a condition expression (only if
 * newer) and then read back for a configurable duration.
 *
 * <p>Usage: mvn exec:java -Dexec.mainClass=com.scylladb.alternator.demo.Demo6 -Dexec.classpathScope=test
 * -Dexec.args="-e http://localhost:8043 -n 1000 -m 2"
 */
public class Demo6 {

  private static final String TABLE_NAME = "demo6_catalog";
  private static final String PK_ATTR = "id";
  private static final String LAST_UPDATED_ATTR = "last_updated_millis";
  private static final String DATA_ATTR = "data";
  private static final String CHUNK_PATH_ATTR = "chunk_path";

  private static final Map<String, String> EXPRESSION_ATTRIBUTE_NAMES =
      Map.of("#k", PK_ATTR, "#l", LAST_UPDATED_ATTR);
  private static final String CONDITION_EXPRESSION =
      "attribute_not_exists(#k) OR attribute_not_exists(#l) OR #l < :new_val";

  public static void main(String[] args) {
    Logger logger = Logger.getLogger("com.scylladb.alternator");
    ConsoleHandler handler = new ConsoleHandler();
    handler.setLevel(Level.INFO);
    logger.setLevel(Level.INFO);
    logger.addHandler(handler);
    logger.setUseParentHandlers(false);

    ArgumentParser parser =
        ArgumentParsers.newFor("Demo6")
            .build()
            .defaultHelp(true)
            .description("Conditional-write workload with concurrent writers and readers");

    try {
      parser
          .addArgument("-e", "--endpoint")
          .setDefault(new URI("http://localhost:8043"))
          .help("Alternator endpoint URI");
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
    parser.addArgument("-u", "--user").setDefault("none").help("Credentials username");
    parser.addArgument("-p", "--password").setDefault("none").help("Credentials password");
    parser
        .addArgument("-n", "--num-records")
        .type(Integer.class)
        .setDefault(1000)
        .help("Number of records to populate");
    parser
        .addArgument("-m", "--read-minutes")
        .type(Integer.class)
        .setDefault(2)
        .help("Minutes to run the read workload");
    parser
        .addArgument("-t", "--threads")
        .type(Integer.class)
        .setDefault(Runtime.getRuntime().availableProcessors() * 4)
        .help("Thread pool size for concurrent operations");

    Namespace ns = null;
    try {
      ns = parser.parseArgs(args);
    } catch (ArgumentParserException e) {
      parser.handleError(e);
      System.exit(1);
    }

    String endpoint = ns.getString("endpoint");
    String user = ns.getString("user");
    String pass = ns.getString("password");
    int numRecords = ns.getInt("num_records");
    int readMinutes = ns.getInt("read_minutes");
    int threadPoolSize = ns.getInt("threads");

    AwsCredentialsProvider credentials =
        StaticCredentialsProvider.create(AwsBasicCredentials.create(user, pass));

    KeyRouteAffinityConfig affinityConfig =
        KeyRouteAffinityConfig.builder()
            .withType(KeyRouteAffinity.RMW)
            .withPkInfo(TABLE_NAME, PK_ATTR)
            .build();

    DynamoDbClient client =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(URI.create(endpoint))
            .credentialsProvider(credentials)
            .withKeyRouteAffinity(affinityConfig)
            .withOptimizeHeaders(true)
            .build();

    ExecutorService executor = Executors.newFixedThreadPool(threadPoolSize);

    try {
      ensureTable(client);
      populateData(client, executor, numRecords);
      readWorkload(client, executor, numRecords, readMinutes);
    } finally {
      executor.shutdown();
      client.close();
    }
  }

  private static void ensureTable(DynamoDbClient client) {
    try {
      client.describeTable(b -> b.tableName(TABLE_NAME));
      System.out.println("Table " + TABLE_NAME + " already exists.");
      return;
    } catch (ResourceNotFoundException ignored) {
      // table does not exist, create it
    }

    System.out.println("Creating table " + TABLE_NAME + "...");
    client.createTable(
        CreateTableRequest.builder()
            .tableName(TABLE_NAME)
            .keySchema(
                KeySchemaElement.builder().attributeName(PK_ATTR).keyType(KeyType.HASH).build())
            .attributeDefinitions(
                AttributeDefinition.builder()
                    .attributeName(PK_ATTR)
                    .attributeType(ScalarAttributeType.S)
                    .build())
            .billingMode(BillingMode.PAY_PER_REQUEST)
            .build());
    System.out.println("Table " + TABLE_NAME + " created.");
  }

  private static void populateData(DynamoDbClient client, ExecutorService executor, int numRecords) {
    System.out.println("Populating " + numRecords + " records...");
    Instant start = Instant.now();
    AtomicLong written = new AtomicLong();
    AtomicLong skipped = new AtomicLong();

    List<CompletableFuture<Void>> futures = new ArrayList<>();
    for (int i = 0; i < numRecords; i++) {
      final String recordId = "record-" + i;
      final long timestamp = System.currentTimeMillis();
      final String chunkPath = "s3://bucket/chunk-" + (i % 100) + ".parquet";
      final String data = "payload-" + i;

      futures.add(
          CompletableFuture.runAsync(
              () -> {
                try {
                  conditionalPut(client, recordId, timestamp, chunkPath, data);
                  written.incrementAndGet();
                } catch (ConditionalCheckFailedException e) {
                  skipped.incrementAndGet();
                }
              },
              executor));
    }

    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    Duration elapsed = Duration.between(start, Instant.now());
    System.out.printf(
        "Populate done: %d written, %d skipped (condition), took %s%n",
        written.get(), skipped.get(), elapsed);
  }

  private static void conditionalPut(
      DynamoDbClient client, String id, long timestamp, String chunkPath, String data) {

    Map<String, AttributeValue> item = new HashMap<>();
    item.put(PK_ATTR, AttributeValue.fromS(id));
    item.put(LAST_UPDATED_ATTR, AttributeValue.fromN(Long.toString(timestamp)));
    item.put(CHUNK_PATH_ATTR, AttributeValue.fromS(chunkPath));
    item.put(DATA_ATTR, AttributeValue.fromS(data));

    Map<String, AttributeValue> expressionValues =
        Map.of(":new_val", AttributeValue.fromN(Long.toString(timestamp)));

    PutItemRequest request =
        PutItemRequest.builder()
            .tableName(TABLE_NAME)
            .item(item)
            .conditionExpression(CONDITION_EXPRESSION)
            .expressionAttributeNames(EXPRESSION_ATTRIBUTE_NAMES)
            .expressionAttributeValues(expressionValues)
            .build();

    client.putItem(request);
  }

  private static void readWorkload(
      DynamoDbClient client, ExecutorService executor, int numRecords, int readMinutes) {
    System.out.printf("Starting read workload for %d minutes across %d records...%n", readMinutes, numRecords);
    Instant deadline = Instant.now().plus(Duration.ofMinutes(readMinutes));
    AtomicLong reads = new AtomicLong();
    AtomicLong notFound = new AtomicLong();
    AtomicLong errors = new AtomicLong();

    int concurrency = Runtime.getRuntime().availableProcessors() * 4;
    List<CompletableFuture<Void>> workers = new ArrayList<>();

    for (int w = 0; w < concurrency; w++) {
      workers.add(
          CompletableFuture.runAsync(
              () -> {
                ThreadLocalRandom rng = ThreadLocalRandom.current();
                while (Instant.now().isBefore(deadline)) {
                  String recordId = "record-" + rng.nextInt(numRecords);
                  try {
                    GetItemResponse resp =
                        client.getItem(
                            GetItemRequest.builder()
                                .tableName(TABLE_NAME)
                                .key(Map.of(PK_ATTR, AttributeValue.fromS(recordId)))
                                .consistentRead(false)
                                .build());
                    if (resp.hasItem() && !resp.item().isEmpty()) {
                      reads.incrementAndGet();
                    } else {
                      notFound.incrementAndGet();
                    }
                  } catch (Exception e) {
                    errors.incrementAndGet();
                  }
                }
              },
              executor));
    }

    // Progress reporting
    while (Instant.now().isBefore(deadline)) {
      try {
        Thread.sleep(10_000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
      System.out.printf(
          "  [progress] reads=%d, not_found=%d, errors=%d%n",
          reads.get(), notFound.get(), errors.get());
    }

    CompletableFuture.allOf(workers.toArray(new CompletableFuture[0])).join();
    System.out.printf(
        "Read workload done: reads=%d, not_found=%d, errors=%d%n",
        reads.get(), notFound.get(), errors.get());
  }
}
