package com.scylladb.alternator.demo;

import static java.util.concurrent.Executors.newFixedThreadPool;

import com.scylladb.alternator.AlternatorDynamoDbAsyncClient;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
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
import software.amazon.awssdk.core.client.config.ClientAsyncConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedAsyncClientOption;
import software.amazon.awssdk.core.internal.http.loader.DefaultSdkAsyncHttpClientBuilder;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbAsyncTable;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedAsyncClient;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.http.SdkHttpConfigurationOption;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.utils.AttributeMap;

/**
 * Demo application showing how to use the DynamoDB Enhanced Async Client with Alternator load
 * balancing.
 *
 * <p>The enhanced async client provides a high-level, object-mapping interface with asynchronous
 * operations returning CompletableFuture. Since it wraps the standard DynamoDbAsyncClient, load
 * balancing works transparently.
 */
public class Demo5 {

  /** Simple model class for demonstrating the enhanced client. */
  @DynamoDbBean
  public static class Product {
    private String sku;
    private String name;
    private Double price;

    public Product() {}

    public Product(String sku, String name, Double price) {
      this.sku = sku;
      this.name = name;
      this.price = price;
    }

    @DynamoDbPartitionKey
    public String getSku() {
      return sku;
    }

    public void setSku(String sku) {
      this.sku = sku;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public Double getPrice() {
      return price;
    }

    public void setPrice(Double price) {
      this.price = price;
    }

    @Override
    public String toString() {
      return "Product{sku='" + sku + "', name='" + name + "', price=" + price + "}";
    }
  }

  public static void main(String[] args) throws MalformedURLException {
    ArgumentParser parser =
        ArgumentParsers.newFor("Demo5")
            .build()
            .defaultHelp(true)
            .description(
                "Example of DynamoDB Enhanced Async Client with Alternator load balancing");

    parser
        .addArgument("-e", "--endpoint")
        .setDefault(new URL("http://localhost:8043"))
        .help("DynamoDB/Alternator endpoint");
    parser.addArgument("-u", "--user").setDefault("none").help("Credentials username");
    parser.addArgument("-p", "--password").setDefault("none").help("Credentials password");
    parser.addArgument("-r", "--region").setDefault("us-east-1").help("AWS region");
    parser
        .addArgument("--threads")
        .type(Integer.class)
        .setDefault(Runtime.getRuntime().availableProcessors() * 2)
        .help("Max worker threads");
    parser
        .addArgument("--trust-ssl")
        .type(Boolean.class)
        .setDefault(false)
        .help("Trust all certificates");
    parser.addArgument("--datacenter").type(String.class).setDefault("").help("Target datacenter");
    parser.addArgument("--rack").type(String.class).setDefault("").help("Target rack");
    parser
        .addArgument("--table")
        .type(String.class)
        .setDefault("demo5_products")
        .help("Table name to use");

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
    int threads = ns.getInt("threads");
    Region region = Region.of(ns.getString("region"));
    Boolean trustSSL = ns.getBoolean("trust-ssl");
    String datacenter = ns.getString("datacenter");
    String rack = ns.getString("rack");
    String tableName = ns.getString("table");

    Logger logger = Logger.getLogger("com.scylladb.alternator");
    ConsoleHandler handler = new ConsoleHandler();
    handler.setLevel(Level.FINEST);
    logger.setLevel(Level.FINEST);
    logger.addHandler(handler);
    logger.setUseParentHandlers(false);

    ExecutorService executor = newFixedThreadPool(threads);
    ClientAsyncConfiguration cas =
        ClientAsyncConfiguration.builder()
            .advancedOption(SdkAdvancedAsyncClientOption.FUTURE_COMPLETION_EXECUTOR, executor)
            .build();

    // 1. Build the async client using AlternatorDynamoDbAsyncClient
    AlternatorDynamoDbAsyncClient.AlternatorDynamoDbAsyncClientBuilder b =
        AlternatorDynamoDbAsyncClient.builder().region(region).asyncConfiguration(cas);

    if (datacenter != null && !datacenter.isEmpty()) {
      b.withDatacenter(datacenter);
    }
    if (rack != null && !rack.isEmpty()) {
      b.withRack(rack);
    }

    if (endpoint != null) {
      URI uri = URI.create(endpoint);
      b.endpointOverride(uri);

      if (trustSSL != null && trustSSL.booleanValue()) {
        SdkAsyncHttpClient http =
            new DefaultSdkAsyncHttpClientBuilder()
                .buildWithDefaults(
                    AttributeMap.builder()
                        .put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, true)
                        .build());
        b.httpClient(http);
      }
    }

    if (user != null) {
      AwsCredentialsProvider cp =
          StaticCredentialsProvider.create(AwsBasicCredentials.create(user, pass));
      b.credentialsProvider(cp);
    }

    DynamoDbAsyncClient dynamoDBClient = b.build();

    // 2. Wrap with DynamoDbEnhancedAsyncClient - load balancing works transparently
    DynamoDbEnhancedAsyncClient enhancedClient =
        DynamoDbEnhancedAsyncClient.builder().dynamoDbClient(dynamoDBClient).build();

    // 3. Get table reference with schema
    DynamoDbAsyncTable<Product> productTable =
        enhancedClient.table(tableName, TableSchema.fromBean(Product.class));

    // 4. Create table if it doesn't exist
    System.out.println("Checking if table exists...");
    productTable
        .describeTable()
        .exceptionally(
            ex -> {
              if (ex.getCause() instanceof ResourceNotFoundException) {
                System.out.println("Creating table " + tableName);
                productTable.createTable().join();
                System.out.println("Table created");
              }
              return null;
            })
        .join();

    // 5. Put some items asynchronously - each request goes to a different node
    System.out.println("\nPutting items asynchronously (watch logs for load balancing):");
    List<CompletableFuture<Void>> putFutures = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      Product product = new Product("SKU-" + i, "Product " + i, 9.99 + i);
      CompletableFuture<Void> future =
          productTable.putItem(product).thenAccept(v -> System.out.println("  Put: " + product));
      putFutures.add(future);
    }
    CompletableFuture.allOf(putFutures.toArray(new CompletableFuture[0])).join();

    // 6. Get items back asynchronously - each request goes to a different node
    System.out.println("\nGetting items asynchronously:");
    List<CompletableFuture<Void>> getFutures = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      final int index = i;
      CompletableFuture<Void> future =
          productTable
              .getItem(Key.builder().partitionValue("SKU-" + index).build())
              .thenAccept(product -> System.out.println("  Got: " + product));
      getFutures.add(future);
    }
    CompletableFuture.allOf(getFutures.toArray(new CompletableFuture[0])).join();

    // 7. Scan table asynchronously
    System.out.println("\nScanning table asynchronously:");
    productTable
        .scan()
        .items()
        .subscribe(product -> System.out.println("  Scanned: " + product))
        .join();

    System.out.println("\nDone");
    System.exit(0);
  }
}
