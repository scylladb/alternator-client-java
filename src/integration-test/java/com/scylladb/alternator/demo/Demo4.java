package com.scylladb.alternator.demo;

import com.scylladb.alternator.AlternatorDynamoDbClient;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.cert.X509Certificate;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpConfigurationOption;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.utils.AttributeMap;

/**
 * Demo application showing how to use the DynamoDB Enhanced Client with Alternator load balancing.
 *
 * <p>The enhanced client provides a high-level, object-mapping interface that simplifies working
 * with DynamoDB tables by mapping Java classes to table items. Since it wraps the standard
 * DynamoDbClient, load balancing works transparently.
 */
public class Demo4 {

  /** Simple model class for demonstrating the enhanced client. */
  @DynamoDbBean
  public static class Customer {
    private String id;
    private String name;
    private String email;

    public Customer() {}

    public Customer(String id, String name, String email) {
      this.id = id;
      this.name = name;
      this.email = email;
    }

    @DynamoDbPartitionKey
    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public String getEmail() {
      return email;
    }

    public void setEmail(String email) {
      this.email = email;
    }

    @Override
    public String toString() {
      return "Customer{id='" + id + "', name='" + name + "', email='" + email + "'}";
    }
  }

  static DynamoDbClient getAlternatorClient(
      URI url, AwsCredentialsProvider myCredentials, String datacenter, String rack) {
    SdkHttpClient http =
        ApacheHttpClient.builder()
            .buildWithDefaults(
                AttributeMap.builder()
                    .put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, true)
                    .build());

    AlternatorDynamoDbClient.AlternatorDynamoDbClientBuilder builder =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(url)
            .credentialsProvider(myCredentials)
            .httpClient(http)
            .region(Region.US_EAST_1);

    if (datacenter != null && !datacenter.isEmpty()) {
      builder.withDatacenter(datacenter);
    }
    if (rack != null && !rack.isEmpty()) {
      builder.withRack(rack);
    }

    return builder.build();
  }

  public static void main(String[] args) {
    Logger logger = Logger.getLogger("com.scylladb.alternator");
    ConsoleHandler handler = new ConsoleHandler();
    handler.setLevel(Level.FINEST);
    logger.setLevel(Level.FINEST);
    logger.addHandler(handler);
    logger.setUseParentHandlers(false);

    ArgumentParser parser =
        ArgumentParsers.newFor("Demo4")
            .build()
            .defaultHelp(true)
            .description("Example of DynamoDB Enhanced Client with Alternator load balancing");

    try {
      parser
          .addArgument("-e", "--endpoint")
          .setDefault(new URI("http://localhost:8043"))
          .help("DynamoDB/Alternator endpoint");
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
    parser.addArgument("-u", "--user").setDefault("none").help("Credentials username");
    parser.addArgument("-p", "--password").setDefault("none").help("Credentials password");
    parser.addArgument("--datacenter").type(String.class).setDefault("").help("Target datacenter");
    parser.addArgument("--rack").type(String.class).setDefault("").help("Target rack");
    parser
        .addArgument("--table")
        .type(String.class)
        .setDefault("demo4_customers")
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
    String datacenter = ns.getString("datacenter");
    String rack = ns.getString("rack");
    String tableName = ns.getString("table");

    disableCertificateChecks();
    AwsCredentialsProvider myCredentials =
        StaticCredentialsProvider.create(AwsBasicCredentials.create(user, pass));

    // 1. Create standard DynamoDbClient with Alternator load balancing
    DynamoDbClient ddb = getAlternatorClient(URI.create(endpoint), myCredentials, datacenter, rack);

    // 2. Wrap with DynamoDbEnhancedClient - load balancing works transparently
    DynamoDbEnhancedClient enhancedClient =
        DynamoDbEnhancedClient.builder().dynamoDbClient(ddb).build();

    // 3. Get table reference with schema
    DynamoDbTable<Customer> customerTable =
        enhancedClient.table(tableName, TableSchema.fromBean(Customer.class));

    // 4. Create table if it doesn't exist
    try {
      customerTable.describeTable();
      System.out.println("Table " + tableName + " already exists");
    } catch (ResourceNotFoundException e) {
      System.out.println("Creating table " + tableName);
      customerTable.createTable();
      System.out.println("Table created");
    }

    // 5. Put some items - each request goes to a different node
    System.out.println("\nPutting items (watch logs for load balancing):");
    for (int i = 0; i < 5; i++) {
      Customer customer =
          new Customer("customer-" + i, "Customer " + i, "customer" + i + "@example.com");
      customerTable.putItem(customer);
      System.out.println("  Put: " + customer);
    }

    // 6. Get items back - each request goes to a different node
    System.out.println("\nGetting items:");
    for (int i = 0; i < 5; i++) {
      Customer customer =
          customerTable.getItem(Key.builder().partitionValue("customer-" + i).build());
      System.out.println("  Got: " + customer);
    }

    // 7. Scan table - request goes to one of the nodes
    System.out.println("\nScanning table:");
    customerTable.scan().items().forEach(customer -> System.out.println("  Scanned: " + customer));

    System.out.println("\nDone");
  }

  static void disableCertificateChecks() {
    TrustManager[] trustAllCerts =
        new TrustManager[] {
          new X509TrustManager() {
            public java.security.cert.X509Certificate[] getAcceptedIssuers() {
              return null;
            }

            public void checkClientTrusted(X509Certificate[] certs, String authType) {}

            public void checkServerTrusted(X509Certificate[] certs, String authType) {}
          }
        };
    try {
      SSLContext sc = SSLContext.getInstance("SSL");
      sc.init(null, trustAllCerts, new java.security.SecureRandom());
      HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
    } catch (Exception e) {
      e.printStackTrace();
    }
    HttpsURLConnection.setDefaultHostnameVerifier(
        new HostnameVerifier() {
          @Override
          public boolean verify(String arg0, SSLSession arg1) {
            return true;
          }
        });
  }
}
