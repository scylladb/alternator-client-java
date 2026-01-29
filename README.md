# Alternator - Client-side load balancing - Java

## Introduction
As explained in the [toplevel README](../README.md), DynamoDB applications
are usually aware of a _single endpoint_, a single URL to which they
connect - e.g., `http://dynamodb.us-east-1.amazonaws.com`. But Alternator
is distributed over a cluster of nodes and we would like the application to
send requests to all these nodes - not just to one. This is important for two
reasons: **high availability** (the failure of a single Alternator node should
not prevent the client from proceeding) and **load balancing** over all
Alternator nodes.

One of the ways to do this is to provide a modified library, which will
allow a mostly-unmodified application which is only aware of one
"enpoint URL" to send its requests to many different Alternator nodes.

Our intention is _not_ to fork the existing AWS client library (SDK) for Java.
Rather, our intention is to provide a tiny library which tacks on to any
version of the AWS SDK that the application is already using, and makes
it do the right thing for Alternator.

This library supports AWS SDK for Java Version 2 (requires 2.20 or above).

## Add `load-balancing` to your project

### Maven Dependency

Add the `load-balancing` dependency to your Maven project by adding the
following `dependency` to your `pom.xml` definition:

~~~ xml
<dependency>
  <groupId>com.scylladb.alternator</groupId>
  <artifactId>load-balancing</artifactId>
  <version>1.0.0</version>
</dependency>
~~~

You can find the latest version [here](https://central.sonatype.com/artifact/com.scylladb.alternator/load-balancing).

### Alternatively, build the LoadBalancing jar
To build a jar of the Alternator client-side load balancer, use
```
mvn package
```
Which creates `target/load-balancing-1.0.0-SNAPSHOT.jar`.

## Usage

As explained above, this package does not _replace_ the AWS SDK for Java, but
accompanies it. The package provides a new mechanism to configure a
DynamoDbClient object, which the application can then use normally using the
standard AWS SDK for Java, to make requests. The load balancer library ensures
that each of these requests goes to a different live Alternator node.

The load balancer library is also responsible for _discovering_ which
Alternator nodes exist, and maintaining this list as the Alternator cluster
changes. It does this using an additional background thread, which
periodically polls one of the known nodes, asking it for a list of all other
nodes (in this data-center).

### Using the library

An application creates a `DynamoDbClient` object
and then uses it to perform various requests. Traditionally, to create such
an object, an application that wishes to connect to a specific URL would use
code that looks something like this:

```java
    static AwsCredentialsProvider myCredentials =
        StaticCredentialsProvider.create(AwsBasicCredentials.create("myuser", "mypassword"));
    URI uri = URI.create("https://127.0.0.1:8043");
    DynamoDbClient client = DynamoDbClient.builder()
        .region(Region.US_EAST_1)
        .endpointOverride(url)
        .credentialsProvider(myCredentials)
        .build();
```

The `region()` chosen doesn't matter when the endpoint is explicitly chosen
with `endpointOverride()`, but nevertheless should be specified otherwise the
SDK will try to look it up in a configuration file, and complain if it isn't
set there.

#### Option 1: Using `AlternatorDynamoDbClient` (Recommended)

The simplest way to use the Alternator load balancer is to use the `AlternatorDynamoDbClient.builder()`,
which provides a familiar builder API that implements `DynamoDbClientBuilder`:

```java
    import com.scylladb.alternator.AlternatorDynamoDbClient;

static AwsCredentialsProvider myCredentials =
    StaticCredentialsProvider.create(AwsBasicCredentials.create("myuser", "mypassword"));

DynamoDbClient client = AlternatorDynamoDbClient.builder()
    .endpointOverride(URI.create("https://127.0.0.1:8043"))
    .credentialsProvider(myCredentials)
    .build();
```

The `AlternatorDynamoDbClient` automatically integrates the `AlternatorEndpointProvider`
to provide load balancing across all Alternator nodes.

#### Option 2: Using `AlternatorEndpointProvider` directly (Outdated)

Alternatively, you can manually use the `AlternatorEndpointProvider`:

```java
    import com.scylladb.alternator.AlternatorEndpointProvider;

    static AwsCredentialsProvider myCredentials =
        StaticCredentialsProvider.create(AwsBasicCredentials.create("myuser", "mypassword"));
    URI uri = URI.create("https://127.0.0.1:8043");
    AlternatorEndpointProvider alternatorEndpointProvider = new AlternatorEndpointProvider(uri);
    DynamoDbClient client = DynamoDbClient.builder()
        .region(Region.US_EAST_1)
        .endpointProvider(alternatorEndpointProvider)
        .credentialsProvider(myCredentials)
        .build();
```

Please note that the `endpointProvider()` API is new to AWS Java SDK 2.20
(Release February 2023), so you should use this version or newer.

The application can then use this `DynamoDbClient` object completely normally,
just that each request will go to a different Alternator node, instead of all
of them going to the same URL.

The parameter `uri` is one known Alternator node, which is then contacted to
discover the rest. After this initialization, this original node may go down
at any time - any other already-known node can be used to retrieve the node
list, and we no longer rely on the original node.

You can see `src/test/java/com/scylladb/alternator/test/Demo2.java` for a
complete example of using client-side load balancing with AWS SDK for Java.
After building with `mvn package`, you can run this demo with the command:
```
mvn exec:java -Dexec.mainClass=com.scylladb.alternator.test.Demo2 -Dexec.classpathScope=test
```

#### Asynchronous operation

You can achieve better scalability and performance using the asynchronous
versions of API calls and `java.util.concurrent` completion chaining.

##### Option 1: Using `AlternatorDynamoDbAsyncClient` (Recommended)

The simplest way to create a `DynamoDbAsyncClient` with Alternator load balancing is to use
the `AlternatorDynamoDbAsyncClient.builder()`:

```java
    import com.scylladb.alternator.AlternatorDynamoDbAsyncClient;

static AwsCredentialsProvider myCredentials =
    StaticCredentialsProvider.create(AwsBasicCredentials.create("myuser", "mypassword"));

DynamoDbAsyncClient client = AlternatorDynamoDbAsyncClient.builder()
    .endpointOverride(URI.create("https://127.0.0.1:8043"))
    .credentialsProvider(myCredentials)
    .build();
```

##### Option 2: Using `AlternatorEndpointProvider` directly (Outdated)

Alternatively, you can manually use the `endpointProvider()` method on the
`DynamoDbAsyncClientBuilder`, passing an `AlternatorEndpointProvider` object:

```java
    import com.scylladb.alternator.AlternatorEndpointProvider;

    static AwsCredentialsProvider myCredentials =
        StaticCredentialsProvider.create(AwsBasicCredentials.create("myuser", "mypassword"));
    URI uri = URI.create("https://127.0.0.1:8043");
    AlternatorEndpointProvider alternatorEndpointProvider = new AlternatorEndpointProvider(uri);
    DynamoDbAsyncClient client = DynamoDbAsyncClient.builder()
        .region(Region.US_EAST_1)
        .endpointProvider(alternatorEndpointProvider)
        .credentialsProvider(myCredentials)
        .build();
```

You can see `src/test/java/com/scylladb/alternator/test/Demo3.java` for a
complete example of using client-side load balancing with the asynchronous API.
After building with `mvn package`, you can run this demo with the command:
```
mvn exec:java -Dexec.mainClass=com.scylladb.alternator.test.Demo3 -Dexec.classpathScope=test
```

### Using DynamoDB Enhanced Client

The [DynamoDB Enhanced Client](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/dynamodb-enhanced-client.html)
provides a high-level, object-mapping interface that simplifies working with DynamoDB tables
by mapping Java classes to table items. Since the enhanced client wraps the standard
`DynamoDbClient`, load balancing works transparently.

#### Synchronous Enhanced Client

```java
import com.scylladb.alternator.AlternatorDynamoDbClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;

@DynamoDbBean
public class Customer {
    private String id;
    private String name;

    @DynamoDbPartitionKey
    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
}

// 1. Create standard DynamoDbClient with load balancing
DynamoDbClient client = AlternatorDynamoDbClient.builder()
    .endpointOverride(URI.create("http://localhost:8043"))
    .credentialsProvider(myCredentials)
    .build();

// 2. Wrap with DynamoDbEnhancedClient
DynamoDbEnhancedClient enhancedClient = DynamoDbEnhancedClient.builder()
    .dynamoDbClient(client)
    .build();

// 3. Use the enhanced client normally
DynamoDbTable<Customer> customerTable = enhancedClient.table(
    "customers",
    TableSchema.fromBean(Customer.class)
);
customerTable.putItem(new Customer("123", "John Doe"));
```

You can see `src/test/java/com/scylladb/alternator/test/Demo4.java` for a
complete example. After building with `mvn package`, you can run this demo with:
```
mvn exec:java -Dexec.mainClass=com.scylladb.alternator.test.Demo4 -Dexec.classpathScope=test
```

#### Asynchronous Enhanced Client

```java
import com.scylladb.alternator.AlternatorDynamoDbAsyncClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedAsyncClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbAsyncTable;

// 1. Create async DynamoDbAsyncClient with load balancing
DynamoDbAsyncClient asyncClient = AlternatorDynamoDbAsyncClient.builder()
    .endpointOverride(URI.create("http://localhost:8043"))
    .credentialsProvider(myCredentials)
    .build();

// 2. Wrap with DynamoDbEnhancedAsyncClient
DynamoDbEnhancedAsyncClient enhancedAsyncClient = DynamoDbEnhancedAsyncClient.builder()
    .dynamoDbClient(asyncClient)
    .build();

// 3. Use the enhanced async client with CompletableFuture
DynamoDbAsyncTable<Customer> customerTable = enhancedAsyncClient.table(
    "customers",
    TableSchema.fromBean(Customer.class)
);
customerTable.putItem(new Customer("123", "John Doe"))
    .thenAccept(v -> System.out.println("Item saved"))
    .join();
```

You can see `src/test/java/com/scylladb/alternator/test/Demo5.java` for a
complete example. After building with `mvn package`, you can run this demo with:
```
mvn exec:java -Dexec.mainClass=com.scylladb.alternator.test.Demo5 -Dexec.classpathScope=test
```

### Request Compression

The library supports optional GZIP compression for HTTP request bodies, which can
reduce network bandwidth usage when sending large payloads to Alternator.

#### Why not use AWS SDK's built-in compression?

AWS SDK for Java v2 includes a `CompressionConfiguration` feature, but it **only works
for AWS services that have the `@requestCompression` trait** in their service model
(e.g., CloudWatch's `PutMetricData`). DynamoDB does not support this trait, so the
SDK's built-in compression cannot be used for DynamoDB or Alternator requests.

This library provides its own compression implementation using an `ExecutionInterceptor`
that works with any DynamoDB/Alternator operation.

#### Enabling compression

To enable request compression, configure `AlternatorConfig` with a compression algorithm:

```java
import com.scylladb.alternator.AlternatorConfig;
import com.scylladb.alternator.AlternatorDynamoDbClient;
import com.scylladb.alternator.RequestCompressionAlgorithm;

AlternatorConfig config = AlternatorConfig.builder()
    .withCompressionAlgorithm(RequestCompressionAlgorithm.GZIP)
    .build();

DynamoDbClient client = AlternatorDynamoDbClient.builder()
    .endpointOverride(URI.create("https://127.0.0.1:8043"))
    .credentialsProvider(myCredentials)
    .withAlternatorConfig(config)
    .build();
```

#### Compression threshold

By default, only requests with bodies >= 1024 bytes (1 KB) are compressed. This avoids
the overhead of compressing small payloads that may not benefit from compression.
You can customize this threshold:

```java
AlternatorConfig config = AlternatorConfig.builder()
    .withCompressionAlgorithm(RequestCompressionAlgorithm.GZIP)
    .withMinCompressionSizeBytes(2048)  // Only compress requests >= 2KB
    .build();
```

#### When to use compression

Request compression is recommended for:
- Large item attributes (documents, JSON blobs)
- Batch operations with many items (`BatchWriteItem`, `BatchGetItem`)
- Text-heavy data that compresses well

Request compression may not be beneficial for:
- Small requests (< 1KB)
- Already-compressed binary data
- Low-latency requirements where CPU overhead matters

### Headers Optimization

The library supports optional HTTP headers optimization, which reduces network bandwidth by
removing headers that Alternator does not use. According to benchmarks, this can reduce
outgoing traffic by up to 56% depending on workload and encryption.

#### Enabling headers optimization

To enable headers optimization, configure `AlternatorConfig` with `withOptimizeHeaders(true)`:

```java
import com.scylladb.alternator.AlternatorConfig;
import com.scylladb.alternator.AlternatorDynamoDbClient;

AlternatorConfig config = AlternatorConfig.builder()
    .withOptimizeHeaders(true)
    .build();

DynamoDbClient client = AlternatorDynamoDbClient.builder()
    .endpointOverride(URI.create("https://127.0.0.1:8043"))
    .credentialsProvider(myCredentials)
    .withAlternatorConfig(config)
    .build();
```

#### Default headers whitelist

When headers optimization is enabled, only the following headers are preserved by default:
- `Host` - Required by HTTP/1.1
- `X-Amz-Target` - Specifies the DynamoDB operation
- `Content-Type` - MIME type for DynamoDB API
- `Content-Length` - Required for request body
- `Accept-Encoding` - For response compression negotiation
- `Content-Encoding` - For request compression (when enabled)
- `Authorization` - AWS SigV4 signature
- `X-Amz-Date` - Timestamp for AWS signature
- `X-Amz-Content-Sha256` - Content hash for AWS SigV4

All other headers (such as `User-Agent`, `X-Amz-Sdk-Invocation-Id`, `amz-sdk-request`) are removed.

#### Custom headers whitelist

You can provide your own custom headers whitelist if needed:

```java
import java.util.Arrays;

AlternatorConfig config = AlternatorConfig.builder()
    .withOptimizeHeaders(true)
    .withHeadersWhitelist(Arrays.asList(
        "Host", "X-Amz-Target", "Content-Type", "Content-Length",
        "Authorization", "X-Amz-Date", "X-Custom-Header"))
    .build();

DynamoDbClient client = AlternatorDynamoDbClient.builder()
    .endpointOverride(URI.create("https://127.0.0.1:8043"))
    .credentialsProvider(myCredentials)
    .withAlternatorConfig(config)
    .build();
```

**Important:** When using a custom whitelist, make sure to include all headers required for
authentication (`Authorization`, `X-Amz-Date`) and operation (`Host`, `X-Amz-Target`,
`Content-Type`, `Content-Length`).

#### Combining with compression

Headers optimization can be combined with request compression for maximum bandwidth savings:

```java
AlternatorConfig config = AlternatorConfig.builder()
    .withCompressionAlgorithm(RequestCompressionAlgorithm.GZIP)
    .withOptimizeHeaders(true)
    .build();
```

#### No authentication mode

When connecting to Alternator clusters with authentication disabled, you can configure the client
to skip authentication entirely:

```java
AlternatorConfig config = AlternatorConfig.builder()
    .withAuthenticationEnabled(false)
    .withOptimizeHeaders(true)
    .build();

// No credentials needed - client will use anonymous credentials
DynamoDbClient client = AlternatorDynamoDbClient.builder()
    .endpointOverride(URI.create("http://localhost:8000"))
    .withAlternatorConfig(config)
    .build();
```

When `withAuthenticationEnabled(false)` is set:
- The client uses anonymous credentials (no AWS signature)
- When header optimization is enabled, authentication headers (`Authorization`, `X-Amz-Date`)
  are automatically excluded from the whitelist

**Auto-detection:** When using `AlternatorDynamoDbClient` or `AlternatorDynamoDbAsyncClient` builders,
authentication is automatically detected based on whether `credentialsProvider()` was called. If no
credentials provider is set, the client automatically uses anonymous credentials:

```java
// Automatic: no credentials provider = no authentication
DynamoDbClient client = AlternatorDynamoDbClient.builder()
    .endpointOverride(URI.create("http://localhost:8000"))
    .build();
```

### Routing Scope (Datacenter/Rack Targeting)

The library supports targeting specific datacenters and racks with hierarchical fallback chains.
This allows you to prefer local nodes while gracefully falling back to other nodes when needed.

#### Basic Usage

```java
import com.scylladb.alternator.AlternatorDynamoDbClient;
import com.scylladb.alternator.routing.ClusterScope;
import com.scylladb.alternator.routing.DatacenterScope;
import com.scylladb.alternator.routing.RackScope;

// Target a specific rack with fallback to datacenter, then cluster
RoutingScope scope = RackScope.of("dc1", "rack1",
    DatacenterScope.of("dc1",
        ClusterScope.create()));

DynamoDbClient client = AlternatorDynamoDbClient.builder()
    .endpointOverride(URI.create("https://127.0.0.1:8043"))
    .credentialsProvider(myCredentials)
    .withRoutingScope(scope)
    .build();
```

#### Scope Types

- **ClusterScope** - Use all nodes in the cluster (no filtering)
- **DatacenterScope** - Use only nodes in a specific datacenter
- **RackScope** - Use only nodes in a specific rack within a datacenter

#### Fallback Chains

Each scope can have a fallback scope. When no nodes are available in the current scope,
the client automatically falls back to the next scope in the chain:

```java
// Rack -> Datacenter -> Cluster fallback chain
RoutingScope scope = RackScope.of("dc1", "rack1",
    DatacenterScope.of("dc1",
        ClusterScope.create()));

// Rack -> Another Rack -> Datacenter -> Cluster
RoutingScope scope = RackScope.of("dc1", "rack1",
    RackScope.of("dc1", "rack2",
        DatacenterScope.of("dc1",
            ClusterScope.create())));

// Datacenter -> Cluster fallback
RoutingScope scope = DatacenterScope.of("dc1", ClusterScope.create());

// Strict targeting (no fallback) - fails if no nodes available
RoutingScope scope = DatacenterScope.of("dc1", null);
```

#### Using with AlternatorConfig

You can also configure routing scope via `AlternatorConfig`:

```java
import com.scylladb.alternator.AlternatorConfig;

AlternatorConfig config = AlternatorConfig.builder()
    .withSeedHosts(Arrays.asList("192.168.1.100", "192.168.1.101"))
    .withScheme("https")
    .withPort(8043)
    .withRoutingScope(DatacenterScope.of("dc1", ClusterScope.create()))
    .withCompressionAlgorithm(RequestCompressionAlgorithm.GZIP)
    .withOptimizeHeaders(true)
    .build();

AlternatorLiveNodes liveNodes = new AlternatorLiveNodes(config);
```
