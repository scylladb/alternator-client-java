/*
 * Copyright ScyllaDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.scylladb.alternator.queryplan;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.core.SelectedAuthScheme;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.SdkInternalExecutionAttribute;
import software.amazon.awssdk.http.ContentStreamProvider;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.async.SdkHttpContentPublisher;
import software.amazon.awssdk.http.auth.spi.scheme.AuthSchemeOption;
import software.amazon.awssdk.http.auth.spi.signer.AsyncSignRequest;
import software.amazon.awssdk.http.auth.spi.signer.AsyncSignedRequest;
import software.amazon.awssdk.http.auth.spi.signer.HttpSigner;
import software.amazon.awssdk.http.auth.spi.signer.SignRequest;
import software.amazon.awssdk.http.auth.spi.signer.SignedRequest;
import software.amazon.awssdk.identity.spi.Identity;

public class AttemptRequestSignerTest {
  private static final Identity IDENTITY =
      new Identity() {
        @Override
        public Optional<Instant> expirationTime() {
          return Optional.empty();
        }

        @Override
        public Optional<String> providerName() {
          return Optional.of("test");
        }
      };

  @Test
  public void signerCanRemoveSyncAndAsyncPayloads() {
    RemovingSigner signer = new RemovingSigner();
    BasicQueryPlanInterceptor.RoutedRequest routed = routedRequest(signer);

    AttemptRequestSigner.SyncResult sync =
        AttemptRequestSigner.signSync(routed, ContentStreamProvider.fromUtf8String("body"));
    AttemptRequestSigner.AsyncResult async =
        AttemptRequestSigner.signAsync(routed, publisher()).join();

    assertNull(sync.payload);
    assertTrue(async.payload.contentLength().isPresent());
    assertEquals(Long.valueOf(0), async.payload.contentLength().get());
    AtomicReference<Boolean> completed = new AtomicReference<>(false);
    async.payload.subscribe(completionSubscriber(completed));
    assertTrue(completed.get());
  }

  @Test
  public void cancellingAsyncResultCancelsSignerFuture() {
    PendingSigner signer = new PendingSigner();
    CompletableFuture<AttemptRequestSigner.AsyncResult> result =
        AttemptRequestSigner.signAsync(routedRequest(signer), publisher());

    result.cancel(true);

    assertTrue(result.isCancelled());
    assertTrue(signer.result.isCancelled());
  }

  @Test
  public void synchronousAsyncSignerFailureCompletesResultExceptionally() {
    CompletableFuture<AttemptRequestSigner.AsyncResult> result =
        AttemptRequestSigner.signAsync(routedRequest(new ThrowingSigner()), publisher());

    assertTrue(result.isCompletedExceptionally());
  }

  @Test
  public void transformedAsyncPayloadDoesNotReuseOriginalContentLength() {
    AttemptRequestSigner.AsyncResult result =
        AttemptRequestSigner.signAsync(routedRequest(new TransformingSigner()), publisher()).join();

    assertFalse(result.payload.contentLength().isPresent());
  }

  @Test
  public void ipv6AuthoritiesAreBracketedForSyncAndAsyncSigning() {
    RemovingSigner signer = new RemovingSigner();
    BasicQueryPlanInterceptor.RoutedRequest routed = routedRequest(signer, "http", "::1", 8080);

    AttemptRequestSigner.SyncResult sync =
        AttemptRequestSigner.signSync(routed, ContentStreamProvider.fromUtf8String("body"));
    AttemptRequestSigner.AsyncResult async =
        AttemptRequestSigner.signAsync(routed, publisher()).join();

    assertEquals("[::1]:8080", sync.request.firstMatchingHeader("Host").get());
    assertEquals("[::1]:8080", async.request.firstMatchingHeader("Host").get());
    assertEquals("[::1]:8080", sync.request.getUri().getRawAuthority());
    assertEquals("[::1]:8080", async.request.getUri().getRawAuthority());

    AttemptRequestSigner.SyncResult standardPort =
        AttemptRequestSigner.signSync(
            routedRequest(signer, "https", "[2001:db8::1]", 443),
            ContentStreamProvider.fromUtf8String("body"));
    assertEquals("[2001:db8::1]", standardPort.request.firstMatchingHeader("Host").get());
  }

  private static BasicQueryPlanInterceptor.RoutedRequest routedRequest(
      HttpSigner<Identity> signer) {
    return routedRequest(signer, "http", "new.local", 8080);
  }

  private static BasicQueryPlanInterceptor.RoutedRequest routedRequest(
      HttpSigner<Identity> signer, String protocol, String host, int port) {
    ExecutionAttributes attributes = ExecutionAttributes.builder().build();
    attributes.putAttribute(
        SdkInternalExecutionAttribute.SELECTED_AUTH_SCHEME,
        new SelectedAuthScheme<>(
            CompletableFuture.completedFuture(IDENTITY),
            signer,
            AuthSchemeOption.builder().schemeId("test").build()));
    SdkHttpRequest request =
        SdkHttpRequest.builder()
            .protocol(protocol)
            .host(host)
            .port(port)
            .method(SdkHttpMethod.POST)
            .encodedPath("/")
            .putHeader("Host", "old.local:8080")
            .putHeader("Authorization", "old-signature")
            .build();
    return new BasicQueryPlanInterceptor.RoutedRequest(request, attributes, true);
  }

  private static SdkHttpContentPublisher publisher() {
    return new SdkHttpContentPublisher() {
      @Override
      public Optional<Long> contentLength() {
        return Optional.of(4L);
      }

      @Override
      public void subscribe(Subscriber<? super ByteBuffer> subscriber) {
        subscriber.onSubscribe(
            new Subscription() {
              @Override
              public void request(long count) {
                subscriber.onNext(ByteBuffer.wrap(new byte[] {1, 2, 3, 4}));
                subscriber.onComplete();
              }

              @Override
              public void cancel() {}
            });
      }
    };
  }

  private static Subscriber<ByteBuffer> completionSubscriber(AtomicReference<Boolean> completed) {
    return new Subscriber<ByteBuffer>() {
      @Override
      public void onSubscribe(Subscription subscription) {
        subscription.request(Long.MAX_VALUE);
      }

      @Override
      public void onNext(ByteBuffer item) {
        throw new AssertionError("empty publisher emitted content");
      }

      @Override
      public void onError(Throwable failure) {
        throw new AssertionError(failure);
      }

      @Override
      public void onComplete() {
        completed.set(true);
      }
    };
  }

  private static final class RemovingSigner implements HttpSigner<Identity> {
    @Override
    public SignedRequest sign(SignRequest<? extends Identity> request) {
      return SignedRequest.builder().request(request.request()).build();
    }

    @Override
    public CompletableFuture<AsyncSignedRequest> signAsync(
        AsyncSignRequest<? extends Identity> request) {
      return CompletableFuture.completedFuture(
          AsyncSignedRequest.builder().request(request.request()).build());
    }
  }

  private static final class PendingSigner implements HttpSigner<Identity> {
    private final CompletableFuture<AsyncSignedRequest> result = new CompletableFuture<>();

    @Override
    public SignedRequest sign(SignRequest<? extends Identity> request) {
      throw new AssertionError("sync signer should not run");
    }

    @Override
    public CompletableFuture<AsyncSignedRequest> signAsync(
        AsyncSignRequest<? extends Identity> request) {
      return result;
    }
  }

  private static final class ThrowingSigner implements HttpSigner<Identity> {
    @Override
    public SignedRequest sign(SignRequest<? extends Identity> request) {
      throw new AssertionError("sync signer should not run");
    }

    @Override
    public CompletableFuture<AsyncSignedRequest> signAsync(
        AsyncSignRequest<? extends Identity> request) {
      throw new IllegalStateException("simulated signer failure");
    }
  }

  private static final class TransformingSigner implements HttpSigner<Identity> {
    @Override
    public SignedRequest sign(SignRequest<? extends Identity> request) {
      throw new AssertionError("sync signer should not run");
    }

    @Override
    public CompletableFuture<AsyncSignedRequest> signAsync(
        AsyncSignRequest<? extends Identity> request) {
      Publisher<ByteBuffer> transformed =
          subscriber -> request.payload().get().subscribe(subscriber);
      return CompletableFuture.completedFuture(
          AsyncSignedRequest.builder().request(request.request()).payload(transformed).build());
    }
  }
}
