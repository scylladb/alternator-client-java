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

import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.core.SelectedAuthScheme;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.SdkExecutionAttribute;
import software.amazon.awssdk.core.interceptor.SdkInternalExecutionAttribute;
import software.amazon.awssdk.http.ContentStreamProvider;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.async.SdkHttpContentPublisher;
import software.amazon.awssdk.http.auth.spi.scheme.AuthSchemeOption;
import software.amazon.awssdk.http.auth.spi.signer.AsyncSignRequest;
import software.amazon.awssdk.http.auth.spi.signer.AsyncSignedRequest;
import software.amazon.awssdk.http.auth.spi.signer.BaseSignRequest;
import software.amazon.awssdk.http.auth.spi.signer.HttpSigner;
import software.amazon.awssdk.http.auth.spi.signer.PayloadChecksumStore;
import software.amazon.awssdk.http.auth.spi.signer.SdkInternalHttpSignerProperty;
import software.amazon.awssdk.http.auth.spi.signer.SignRequest;
import software.amazon.awssdk.http.auth.spi.signer.SignedRequest;
import software.amazon.awssdk.identity.spi.Identity;

/** Re-signs a physical attempt when late health routing changes its authority. */
final class AttemptRequestSigner {
  private static final String AUTHORIZATION = "Authorization";
  private static final String HOST = "Host";

  private AttemptRequestSigner() {}

  static SyncResult signSync(
      BasicQueryPlanInterceptor.RoutedRequest routed, ContentStreamProvider originalPayload) {
    if (!routed.authorityChanged) {
      return new SyncResult(routed.request, originalPayload);
    }

    SdkHttpRequest unsigned = unsignedRequest(routed.request);
    SelectedAuthScheme<?> authScheme = selectedAuthScheme(routed.executionAttributes);
    if (authScheme == null) {
      requireUnsigned(routed.request);
      return new SyncResult(unsigned, originalPayload);
    }

    Identity identity = authScheme.identity().join();
    SignRequest.Builder<Identity> request =
        SignRequest.builder(identity).request(unsigned).payload(originalPayload);
    addSignerProperties(request, authScheme.authSchemeOption(), routed.executionAttributes);
    SignedRequest signed = sign(authScheme, request.build());
    return new SyncResult(signed.request(), signed.payload().orElse(null));
  }

  static CompletableFuture<AsyncResult> signAsync(
      BasicQueryPlanInterceptor.RoutedRequest routed, SdkHttpContentPublisher originalPayload) {
    if (!routed.authorityChanged) {
      return CompletableFuture.completedFuture(new AsyncResult(routed.request, originalPayload));
    }

    SdkHttpRequest unsigned = unsignedRequest(routed.request);
    SelectedAuthScheme<?> authScheme = selectedAuthScheme(routed.executionAttributes);
    if (authScheme == null) {
      requireUnsigned(routed.request);
      return CompletableFuture.completedFuture(new AsyncResult(unsigned, originalPayload));
    }

    CompletableFuture<AsyncResult> result = new CompletableFuture<>();
    CompletableFuture<? extends Identity> identity = authScheme.identity();
    AtomicReference<CompletableFuture<AsyncSignedRequest>> signing = new AtomicReference<>();
    identity.whenComplete(
        (resolvedIdentity, identityFailure) -> {
          if (identityFailure != null) {
            result.completeExceptionally(identityFailure);
            return;
          }
          if (result.isCancelled()) {
            return;
          }
          try {
            AsyncSignRequest.Builder<Identity> request =
                AsyncSignRequest.builder(resolvedIdentity)
                    .request(unsigned)
                    .payload(originalPayload);
            addSignerProperties(request, authScheme.authSchemeOption(), routed.executionAttributes);
            CompletableFuture<AsyncSignedRequest> signed = signAsync(authScheme, request.build());
            signing.set(signed);
            if (result.isCancelled()) {
              signed.cancel(true);
            }
            signed.whenComplete(
                (signedRequest, signingFailure) -> {
                  if (signed.isCancelled()) {
                    result.cancel(false);
                  } else if (signingFailure != null) {
                    result.completeExceptionally(signingFailure);
                  } else {
                    try {
                      result.complete(
                          new AsyncResult(
                              signedRequest.request(),
                              signedRequest
                                  .payload()
                                  .map(AttemptRequestSigner::adaptPublisher)
                                  .orElseGet(AttemptRequestSigner::emptyPublisher)));
                    } catch (RuntimeException | Error failure) {
                      result.completeExceptionally(failure);
                    }
                  }
                });
          } catch (RuntimeException | Error failure) {
            result.completeExceptionally(failure);
          }
        });
    result.whenComplete(
        (ignored, failure) -> {
          if (result.isCancelled()) {
            identity.cancel(true);
            CompletableFuture<AsyncSignedRequest> signed = signing.get();
            if (signed != null) {
              signed.cancel(true);
            }
          }
        });
    return result;
  }

  private static SdkHttpRequest unsignedRequest(SdkHttpRequest routed) {
    String host = bracketIpv6Literal(routed.host());
    return routed.toBuilder()
        .host(host)
        .removeHeader(AUTHORIZATION)
        .putHeader(HOST, authority(routed.protocol(), host, routed.port()))
        .build();
  }

  private static String bracketIpv6Literal(String host) {
    if (host.indexOf(':') >= 0 && !(host.startsWith("[") && host.endsWith("]"))) {
      return "[" + host + "]";
    }
    return host;
  }

  private static String authority(String protocol, String host, int port) {
    boolean standard =
        ("http".equalsIgnoreCase(protocol) && port == 80)
            || ("https".equalsIgnoreCase(protocol) && port == 443);
    return standard ? host : host + ":" + port;
  }

  private static void requireUnsigned(SdkHttpRequest original) {
    if (original.firstMatchingHeader(AUTHORIZATION).isPresent()) {
      throw new IllegalStateException(
          "cannot re-sign rerouted request because SDK signing context is unavailable");
    }
  }

  private static SelectedAuthScheme<?> selectedAuthScheme(ExecutionAttributes attributes) {
    return attributes != null
        ? attributes.getAttribute(SdkInternalExecutionAttribute.SELECTED_AUTH_SCHEME)
        : null;
  }

  private static Clock signingClock(ExecutionAttributes attributes) {
    Integer offset = attributes.getAttribute(SdkExecutionAttribute.TIME_OFFSET);
    return Clock.offset(Clock.systemUTC(), Duration.ofSeconds(-(offset != null ? offset : 0)));
  }

  private static <B extends BaseSignRequest.Builder<B, ?, ?>> void addSignerProperties(
      B request, AuthSchemeOption option, ExecutionAttributes executionAttributes) {
    request.putProperty(HttpSigner.SIGNING_CLOCK, signingClock(executionAttributes));
    PayloadChecksumStore checksumStore =
        executionAttributes.getAttribute(SdkInternalExecutionAttribute.CHECKSUM_STORE);
    if (checksumStore != null) {
      request.putProperty(SdkInternalHttpSignerProperty.CHECKSUM_STORE, checksumStore);
    }
    option.forEachSignerProperty(request::putProperty);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static SignedRequest sign(SelectedAuthScheme authScheme, SignRequest request) {
    return authScheme.signer().sign(request);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static CompletableFuture<AsyncSignedRequest> signAsync(
      SelectedAuthScheme authScheme, AsyncSignRequest request) {
    return authScheme.signer().signAsync(request);
  }

  private static SdkHttpContentPublisher adaptPublisher(Publisher<ByteBuffer> signed) {
    if (signed instanceof SdkHttpContentPublisher) {
      return (SdkHttpContentPublisher) signed;
    }
    return new SdkHttpContentPublisher() {
      @Override
      public Optional<Long> contentLength() {
        return Optional.empty();
      }

      @Override
      public void subscribe(Subscriber<? super ByteBuffer> subscriber) {
        signed.subscribe(subscriber);
      }
    };
  }

  private static SdkHttpContentPublisher emptyPublisher() {
    return new SdkHttpContentPublisher() {
      @Override
      public Optional<Long> contentLength() {
        return Optional.of(0L);
      }

      @Override
      public void subscribe(Subscriber<? super ByteBuffer> subscriber) {
        AtomicBoolean completed = new AtomicBoolean();
        subscriber.onSubscribe(
            new Subscription() {
              @Override
              public void request(long count) {
                if (completed.compareAndSet(false, true)) {
                  subscriber.onComplete();
                }
              }

              @Override
              public void cancel() {
                completed.set(true);
              }
            });
      }
    };
  }

  static final class SyncResult {
    final SdkHttpRequest request;
    final ContentStreamProvider payload;

    private SyncResult(SdkHttpRequest request, ContentStreamProvider payload) {
      this.request = request;
      this.payload = payload;
    }
  }

  static final class AsyncResult {
    final SdkHttpRequest request;
    final SdkHttpContentPublisher payload;

    private AsyncResult(SdkHttpRequest request, SdkHttpContentPublisher payload) {
      this.request = request;
      this.payload = payload;
    }
  }
}
