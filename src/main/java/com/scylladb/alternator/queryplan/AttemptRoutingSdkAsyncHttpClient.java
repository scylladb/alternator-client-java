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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import software.amazon.awssdk.http.async.AsyncExecuteRequest;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;

/** Routes each asynchronous SDK transmission through its request query plan. */
public final class AttemptRoutingSdkAsyncHttpClient implements SdkAsyncHttpClient {
  private final SdkAsyncHttpClient delegate;
  private final BasicQueryPlanInterceptor router;

  public AttemptRoutingSdkAsyncHttpClient(
      SdkAsyncHttpClient delegate, BasicQueryPlanInterceptor router) {
    if (delegate == null) {
      throw new IllegalArgumentException("delegate cannot be null");
    }
    if (router == null) {
      throw new IllegalArgumentException("router cannot be null");
    }
    this.delegate = delegate;
    this.router = router;
  }

  @Override
  public CompletableFuture<Void> execute(AsyncExecuteRequest request) {
    BasicQueryPlanInterceptor.RoutedRequest routed =
        router.routeAttemptWithContext(request.request());
    CompletableFuture<AttemptRequestSigner.AsyncResult> signing =
        AttemptRequestSigner.signAsync(routed, request.requestContentPublisher());
    CompletableFuture<Void> result = new CompletableFuture<>();
    AtomicReference<CompletableFuture<Void>> transmission = new AtomicReference<>();
    signing.whenComplete(
        (signed, signingFailure) -> {
          if (signingFailure != null) {
            if (signing.isCancelled()) {
              result.cancel(false);
            } else {
              result.completeExceptionally(signingFailure);
            }
            return;
          }
          if (result.isCancelled()) {
            return;
          }
          CompletableFuture<Void> delegateResult;
          try {
            delegateResult = delegate.execute(withSignedAttempt(request, signed));
          } catch (RuntimeException | Error failure) {
            result.completeExceptionally(failure);
            return;
          }
          transmission.set(delegateResult);
          if (result.isCancelled()) {
            delegateResult.cancel(true);
          }
          delegateResult.whenComplete(
              (ignored, transmissionFailure) -> {
                if (delegateResult.isCancelled()) {
                  result.cancel(false);
                } else if (transmissionFailure != null) {
                  result.completeExceptionally(transmissionFailure);
                } else {
                  result.complete(null);
                }
              });
        });
    result.whenComplete(
        (ignored, failure) -> {
          if (result.isCancelled()) {
            signing.cancel(true);
            CompletableFuture<Void> delegateResult = transmission.get();
            if (delegateResult != null) {
              delegateResult.cancel(true);
            }
          }
        });
    return result;
  }

  private AsyncExecuteRequest withSignedAttempt(
      AsyncExecuteRequest request, AttemptRequestSigner.AsyncResult signed) {
    AsyncExecuteRequest routedExecuteRequest =
        AsyncExecuteRequest.builder()
            .request(signed.request)
            .requestContentPublisher(signed.payload)
            .responseHandler(request.responseHandler())
            .fullDuplex(request.fullDuplex())
            .metricCollector(request.metricCollector().orElse(null))
            .httpExecutionAttributes(request.httpExecutionAttributes())
            .build();
    return routedExecuteRequest;
  }

  @Override
  public void close() {
    delegate.close();
  }

  @Override
  public String clientName() {
    return delegate.clientName();
  }
}
