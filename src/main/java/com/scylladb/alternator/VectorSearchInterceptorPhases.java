// Copyright 2026-present ScyllaDB
//
// SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

package com.scylladb.alternator;

import com.scylladb.alternator.vectorsearch.VectorSearchInterceptor;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Optional;
import org.reactivestreams.Publisher;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.SdkHttpResponse;

/**
 * Splits vector request and response processing so both phases can run before caller-provided
 * interceptors.
 *
 * <p>The AWS SDK invokes request hooks in registration order and response hooks in reverse order.
 * Registering these two adapters on opposite sides of caller interceptors preserves that ordering
 * without changing the public, full-duplex {@link VectorSearchInterceptor#INSTANCE}.
 */
final class VectorSearchInterceptorPhases {

  static final ExecutionInterceptor REQUEST =
      new ExecutionInterceptor() {
        @Override
        public SdkHttpRequest modifyHttpRequest(
            Context.ModifyHttpRequest context, ExecutionAttributes executionAttributes) {
          return VectorSearchInterceptor.INSTANCE.modifyHttpRequest(context, executionAttributes);
        }

        @Override
        public Optional<RequestBody> modifyHttpContent(
            Context.ModifyHttpRequest context, ExecutionAttributes executionAttributes) {
          return VectorSearchInterceptor.INSTANCE.modifyHttpContent(context, executionAttributes);
        }

        @Override
        public String toString() {
          return "VectorSearchRequestInterceptor";
        }
      };

  static final ExecutionInterceptor RESPONSE =
      new ExecutionInterceptor() {
        @Override
        public SdkHttpResponse modifyHttpResponse(
            Context.ModifyHttpResponse context, ExecutionAttributes executionAttributes) {
          return VectorSearchInterceptor.INSTANCE.modifyHttpResponse(context, executionAttributes);
        }

        @Override
        public Optional<InputStream> modifyHttpResponseContent(
            Context.ModifyHttpResponse context, ExecutionAttributes executionAttributes) {
          return VectorSearchInterceptor.INSTANCE.modifyHttpResponseContent(
              context, executionAttributes);
        }

        @Override
        public Optional<Publisher<ByteBuffer>> modifyAsyncHttpResponseContent(
            Context.ModifyHttpResponse context, ExecutionAttributes executionAttributes) {
          return VectorSearchInterceptor.INSTANCE.modifyAsyncHttpResponseContent(
              context, executionAttributes);
        }

        @Override
        public String toString() {
          return "VectorSearchResponseInterceptor";
        }
      };

  private VectorSearchInterceptorPhases() {}
}
