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
package com.scylladb.alternator.internal;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;

/** Executes and buffers one polling response under a hard caller deadline and size cap. */
final class BoundedPollingAttempt {

  private static final int BUFFER_SIZE = 8 * 1024;
  private static final ThreadPoolExecutor EXECUTOR = newExecutor(4);
  // A custom request implementation can ignore both interrupt and abort. Keep an independent
  // two-worker lane for retained seeds so stuck learned-node transports cannot consume all
  // recovery work, and one stuck seed cannot block a later seed or newer refresh generation.
  private static final ThreadPoolExecutor SEED_EXECUTOR = newExecutor(2);

  private static ThreadPoolExecutor newExecutor(int workers) {
    ThreadPoolExecutor executor =
        new ThreadPoolExecutor(
            workers,
            workers,
            60L,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(32),
            new AttemptThreadFactory(),
            new ThreadPoolExecutor.AbortPolicy());
    executor.allowCoreThreadTimeOut(true);
    return executor;
  }

  private BoundedPollingAttempt() {}

  static Result execute(ExecutableHttpRequest request, long timeoutMillis, int maximumResponseBytes)
      throws IOException {
    return execute(request, timeoutMillis, maximumResponseBytes, false);
  }

  static Result execute(
      ExecutableHttpRequest request,
      long timeoutMillis,
      int maximumResponseBytes,
      boolean seedCandidate)
      throws IOException {
    if (maximumResponseBytes <= 0) {
      request.abort();
      throw new IllegalArgumentException("maximumResponseBytes must be positive");
    }
    if (timeoutMillis <= 0) {
      request.abort();
      throw timeout(0, null);
    }
    Control control = new Control(request);
    FutureTask<Result> future =
        new FutureTask<>(() -> executeOnWorker(request, control, maximumResponseBytes));
    ThreadPoolExecutor executor = seedCandidate ? SEED_EXECUTOR : EXECUTOR;
    try {
      executor.execute(future);
    } catch (RejectedExecutionException e) {
      control.abort();
      throw new IOException("Shared polling-attempt work queue is full", e);
    }

    try {
      return future.get(timeoutMillis, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      future.cancel(true);
      executor.remove(future);
      control.abort();
      throw timeout(timeoutMillis, e);
    } catch (InterruptedException e) {
      future.cancel(true);
      executor.remove(future);
      control.abort();
      Thread.currentThread().interrupt();
      InterruptedIOException interrupted =
          new InterruptedIOException("Interrupted while reading /localnodes");
      interrupted.initCause(e);
      throw interrupted;
    } catch (ExecutionException e) {
      control.abort();
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      throw new IOException("Failed to execute /localnodes request", cause);
    }
  }

  private static Result executeOnWorker(
      ExecutableHttpRequest request, Control control, int maximumResponseBytes) throws IOException {
    try {
      HttpExecuteResponse response = request.call();
      int statusCode = response.httpResponse().statusCode();
      Optional<AbortableInputStream> responseBody = response.responseBody();
      if (statusCode != 200) {
        responseBody.ifPresent(control::setBodyAndAbort);
        control.abort();
        return Result.nonSuccess(statusCode);
      }
      if (!responseBody.isPresent()) {
        control.abort();
        return Result.missingBody(statusCode);
      }

      AbortableInputStream body = responseBody.get();
      control.setBody(body);
      try (AbortableInputStream ignored = body) {
        return Result.success(statusCode, readBounded(body, maximumResponseBytes, control));
      } finally {
        control.clearBody(body);
      }
    } catch (IOException | RuntimeException e) {
      control.abort();
      throw e;
    }
  }

  private static byte[] readBounded(
      AbortableInputStream body, int maximumResponseBytes, Control control) throws IOException {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    byte[] buffer = new byte[BUFFER_SIZE];
    while (true) {
      int read = body.read(buffer);
      if (read < 0) {
        return output.toByteArray();
      }
      if (read > maximumResponseBytes - output.size()) {
        control.abort();
        throw new IOException("/localnodes response exceeds " + maximumResponseBytes + " bytes");
      }
      output.write(buffer, 0, read);
    }
  }

  private static SocketTimeoutException timeout(long timeoutMillis, Exception cause) {
    SocketTimeoutException timeout =
        new SocketTimeoutException(
            "/localnodes request exceeded whole-attempt deadline of " + timeoutMillis + " ms");
    if (cause != null) {
      timeout.initCause(cause);
    }
    return timeout;
  }

  static final class Result {
    final int statusCode;
    final byte[] body;
    final boolean bodyPresent;

    private Result(int statusCode, byte[] body, boolean bodyPresent) {
      this.statusCode = statusCode;
      this.body = body;
      this.bodyPresent = bodyPresent;
    }

    static Result success(int statusCode, byte[] body) {
      return new Result(statusCode, body, true);
    }

    static Result nonSuccess(int statusCode) {
      return new Result(statusCode, null, false);
    }

    static Result missingBody(int statusCode) {
      return new Result(statusCode, null, false);
    }

    boolean isSuccess() {
      return statusCode == 200 && bodyPresent;
    }
  }

  private static final class Control {
    private final ExecutableHttpRequest request;
    private final AtomicReference<AbortableInputStream> body = new AtomicReference<>();
    private final AtomicBoolean aborted = new AtomicBoolean(false);

    Control(ExecutableHttpRequest request) {
      this.request = request;
    }

    void setBody(AbortableInputStream responseBody) {
      body.set(responseBody);
      if (aborted.get()) {
        abortBody(responseBody);
      }
    }

    void setBodyAndAbort(AbortableInputStream responseBody) {
      setBody(responseBody);
      abort();
    }

    void clearBody(AbortableInputStream responseBody) {
      body.compareAndSet(responseBody, null);
    }

    void abort() {
      if (aborted.compareAndSet(false, true)) {
        AbortableInputStream responseBody = body.get();
        if (responseBody != null) {
          abortBody(responseBody);
        }
        try {
          request.abort();
        } catch (RuntimeException ignored) {
          // Best-effort transport cancellation.
        }
      }
    }

    private static void abortBody(AbortableInputStream responseBody) {
      try {
        responseBody.abort();
      } catch (RuntimeException ignored) {
        // Best-effort response cancellation.
      }
    }
  }

  private static final class AttemptThreadFactory implements ThreadFactory {
    private final AtomicInteger sequence = new AtomicInteger();

    @Override
    public Thread newThread(Runnable runnable) {
      Thread thread =
          new Thread(runnable, "alternator-bounded-poll-attempt-" + sequence.incrementAndGet());
      thread.setDaemon(true);
      return thread;
    }
  }
}
