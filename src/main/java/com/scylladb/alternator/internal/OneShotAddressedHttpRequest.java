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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.IDN;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpFullResponse;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;

/** A bounded, one-shot HTTP/1.1 GET connected to an explicitly selected address. */
final class OneShotAddressedHttpRequest implements ExecutableHttpRequest {

  interface Lifecycle {
    ScheduledFuture<?> activate(OneShotAddressedHttpRequest request, Runnable timeoutTask)
        throws IOException;

    void deactivate(OneShotAddressedHttpRequest request);
  }

  private static final int MAX_LINE_BYTES = 8 * 1024;
  private static final int MAX_INTERIM_RESPONSES = 8;
  private static final int BUFFER_SIZE = 8 * 1024;

  private final HttpExecuteRequest executeRequest;
  private final InetAddress address;
  private final SSLSocketFactory sslSocketFactory;
  private final boolean verifyHostname;
  private final LiveNodesPollingLimits limits;
  private final Lifecycle lifecycle;
  private final AtomicBoolean called = new AtomicBoolean(false);
  private final AtomicBoolean aborted = new AtomicBoolean(false);
  private final AtomicBoolean closedByClient = new AtomicBoolean(false);
  private final AtomicBoolean timedOut = new AtomicBoolean(false);
  private final AtomicReference<Socket> socket = new AtomicReference<>();

  OneShotAddressedHttpRequest(
      HttpExecuteRequest executeRequest,
      InetAddress address,
      SSLSocketFactory sslSocketFactory,
      boolean verifyHostname,
      LiveNodesPollingLimits limits,
      Lifecycle lifecycle) {
    this.executeRequest = Objects.requireNonNull(executeRequest, "executeRequest");
    this.address = Objects.requireNonNull(address, "address");
    this.sslSocketFactory = Objects.requireNonNull(sslSocketFactory, "sslSocketFactory");
    this.verifyHostname = verifyHostname;
    this.limits = Objects.requireNonNull(limits, "limits");
    this.lifecycle = Objects.requireNonNull(lifecycle, "lifecycle");
    validateRequest(executeRequest);
  }

  @Override
  public HttpExecuteResponse call() throws IOException {
    if (!called.compareAndSet(false, true)) {
      throw new IOException("Addressed polling request can only be executed once");
    }
    if (aborted.get()) {
      throw abortedException(null);
    }

    ScheduledFuture<?> timeoutTask = lifecycle.activate(this, this::timeOut);
    try {
      return execute();
    } catch (IOException e) {
      if (timedOut.get()) {
        SocketTimeoutException timeout =
            new SocketTimeoutException(
                "Addressed polling request exceeded " + limits.attemptTimeoutMillis + " ms");
        timeout.initCause(e);
        throw timeout;
      }
      if (aborted.get()) {
        throw abortedException(e);
      }
      throw e;
    } finally {
      timeoutTask.cancel(false);
      closeSocket();
      lifecycle.deactivate(this);
    }
  }

  @Override
  public void abort() {
    aborted.set(true);
    closeSocket();
  }

  void abortForClientClose() {
    closedByClient.set(true);
    abort();
  }

  private HttpExecuteResponse execute() throws IOException {
    SdkHttpRequest request = executeRequest.httpRequest();
    URI uri = request.getUri();
    String scheme = uri.getScheme().toLowerCase(Locale.ROOT);
    int port = effectivePort(uri, scheme);

    Socket rawSocket = new Socket();
    trackSocket(rawSocket);
    rawSocket.connect(new InetSocketAddress(address, port), limits.connectTimeoutMillis);
    rawSocket.setSoTimeout(limits.readTimeoutMillis);
    rawSocket.setTcpNoDelay(true);

    Socket connectedSocket = rawSocket;
    if ("https".equals(scheme)) {
      connectedSocket = createTlsSocket(rawSocket, uri.getHost(), port);
    }

    try (OutputStream output = new BufferedOutputStream(connectedSocket.getOutputStream());
        InputStream input = new BufferedInputStream(connectedSocket.getInputStream())) {
      writeRequest(output, request);
      output.flush();
      return readResponse(input);
    }
  }

  private SSLSocket createTlsSocket(Socket rawSocket, String logicalHost, int port)
      throws IOException {
    String tlsPeerName = LogicalHost.tlsPeerName(logicalHost);
    SSLSocket tlsSocket =
        (SSLSocket) sslSocketFactory.createSocket(rawSocket, tlsPeerName, port, true);
    if (!socket.compareAndSet(rawSocket, tlsSocket)) {
      closeQuietly(tlsSocket);
      throw abortedException(null);
    }
    if (aborted.get() || timedOut.get()) {
      closeSocket();
      throw abortedException(null);
    }

    SSLParameters parameters = tlsSocket.getSSLParameters();
    if (!LogicalHost.isIpLiteral(tlsPeerName)) {
      parameters.setServerNames(
          Collections.singletonList(new SNIHostName(IDN.toASCII(tlsPeerName))));
    }
    if (verifyHostname) {
      parameters.setEndpointIdentificationAlgorithm("HTTPS");
    }
    tlsSocket.setSSLParameters(parameters);
    tlsSocket.setSoTimeout(limits.readTimeoutMillis);
    tlsSocket.startHandshake();
    return tlsSocket;
  }

  private static void writeRequest(OutputStream output, SdkHttpRequest request) throws IOException {
    URI asciiUri = URI.create(request.getUri().toASCIIString());
    String rawPath = asciiUri.getRawPath();
    StringBuilder target = new StringBuilder(rawPath == null || rawPath.isEmpty() ? "/" : rawPath);
    if (asciiUri.getRawQuery() != null) {
      target.append('?').append(asciiUri.getRawQuery());
    }
    rejectLineBreaks(target.toString(), "request target");

    writeAscii(output, request.method().name() + " " + target + " HTTP/1.1\r\n");
    boolean hasHost = false;
    for (Map.Entry<String, List<String>> header : request.headers().entrySet()) {
      String name = header.getKey();
      validateHeaderName(name);
      if ("connection".equalsIgnoreCase(name)) {
        continue;
      }
      if ("host".equalsIgnoreCase(name)) {
        if (hasHost || header.getValue().size() != 1) {
          throw new IOException("Addressed polling request must contain one Host header");
        }
        hasHost = true;
      }
      for (String value : header.getValue()) {
        rejectLineBreaks(value, "header value");
        writeLatin1(output, name + ": " + value + "\r\n");
      }
    }
    if (!hasHost) {
      writeAscii(output, "Host: " + logicalAuthority(asciiUri) + "\r\n");
    }
    writeAscii(output, "Connection: close\r\n\r\n");
  }

  private HttpExecuteResponse readResponse(InputStream input) throws IOException {
    HeaderBudget headerBudget = new HeaderBudget(limits.maxHeaderBytes);
    int interimResponses = 0;
    ParsedStatus status;
    Map<String, List<String>> headers;
    while (true) {
      String statusLine = readLine(input, headerBudget);
      if (statusLine == null) {
        throw new EOFException("Server closed before sending an HTTP status line");
      }
      status = parseStatus(statusLine);
      headers = readHeaders(input, headerBudget);
      if (status.statusCode < 100 || status.statusCode >= 200) {
        break;
      }
      if (status.statusCode == 101) {
        throw new IOException("HTTP protocol switching is not supported for polling");
      }
      if (++interimResponses > MAX_INTERIM_RESPONSES) {
        throw new IOException(
            "HTTP response contains more than " + MAX_INTERIM_RESPONSES + " interim responses");
      }
    }
    // Polling callers only consume successful /localnodes bodies. Returning a non-success status
    // immediately lets the shared attempt wrapper abort and close the socket without draining an
    // arbitrarily slow or endless error body.
    byte[] body =
        status.statusCode == 200 ? readBody(input, status.statusCode, headers) : new byte[0];

    SdkHttpFullResponse response =
        SdkHttpFullResponse.builder()
            .statusCode(status.statusCode)
            .statusText(status.statusText)
            .headers(headers)
            .build();
    return HttpExecuteResponse.builder()
        .response(response)
        .responseBody(AbortableInputStream.create(new ByteArrayInputStream(body)))
        .build();
  }

  private Map<String, List<String>> readHeaders(InputStream input, HeaderBudget budget)
      throws IOException {
    Map<String, List<String>> headers = new LinkedHashMap<>();
    while (true) {
      String line = readLine(input, budget);
      if (line == null) {
        throw new EOFException("Server closed in HTTP response headers");
      }
      if (line.isEmpty()) {
        return headers;
      }
      if (line.charAt(0) == ' ' || line.charAt(0) == '\t') {
        throw new IOException("Obsolete folded HTTP headers are not supported");
      }
      int colon = line.indexOf(':');
      if (colon <= 0) {
        throw new IOException("Malformed HTTP response header");
      }
      String name = line.substring(0, colon);
      validateHeaderName(name);
      String value = trimOptionalWhitespace(line.substring(colon + 1));
      String existingName = findHeaderName(headers, name);
      String key = existingName == null ? name : existingName;
      headers.computeIfAbsent(key, ignored -> new ArrayList<>()).add(value);
    }
  }

  private byte[] readBody(InputStream input, int statusCode, Map<String, List<String>> headers)
      throws IOException {
    if ((statusCode >= 100 && statusCode < 200) || statusCode == 204 || statusCode == 304) {
      return new byte[0];
    }

    List<String> transferEncoding = headerValues(headers, "Transfer-Encoding");
    List<String> contentLength = headerValues(headers, "Content-Length");
    if (!transferEncoding.isEmpty()) {
      if (!contentLength.isEmpty()) {
        throw new IOException("HTTP response has both Transfer-Encoding and Content-Length");
      }
      List<String> codings = commaSeparatedTokens(transferEncoding);
      if (codings.size() != 1 || !"chunked".equalsIgnoreCase(codings.get(0))) {
        throw new IOException("Unsupported HTTP Transfer-Encoding: " + transferEncoding);
      }
      return readChunkedBody(input);
    }
    if (!contentLength.isEmpty()) {
      long length = parseContentLength(contentLength);
      if (length > limits.maxResponseBytes) {
        throw new IOException("HTTP response body exceeds " + limits.maxResponseBytes + " bytes");
      }
      return readExactly(input, (int) length);
    }
    return readUntilEnd(input, limits.maxResponseBytes);
  }

  private byte[] readChunkedBody(InputStream input) throws IOException {
    ByteArrayOutputStream body = new ByteArrayOutputStream();
    HeaderBudget framingBudget = new HeaderBudget(limits.maxHeaderBytes);
    while (true) {
      String sizeLine = readLine(input, framingBudget);
      if (sizeLine == null) {
        throw new EOFException("Server closed before the next HTTP chunk");
      }
      int extension = sizeLine.indexOf(';');
      String sizeToken = (extension >= 0 ? sizeLine.substring(0, extension) : sizeLine).trim();
      long chunkSize;
      try {
        chunkSize = Long.parseLong(sizeToken, 16);
      } catch (NumberFormatException e) {
        throw new IOException("Malformed HTTP chunk size", e);
      }
      if (chunkSize < 0 || chunkSize > limits.maxResponseBytes - body.size()) {
        throw new IOException("HTTP response body exceeds " + limits.maxResponseBytes + " bytes");
      }
      if (chunkSize == 0) {
        readTrailers(input, framingBudget);
        return body.toByteArray();
      }
      byte[] chunk = readExactly(input, (int) chunkSize);
      body.write(chunk, 0, chunk.length);
      requireCrlf(input);
    }
  }

  private static void readTrailers(InputStream input, HeaderBudget budget) throws IOException {
    while (true) {
      String trailer = readLine(input, budget);
      if (trailer == null) {
        throw new EOFException("Server closed in HTTP response trailers");
      }
      if (trailer.isEmpty()) {
        return;
      }
      int colon = trailer.indexOf(':');
      if (colon <= 0) {
        throw new IOException("Malformed HTTP response trailer");
      }
      validateHeaderName(trailer.substring(0, colon));
    }
  }

  private byte[] readUntilEnd(InputStream input, int maximum) throws IOException {
    ByteArrayOutputStream body = new ByteArrayOutputStream();
    byte[] buffer = new byte[BUFFER_SIZE];
    while (true) {
      int read = input.read(buffer);
      if (read < 0) {
        return body.toByteArray();
      }
      if (read > maximum - body.size()) {
        throw new IOException("HTTP response body exceeds " + maximum + " bytes");
      }
      body.write(buffer, 0, read);
    }
  }

  private static byte[] readExactly(InputStream input, int length) throws IOException {
    byte[] bytes = new byte[length];
    int offset = 0;
    while (offset < length) {
      int read = input.read(bytes, offset, length - offset);
      if (read < 0) {
        throw new EOFException("Server closed before the complete HTTP response body");
      }
      offset += read;
    }
    return bytes;
  }

  private static void requireCrlf(InputStream input) throws IOException {
    if (input.read() != '\r' || input.read() != '\n') {
      throw new IOException("Malformed HTTP chunk delimiter");
    }
  }

  private static String readLine(InputStream input, HeaderBudget budget) throws IOException {
    ByteArrayOutputStream line = new ByteArrayOutputStream();
    while (true) {
      int value = input.read();
      if (value < 0) {
        return line.size() == 0 ? null : failIncompleteLine();
      }
      budget.addByte();
      if (value == '\r') {
        int newline = input.read();
        if (newline < 0) {
          throw new EOFException("Server closed in an HTTP line ending");
        }
        budget.addByte();
        if (newline != '\n') {
          throw new IOException("HTTP lines must end with CRLF");
        }
        return new String(line.toByteArray(), StandardCharsets.ISO_8859_1);
      }
      if (value == '\n') {
        throw new IOException("HTTP lines must end with CRLF");
      }
      if (line.size() >= MAX_LINE_BYTES) {
        throw new IOException("HTTP response line exceeds " + MAX_LINE_BYTES + " bytes");
      }
      line.write(value);
    }
  }

  private static String failIncompleteLine() throws EOFException {
    throw new EOFException("Server closed in an HTTP response line");
  }

  private static ParsedStatus parseStatus(String line) throws IOException {
    int firstSpace = line.indexOf(' ');
    int secondSpace = firstSpace < 0 ? -1 : line.indexOf(' ', firstSpace + 1);
    String version = firstSpace < 0 ? "" : line.substring(0, firstSpace);
    String statusToken =
        firstSpace < 0
            ? ""
            : line.substring(firstSpace + 1, secondSpace < 0 ? line.length() : secondSpace);
    if (!("HTTP/1.0".equals(version) || "HTTP/1.1".equals(version)) || statusToken.length() != 3) {
      throw new IOException("Malformed HTTP status line");
    }
    int statusCode;
    try {
      statusCode = Integer.parseInt(statusToken);
    } catch (NumberFormatException e) {
      throw new IOException("Malformed HTTP status code", e);
    }
    if (statusCode < 100 || statusCode > 999) {
      throw new IOException("Invalid HTTP status code: " + statusCode);
    }
    String statusText = secondSpace < 0 ? "" : line.substring(secondSpace + 1);
    return new ParsedStatus(statusCode, statusText);
  }

  private static long parseContentLength(List<String> values) throws IOException {
    Long parsed = null;
    for (String token : commaSeparatedTokens(values)) {
      long value;
      try {
        value = Long.parseLong(token);
      } catch (NumberFormatException e) {
        throw new IOException("Malformed HTTP Content-Length", e);
      }
      if (value < 0 || (parsed != null && parsed.longValue() != value)) {
        throw new IOException("Conflicting or negative HTTP Content-Length");
      }
      parsed = value;
    }
    if (parsed == null) {
      throw new IOException("Empty HTTP Content-Length");
    }
    return parsed;
  }

  private static List<String> commaSeparatedTokens(List<String> values) throws IOException {
    List<String> tokens = new ArrayList<>();
    for (String value : values) {
      for (String token : value.split(",", -1)) {
        String trimmed = token.trim();
        if (trimmed.isEmpty()) {
          throw new IOException("Empty token in an HTTP response header");
        }
        tokens.add(trimmed);
      }
    }
    return tokens;
  }

  private static List<String> headerValues(Map<String, List<String>> headers, String name) {
    for (Map.Entry<String, List<String>> header : headers.entrySet()) {
      if (header.getKey().equalsIgnoreCase(name)) {
        return header.getValue();
      }
    }
    return Collections.emptyList();
  }

  private static String findHeaderName(Map<String, List<String>> headers, String name) {
    for (String existing : headers.keySet()) {
      if (existing.equalsIgnoreCase(name)) {
        return existing;
      }
    }
    return null;
  }

  private static String trimOptionalWhitespace(String value) {
    int start = 0;
    int end = value.length();
    while (start < end && (value.charAt(start) == ' ' || value.charAt(start) == '\t')) {
      start++;
    }
    while (end > start && (value.charAt(end - 1) == ' ' || value.charAt(end - 1) == '\t')) {
      end--;
    }
    return value.substring(start, end);
  }

  private static void validateHeaderName(String name) throws IOException {
    if (name == null || name.isEmpty()) {
      throw new IOException("HTTP header name is empty");
    }
    for (int i = 0; i < name.length(); i++) {
      char ch = name.charAt(i);
      if (!isTokenCharacter(ch)) {
        throw new IOException("Invalid character in HTTP header name");
      }
    }
  }

  private static boolean isTokenCharacter(char ch) {
    if (ch >= '0' && ch <= '9' || ch >= 'A' && ch <= 'Z' || ch >= 'a' && ch <= 'z') {
      return true;
    }
    return "!#$%&'*+-.^_`|~".indexOf(ch) >= 0;
  }

  private static void rejectLineBreaks(String value, String description) throws IOException {
    if (value == null || value.indexOf('\r') >= 0 || value.indexOf('\n') >= 0) {
      throw new IOException("Invalid " + description);
    }
  }

  private static void writeAscii(OutputStream output, String value) throws IOException {
    output.write(value.getBytes(StandardCharsets.US_ASCII));
  }

  private static void writeLatin1(OutputStream output, String value) throws IOException {
    output.write(value.getBytes(StandardCharsets.ISO_8859_1));
  }

  private static String logicalAuthority(URI uri) {
    return LogicalHost.authority(uri.getHost(), uri.getPort());
  }

  private static int effectivePort(URI uri, String scheme) throws IOException {
    if (uri.getPort() >= 0) {
      return uri.getPort();
    }
    if ("http".equals(scheme)) {
      return 80;
    }
    if ("https".equals(scheme)) {
      return 443;
    }
    throw new IOException("Unsupported addressed polling scheme: " + scheme);
  }

  private static void validateRequest(HttpExecuteRequest request) {
    SdkHttpRequest httpRequest = request.httpRequest();
    if (httpRequest.method() != SdkHttpMethod.GET) {
      throw new IllegalArgumentException("Addressed polling transport only supports GET");
    }
    if (request.contentStreamProvider().isPresent()) {
      throw new IllegalArgumentException("Addressed polling GET must not contain a request body");
    }
    String scheme = httpRequest.getUri().getScheme();
    if (!("http".equalsIgnoreCase(scheme) || "https".equalsIgnoreCase(scheme))) {
      throw new IllegalArgumentException("Addressed polling transport only supports HTTP(S)");
    }
    if (httpRequest.getUri().getHost() == null) {
      throw new IllegalArgumentException("Addressed polling request requires a logical hostname");
    }
  }

  private void trackSocket(Socket createdSocket) throws IOException {
    if (!socket.compareAndSet(null, createdSocket)) {
      closeQuietly(createdSocket);
      throw abortedException(null);
    }
    if (aborted.get() || timedOut.get()) {
      closeSocket();
      throw abortedException(null);
    }
  }

  private void timeOut() {
    timedOut.set(true);
    closeSocket();
  }

  private void closeSocket() {
    closeQuietly(socket.getAndSet(null));
  }

  private IOException abortedException(IOException cause) {
    IOException exception =
        new IOException(
            closedByClient.get()
                ? "Addressed polling request aborted because the HTTP client closed"
                : "Addressed polling request aborted");
    if (cause != null) {
      exception.initCause(cause);
    }
    return exception;
  }

  private static void closeQuietly(Socket socket) {
    if (socket != null) {
      try {
        socket.close();
      } catch (IOException ignored) {
        // Best-effort cancellation path.
      }
    }
  }

  private static final class HeaderBudget {
    private final int maximum;
    private int consumed;

    HeaderBudget(int maximum) {
      this.maximum = maximum;
    }

    void addByte() throws IOException {
      if (++consumed > maximum) {
        throw new IOException("HTTP response headers exceed " + maximum + " bytes");
      }
    }
  }

  private static final class ParsedStatus {
    final int statusCode;
    final String statusText;

    ParsedStatus(int statusCode, String statusText) {
      this.statusCode = statusCode;
      this.statusText = statusText;
    }
  }
}
