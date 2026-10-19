/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.springcloud.util;

import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.http.client.ClientHttpRequest;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.mock.http.client.MockClientHttpRequest;
import org.springframework.mock.http.client.MockClientHttpResponse;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.function.Supplier;

/**
 * A {@link ClientHttpRequestFactory} for tests, answering each request with the next canned response a test queued and
 * recording what was sent.
 * <p>
 * Lets a {@link org.springframework.web.client.RestClient} be exercised against exact status codes, headers and bodies
 * — including the ones that are awkward to provoke against a real server, such as a connection failure or a
 * {@code 304 Not Modified} — without standing up a server.
 *
 * @author Allard Buijze
 */
public class StubClientHttpRequestFactory implements ClientHttpRequestFactory {

    private final Deque<Supplier<ClientHttpResponse>> responses = new ArrayDeque<>();
    private final List<MockClientHttpRequest> requests = new ArrayList<>();

    /**
     * Queues a {@code 200 OK} response carrying the given {@code json} body and {@code ETag}.
     */
    public StubClientHttpRequestFactory respondingWith(String json, String eTag) {
        responses.add(() -> {
            MockClientHttpResponse response =
                    new MockClientHttpResponse(json.getBytes(StandardCharsets.UTF_8), HttpStatus.OK);
            response.getHeaders().setContentType(MediaType.APPLICATION_JSON);
            if (eTag != null) {
                response.getHeaders().setETag(eTag);
            }
            return response;
        });
        return this;
    }

    /**
     * Queues a {@code 304 Not Modified} response, as an unchanged member answers a conditional request with.
     */
    public StubClientHttpRequestFactory respondingNotModified() {
        responses.add(() -> new MockClientHttpResponse(new byte[0], HttpStatus.NOT_MODIFIED));
        return this;
    }

    /**
     * Queues a response with the given {@code status} and an empty body.
     */
    public StubClientHttpRequestFactory respondingWithStatus(HttpStatusCode status) {
        responses.add(() -> new MockClientHttpResponse(new byte[0], status));
        return this;
    }

    /**
     * Queues a connection failure, as an instance that is down produces.
     */
    public StubClientHttpRequestFactory failingToConnect() {
        responses.add(() -> {
            throw new UncheckedIOException(new IOException("Connection refused"));
        });
        return this;
    }

    public List<MockClientHttpRequest> requests() {
        return List.copyOf(requests);
    }

    public MockClientHttpRequest lastRequest() {
        if (requests.isEmpty()) {
            throw new IllegalStateException("No request was made.");
        }
        return requests.get(requests.size() - 1);
    }

    @Override
    public ClientHttpRequest createRequest(URI uri, HttpMethod httpMethod) {
        MockClientHttpRequest request = new MockClientHttpRequest(httpMethod, uri) {
            @Override
            protected ClientHttpResponse executeInternal() throws IOException {
                Supplier<ClientHttpResponse> next = responses.poll();
                if (next == null) {
                    throw new IllegalStateException("No response was queued for [" + httpMethod + " " + uri + "].");
                }
                try {
                    return next.get();
                } catch (UncheckedIOException e) {
                    throw e.getCause();
                }
            }
        };
        requests.add(request);
        return request;
    }

    /**
     * Carries an {@link IOException} out of a response supplier, which cannot declare one.
     */
    private static class UncheckedIOException extends RuntimeException {

        private UncheckedIOException(IOException cause) {
            super(cause);
        }

        @Override
        public synchronized IOException getCause() {
            return (IOException) super.getCause();
        }
    }
}
