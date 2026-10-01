/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */

package io.axoniq.framework.springcloud.query;

import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A {@link QueryResponseSink} recording what was written to it, standing in for a response stream to another member.
 *
 * @author Allard Buijze
 */
public class RecordingQueryResponseSink implements QueryResponseSink {

    private final List<QueryDispatchResponse> responses = new CopyOnWriteArrayList<>();
    private final List<QueryDispatchResponse> updates = new CopyOnWriteArrayList<>();

    private final List<Runnable> unavailableListeners = new CopyOnWriteArrayList<>();

    private volatile @Nullable QueryDispatchFailure error;
    private volatile boolean completed;
    private volatile @Nullable String subscriptionCompletedFor;
    private volatile @Nullable RuntimeException failOnWrite;

    /**
     * Makes every write fail with the given {@code cause}, as writing to a member that stopped reading does.
     */
    public RecordingQueryResponseSink failingOnWriteWith(RuntimeException cause) {
        this.failOnWrite = cause;
        return this;
    }

    /**
     * Reports that this sink can no longer be written to, as the container does when the member that asked stopped
     * reading or the response stream outlived its timeout.
     */
    public void becomeUnavailable() {
        unavailableListeners.forEach(Runnable::run);
    }

    public List<QueryDispatchResponse> responses() {
        return List.copyOf(responses);
    }

    public @Nullable QueryDispatchFailure error() {
        return error;
    }

    public boolean completed() {
        return completed;
    }

    @Override
    public void onUnavailable(Runnable listener) {
        unavailableListeners.add(listener);
    }

    @Override
    public void update(QueryDispatchResponse update) {
        RuntimeException failure = failOnWrite;
        if (failure != null) {
            throw failure;
        }
        updates.add(update);
    }

    public List<QueryDispatchResponse> updates() {
        return List.copyOf(updates);
    }

    @Override
    public void response(QueryDispatchResponse response) {
        RuntimeException failure = failOnWrite;
        if (failure != null) {
            throw failure;
        }
        responses.add(response);
    }

    @Override
    public void error(QueryDispatchFailure error) {
        this.error = error;
    }

    @Override
    public void complete() {
        this.completed = true;
    }

    @Override
    public void subscriptionComplete(String requestIdentifier) {
        this.subscriptionCompletedFor = requestIdentifier;
        this.completed = true;
    }

    /**
     * Returns the identifier of the subscription query reported over, or {@code null} when none was.
     */
    public @Nullable String subscriptionCompletedFor() {
        return subscriptionCompletedFor;
    }
}
