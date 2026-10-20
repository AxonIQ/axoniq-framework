/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.springcloud.query;

import org.jspecify.annotations.Nullable;

import java.util.Objects;

/**
 * A single event read from a Server-Sent Events stream.
 *
 * @param id    the event's identifier, or {@code null} when the event carried none. A member reconnecting to a stream
 *              reports the last identifier it saw, letting the serving member resume where it left off.
 * @param event the event's type, naming what the {@code data} holds. Defaults to {@link #DEFAULT_EVENT_TYPE} when the
 *              stream did not name one, as the protocol prescribes.
 * @param data  the event's payload. Multiple {@code data} lines in one event are joined with newlines, so this is the
 *              payload as the sending member wrote it.
 * @author Allard Buijze
 * @since 5.4.0
 */
public record ServerSentEvent(@Nullable String id, String event, String data) {

    /**
     * The event type of an event that did not name one.
     */
    public static final String DEFAULT_EVENT_TYPE = "message";

    /**
     * Compact constructor rejecting a missing {@code event} or {@code data}.
     */
    public ServerSentEvent {
        Objects.requireNonNull(event, "The event must not be null.");
        Objects.requireNonNull(data, "The data must not be null.");
    }

}
