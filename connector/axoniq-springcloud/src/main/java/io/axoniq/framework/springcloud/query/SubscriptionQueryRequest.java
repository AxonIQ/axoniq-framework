/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.springcloud.query;

import org.axonframework.messaging.core.Metadata;
import org.jspecify.annotations.Nullable;

import java.util.Map;
import java.util.Objects;

/**
 * A subscription query as one member sends it to another.
 * <p>
 * Carries the same fields as a {@link QueryDispatchRequest}, plus the one thing only a subscription needs: how many
 * updates the answering member may hold for this subscriber.
 * <p>
 * Nothing here says anything about the initial result. That is a query like any other, asked for over the query
 * endpoint once every member has registered its subscription.
 *
 * @param identifier        the identifier of the query being sent, which every response to it carries back
 * @param type              the {@link org.axonframework.messaging.core.MessageType} of the query, as its string form
 * @param payload           the query's payload as its converter wrote it, or {@code null} when it carries none
 * @param metadata          the query's metadata
 * @param priority          the priority the query was dispatched with, or {@code null} when it carried none
 * @param updateBufferSize  how many updates the answering member may hold for this subscriber before failing it
 * @author Allard Buijze
 * @since 5.4.0
 */
public record SubscriptionQueryRequest(
        String identifier,
        String type,
        @Nullable String payload,
        Map<String, @Nullable String> metadata,
        @Nullable Integer priority,
        int updateBufferSize
) {

    /**
     * Compact constructor requiring an {@code identifier} and {@code type}, and defaulting {@code null} metadata to
     * empty so a request from a member that omits the field still reads.
     */
    @SuppressWarnings("MissingJavadoc")
    public SubscriptionQueryRequest {
        Objects.requireNonNull(identifier, "The identifier must not be null.");
        Objects.requireNonNull(type, "The type must not be null.");
        // Metadata is immutable and permits the null values metadata allows, and is what a Message already
        // carries: no copy of our own, and none at all on the way out.
        metadata = Metadata.from(metadata);
    }
}
