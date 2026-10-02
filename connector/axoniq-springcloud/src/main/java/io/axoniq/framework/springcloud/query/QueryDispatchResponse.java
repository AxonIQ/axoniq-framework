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

import org.axonframework.messaging.core.Metadata;
import org.jspecify.annotations.Nullable;

import java.util.Map;
import java.util.Objects;

/**
 * The wire representation of one response to a query, carried by a single event of the response stream.
 * <p>
 * A query may be answered with any number of these, which is why they arrive as a stream rather than as one reply.
 *
 * @param identifier        the response's own identifier
 * @param requestIdentifier the identifier of the query being answered
 * @param type              the response's {@code MessageType}, as its string form
 * @param payload           the response's payload as its converter wrote it, or {@code null} when it carries none
 * @param metadata          the response's metadata
 * @author Allard Buijze
 * @since 5.4.0
 */
public record QueryDispatchResponse(
        String identifier,
        String requestIdentifier,
        String type,
        @Nullable String payload,
        Map<String, @Nullable String> metadata
) {

    /**
     * Compact constructor requiring an {@code identifier}, {@code requestIdentifier} and {@code type}, and defaulting
     * {@code null} metadata to empty so a response from a member that omits the field still reads.
     */
    @SuppressWarnings("MissingJavadoc")
    public QueryDispatchResponse {
        Objects.requireNonNull(identifier, "The response identifier must not be null.");
        Objects.requireNonNull(requestIdentifier, "The request identifier must not be null.");
        Objects.requireNonNull(type, "The response type must not be null.");
        // Metadata is immutable and permits the null values metadata allows, and is what a Message already
        // carries: no copy of our own, and none at all on the way out.
        metadata = Metadata.from(metadata);
    }
}
