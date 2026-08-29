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

package io.axoniq.framework.springcloud.transport;

import org.jspecify.annotations.Nullable;

import java.util.Map;
import java.util.Objects;

/**
 * The wire representation of a query sent to another member of the cluster.
 * <p>
 * A query carries no routing key: unlike a command, it asks nothing of where it is handled, so any member advertising
 * its name may answer it.
 *
 * @param identifier the query's identifier, which the answering member reports back on every response
 * @param type       the query's {@code MessageType}, as its string form
 * @param payload    the query's payload as its converter wrote it, or {@code null} when the query carries none
 * @param metadata   the query's metadata
 * @param priority   the query's priority, or {@code null} when it declares none
 * @author Allard Buijze
 * @since 5.4.0
 */
public record QueryDispatchRequest(
        String identifier,
        String type,
        @Nullable String payload,
        Map<String, @Nullable String> metadata,
        @Nullable Integer priority
) {

    /**
     * Compact constructor requiring an {@code identifier} and {@code type}, and defaulting {@code null} metadata to
     * empty so a request from a member that omits the field still reads.
     */
    @SuppressWarnings("MissingJavadoc")
    public QueryDispatchRequest {
        Objects.requireNonNull(identifier, "The query identifier must not be null.");
        Objects.requireNonNull(type, "The query type must not be null.");
        // WireCodec rather than Map.copyOf: metadata permits null values, which Map.copyOf rejects.
        metadata = WireCodec.copyOf(metadata);
    }
}
