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

import com.fasterxml.jackson.annotation.JsonFormat;
import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.Objects;

/**
 * The wire representation of a failure that ended a query's response stream.
 * <p>
 * A stream that ends without one of these completed normally, however many responses it carried.
 *
 * @param requestIdentifier    the identifier of the query that failed
 * @param errorCode            what kind of failure this is, which decides the exception the dispatching member
 *                             raises, or {@code null} when the answering member reported a kind this one does not
 *                             recognise
 * @param errorMessage         the failure's message
 * @param errorDetails         the messages of the failure and its causes, in order, so that the dispatching member can
 *                             report where a failure originated without needing the classes involved.
 * @param errorOrigin          the name of the member the failure occurred on
 * @param errorDetailsType     the runtime class name of the application-specific details, or {@code null} when the
 *                             failure carried none. Described the same way on the command side, so that a member
 *                             reading either kind of failure reads one thing
 * @param errorDetailsPayload  the application-specific details, Base64-encoded, or {@code null} when the failure
 *                             carried none
 * @author Allard Buijze
 * @since 5.4.0
 */
public record QueryDispatchFailure(
        String requestIdentifier,
        // A code added by a later version of the connector reads as null rather than failing the whole event, so
        // that the failure it reports still reaches the query that caused it. Declared here rather than configured
        // on a converter, so that it holds however this event is read.
        @JsonFormat(with = JsonFormat.Feature.READ_UNKNOWN_ENUM_VALUES_AS_NULL)
        @Nullable QueryErrorCode errorCode,
        @Nullable String errorMessage,
        List<String> errorDetails,
        @Nullable String errorOrigin,
        @Nullable String errorDetailsType,
        @Nullable String errorDetailsPayload
) {

    /**
     * Compact constructor requiring a {@code requestIdentifier}, and defaulting {@code null} error details to an
     * empty list so a failure reported by a member that omits the field still reads.
     */
    @SuppressWarnings("MissingJavadoc")
    public QueryDispatchFailure {
        Objects.requireNonNull(requestIdentifier, "The request identifier must not be null.");
        errorDetails = errorDetails == null ? List.of() : List.copyOf(errorDetails);
    }
}
