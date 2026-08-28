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

import org.axonframework.messaging.commandhandling.CommandResultMessage;
import org.axonframework.messaging.core.MessageType;
import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The wire representation of the outcome of handling a
 * {@link org.axonframework.messaging.commandhandling.CommandMessage command} on another member of the cluster.
 * <p>
 * A reply carries exactly one of three outcomes, distinguished without ambiguity:
 * <ul>
 *     <li>a result, with {@link #type()} and possibly {@link #payload()} set and {@link #errorCode()}
 *     {@code null};</li>
 *     <li>no result at all — the handler returned nothing — with both {@link #type()} and {@link #errorCode()}
 *     {@code null};</li>
 *     <li>a failure, with {@link #errorCode()} set.</li>
 * </ul>
 * A failure is reported as a {@code 200 OK} response carrying this reply, not as an HTTP error status. The command was
 * delivered and handled; that its handler threw is an application outcome, and conflating it with a transport failure
 * would lose the distinction the dispatching member needs to decide whether retrying can help.
 *
 * @param identifier        The identifier of this reply message.
 * @param requestIdentifier The {@link CommandDispatchRequest#identifier() identifier} of the command this replies to.
 * @param type              The {@link MessageType#toString() string form} of the result's
 *                          {@link CommandResultMessage#type() type}, or {@code null} when there is no result.
 * @param payload           The Base64-encoded {@code byte[]} payload of the result, or {@code null} when there is
 *                          none.
 * @param metadata          The {@link CommandResultMessage#metadata() metadata} of the result.
 * @param errorCode         The kind of failure that occurred, or {@code null} when handling succeeded.
 * @param errorMessage      A description of the failure, or {@code null} when handling succeeded.
 * @param errorDetails      The messages found down the failure's cause chain, outermost first, for diagnostics. Empty
 *                          when handling succeeded.
 * @param errorOrigin       The name of the member the failure originated on, or {@code null} when handling succeeded.
 * @param errorDetailsType  The runtime class name of the application-specific details carried by the failure, or
 *                          {@code null} when it carries none. Present for wire-level observability only; the
 *                          receiving member reconstructs details as the type its own code asks for.
 * @param errorDetailsPayload The Base64-encoded serialized application-specific details carried by the failure, or
 *                          {@code null} when it carries none.
 * @author Allard Buijze
 * @since 5.4.0
 */
public record CommandDispatchReply(
        String identifier,
        String requestIdentifier,
        @Nullable String type,
        @Nullable String payload,
        Map<String, @Nullable String> metadata,
        @Nullable CommandErrorCode errorCode,
        @Nullable String errorMessage,
        List<String> errorDetails,
        @Nullable String errorOrigin,
        @Nullable String errorDetailsType,
        @Nullable String errorDetailsPayload
) {

    /**
     * Compact constructor requiring both identifiers, and defaulting {@code null} metadata and error details to empty
     * so a reply from a member that omits either field still reads.
     */
    @SuppressWarnings("MissingJavadoc")
    public CommandDispatchReply {
        Objects.requireNonNull(identifier, "The reply identifier cannot be null.");
        Objects.requireNonNull(requestIdentifier, "The request identifier cannot be null.");
        metadata = metadata == null ? Map.of() : Map.copyOf(metadata);
        errorDetails = errorDetails == null ? List.of() : List.copyOf(errorDetails);
    }

    /**
     * Indicates whether this reply reports a failure.
     *
     * @return {@code true} when handling the command failed, {@code false} when it succeeded
     */
    public boolean isError() {
        return errorCode != null;
    }
}
