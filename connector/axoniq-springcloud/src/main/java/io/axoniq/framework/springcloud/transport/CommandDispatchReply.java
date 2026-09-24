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
import org.axonframework.messaging.core.Metadata;
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
 * @param identifier        the identifier of this reply message
 * @param requestIdentifier the {@link CommandDispatchRequest#identifier() identifier} of the command this replies to
 * @param type              the {@link MessageType#toString() string form} of the result's
 *                          {@link CommandResultMessage#type() type}, or {@code null} when there is no result
 * @param payload           the payload of the result as text, or {@code null} when there is
 *                          none
 * @param metadata          the {@link CommandResultMessage#metadata() metadata} of the result
 * @param errorCode         the kind of failure that occurred, or {@code null} when handling succeeded
 * @param errorMessage      a description of the failure, or {@code null} when handling succeeded
 * @param errorDetails      the messages found down the failure's cause chain, outermost first, for diagnostics. Empty
 *                          when handling succeeded.
 * @param errorOrigin       the name of the member the failure originated on, or {@code null} when handling succeeded
 * @param errorDetailsType  the runtime class name of the application-specific details carried by the failure, or
 *                          {@code null} when it carries none. Present for wire-level observability only; the
 *                          receiving member reconstructs details as the type its own code asks for.
 * @param errorDetailsPayload the Base64-encoded serialized application-specific details carried by the failure, or
 *                          {@code null} when it carries none
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
     * @param errorCode         the kind of failure that occurred, or {@code null} when handling succeeded
     * @param errorMessage      a description of the failure, or {@code null} when handling succeeded
     * @param errorDetails      the messages found down the failure's cause chain, outermost first, for diagnostics. Empty when handling succeeded.
     * @param errorOrigin       the name of the member the failure originated on, or {@code null} when handling succeeded
     * @param errorDetailsPayload the Base64-encoded serialized application-specific details carried by the failure, or
     * @param errorDetailsType the runtime class name of the application-specific details carried by the failure, or
     * @param identifier       the identifier of this reply message
     * @param metadata          the {@link CommandResultMessage#metadata() metadata} of the result
     * @param payload           the payload of the result as text, or {@code null}
     * @param requestIdentifier the {@link CommandDispatchRequest#identifier() identifier} of the command this replies to
     * @param type              the {@link MessageType#toString() string form} of
     */
    public CommandDispatchReply {
        Objects.requireNonNull(identifier, "The reply identifier must not be null.");
        Objects.requireNonNull(requestIdentifier, "The request identifier must not be null.");
        // Metadata is immutable and permits the null values metadata allows, and is what a Message already
        // carries: no copy of our own, and none at all on the way out.
        metadata = Metadata.from(metadata);
        errorDetails = errorDetails == null ? List.of() : List.copyOf(errorDetails);
    }

    /**
     * Constructs a {@code CommandDispatchReply} carrying the successful outcome of a command.
     *
     * @param identifier        the identifier of the result
     * @param requestIdentifier the identifier of the command this is a reply to
     * @param type              the {@link MessageType#toString() string form} of the result's type, or {@code null}
     *                          when the handler produced no result
     * @param payload           the payload of the result as text, or {@code null} when there is
     *                          none
     * @param metadata          the metadata of the result
     * @return a reply carrying the successful outcome of a command
     */
    public static CommandDispatchReply result(String identifier,
                                              String requestIdentifier,
                                              @Nullable String type,
                                              @Nullable String payload,
                                              Map<String, @Nullable String> metadata) {
        return new CommandDispatchReply(identifier, requestIdentifier, type, payload, metadata,
                                        null, null, List.of(), null, null, null);
    }

    /**
     * Constructs a {@code CommandDispatchReply} reporting that the handler produced no result at all.
     *
     * @param identifier        the identifier of the reply
     * @param requestIdentifier the identifier of the command this is a reply to
     * @return a reply reporting that the handler produced no result
     */
    public static CommandDispatchReply noResult(String identifier, String requestIdentifier) {
        return new CommandDispatchReply(identifier, requestIdentifier, null, null, Map.of(),
                                        null, null, List.of(), null, null, null);
    }

    /**
     * Constructs a {@code CommandDispatchReply} reporting that handling the command failed.
     *
     * @param identifier          the identifier of the reply
     * @param requestIdentifier   the identifier of the command this is a reply to
     * @param errorCode           the kind of failure being reported
     * @param errorMessage        the message describing the failure
     * @param errorOrigin         the name of the member reporting the failure
     * @param errorDetails        the descriptions making up the failure's cause chain
     * @param errorDetailsType    the class name of the application-specific details, or {@code null} when there are
     *                            none
     * @param errorDetailsPayload the Base64-encoded application-specific details, or {@code null} when there are none
     * @return a reply reporting that handling the command failed
     */
    public static CommandDispatchReply error(String identifier,
                                             String requestIdentifier,
                                             CommandErrorCode errorCode,
                                             @Nullable String errorMessage,
                                             @Nullable String errorOrigin,
                                             List<String> errorDetails,
                                             @Nullable String errorDetailsType,
                                             @Nullable String errorDetailsPayload) {
        return new CommandDispatchReply(identifier, requestIdentifier, null, null, Map.of(),
                                        errorCode, errorMessage, errorDetails, errorOrigin,
                                        errorDetailsType, errorDetailsPayload);
    }

    /**
     * Indicates whether this reply reports a failure.
     * <p>
     * Deliberately not named {@code isError}: this is derived from {@link #errorCode()} rather than a field of the
     * wire format, and a bean-style name would have a serialization library write it as one.
     *
     * @return {@code true} when handling the command failed, {@code false} when it succeeded
     */
    public boolean hasError() {
        return errorCode != null;
    }
}
