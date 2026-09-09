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

import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.MessageType;
import org.jspecify.annotations.Nullable;

import java.util.Map;
import java.util.Objects;

/**
 * The wire representation of a {@link CommandMessage} being sent to another member of the cluster.
 * <p>
 * Fields mirror what the command bus needs on the far side to reconstruct an equivalent message: the identifier, the
 * {@link MessageType type}, the payload, the metadata, and the routing key and priority the sending member resolved.
 * The routing key travels along even though the receiving member has already been selected, because a handler may read
 * it, and the priority travels along because the receiving member's own priority executor orders work by it.
 * <p>
 * The payload arrives here already written as text by the {@code PayloadConvertingCommandBusConnector} wrapped
 * around the connector, and travels as it stands.
 *
 * @param identifier the {@link CommandMessage#identifier() identifier} of the command
 * @param type       the {@link MessageType#toString() string form} of the command's {@link CommandMessage#type()
 *                   type}, carrying both qualified name and version.
 * @param payload    the command's payload as its converter wrote it, or {@code null} when it has none
 * @param metadata   the {@link CommandMessage#metadata() metadata} of the command
 * @param routingKey the {@link CommandMessage#routingKey() routing key} of the command, or {@code null} when it has
 *                   none
 * @param priority   the {@link CommandMessage#priority() priority} of the command, or {@code null} when it has none
 * @author Allard Buijze
 * @since 5.4.0
 */
public record CommandDispatchRequest(
        String identifier,
        String type,
        @Nullable String payload,
        Map<String, @Nullable String> metadata,
        @Nullable String routingKey,
        @Nullable Integer priority
) {

    /**
     * Compact constructor requiring an {@code identifier} and {@code type}, and defaulting {@code null} metadata to
     * empty so a request from a member that omits the field still reads.
     */
    public CommandDispatchRequest {
        Objects.requireNonNull(identifier, "The command identifier must not be null.");
        Objects.requireNonNull(type, "The command type must not be null.");
        // Metadata is immutable and permits the null values metadata allows, and is what a Message already
        // carries: no copy of our own, and none at all on the way out.
        metadata = Metadata.from(metadata);
    }
}
