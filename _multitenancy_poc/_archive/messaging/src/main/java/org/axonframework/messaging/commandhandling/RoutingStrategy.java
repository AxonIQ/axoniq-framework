/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */

package org.axonframework.messaging.commandhandling;

/**
 * Interface describing a mechanism that generates a routing key for a given command.
 * <p>
 * Commands that should be handled by the same segment, should result in the same routing key.
 *
 * @author Allard Buijze
 * @since 2.0.0
 */
public interface RoutingStrategy {

    /**
     * Generates a routing key for the given {@code command}.
     * <p>
     * Commands that should be handled by the same segment, should result in the same routing key.
     *
     * @param command The command to create a routing key for.
     * @return The routing key for the command.
     */
    String getRoutingKey(CommandMessage command);
}
