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

import org.axonframework.common.AxonConfigurationException;
import org.axonframework.messaging.core.Metadata;

/**
 * A {@link RoutingStrategy} implementation that uses the value in the {@link Metadata} of a
 * {@link CommandMessage} assigned to a given key.
 * <p>
 * The value's {@code toString()} is used to convert the {@code Metadata} value to a String.
 *
 * @author Allard Buijze
 * @since 2.0.0
 */
public class MetadataRoutingStrategy implements RoutingStrategy {

    private final String metadataKey;

    /**
     * Instantiate a {@code MetadataRoutingStrategy} based on the fields contained in the give {@code builder}.
     * <p>
     * Will assert that the {@code metadataKey} is not an empty {@link String} or {@code null} and will throw an
     * {@link AxonConfigurationException} if this is the case.
     *
     * @param metadataKey The key in the {@link Metadata} to use for routing.
     */
    public MetadataRoutingStrategy(String metadataKey) {
        this.metadataKey = metadataKey;
    }

    @Override
    public String getRoutingKey(CommandMessage command) {
        Object value = command.metadata().get(metadataKey);
        return value == null ? null : value.toString();
    }
}
