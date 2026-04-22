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

package io.axoniq.framework.axonserver.connector.configuration;

import io.grpc.ManagedChannelBuilder;

import java.util.function.UnaryOperator;

/**
 * Customizer to add more customizations to a managed channel to Axon Server.
 *
 * @author Marc Gathier
 * @since 4.4.3
 */
@FunctionalInterface
public interface ManagedChannelCustomizer extends UnaryOperator<ManagedChannelBuilder<?>> {

    /**
     * Returns a no-op {@link ManagedChannelCustomizer}.
     *
     * @return a no-op {@link ManagedChannelCustomizer}
     */
    static ManagedChannelCustomizer identity() {
        return c -> c;
    }
}
