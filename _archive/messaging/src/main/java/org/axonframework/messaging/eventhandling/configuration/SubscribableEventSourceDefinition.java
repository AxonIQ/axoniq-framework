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

package org.axonframework.messaging.eventhandling.configuration;

import org.axonframework.common.configuration.Configuration;
import org.axonframework.messaging.core.SubscribableEventSource;

/**
 * Definition for a {@link SubscribableEventSource}.
 *
 * @author Marc Gathier
 * @since 4.10.0
 */
public interface SubscribableEventSourceDefinition {

    /**
     * Creates a {@link SubscribableEventSource} based on this definition and the provided configuration.
     *
     * @param configuration The Axon {@link Configuration} to base the {@link SubscribableEventSource} on.
     * @return A {@link SubscribableEventSource} based on this definition and the provided configuration.
     */
    SubscribableEventSource create(Configuration configuration);
}
