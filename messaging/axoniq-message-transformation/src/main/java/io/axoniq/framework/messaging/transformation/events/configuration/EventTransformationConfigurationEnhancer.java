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

package io.axoniq.framework.messaging.transformation.events.configuration;

import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;

/**
 * ServiceLoader-discovered {@link ConfigurationEnhancer} that installs the
 * {@code TransformingEventStore} decorator. Reads the user-supplied
 * {@code EventTransformerChain} and the active {@code MessageConverter} from the
 * {@code Configuration} at decorator-registration time.
 *
 * @author Laura Devriendt
 * @since 5.2.0
 */
public final class EventTransformationConfigurationEnhancer implements ConfigurationEnhancer {

    @Override
    public void enhance(ComponentRegistry registry) {
        // Tests-first stub: no-op. The registerDecorator call lands with T032.
    }
}
