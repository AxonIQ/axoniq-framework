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

package io.axoniq.framework.kafka.common;

import io.axoniq.license.entitlement.AxoniqAddon;
import org.axonframework.common.annotation.Internal;

/**
 * The {@link AxoniqAddon} that identifies the Kafka connector, so that it is detected and logged at start-up and
 * accounted for by the license entitlement system.
 * <p>
 * Discoverable through {@code META-INF/services/io.axoniq.license.entitlement.AxoniqAddon}.
 * <p>
 * Internal, because it only exists for the {@link java.util.ServiceLoader} and the entitlement system; applications
 * never use it directly.
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
@Internal
public class KafkaAxoniqAddon implements AxoniqAddon {

    /**
     * The identifier this addon is claimed under.
     */
    @SuppressWarnings("java:S1845") // ignore potential naming conflict with identifier()
    public static final String IDENTIFIER = "framework.kafka";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public String name() {
        return "Axoniq Framework - Kafka";
    }
}
