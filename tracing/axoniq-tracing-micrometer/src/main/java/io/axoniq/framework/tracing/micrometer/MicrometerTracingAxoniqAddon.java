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

package io.axoniq.framework.tracing.micrometer;

import io.axoniq.license.entitlement.AxoniqAddon;

/**
 * {@link AxoniqAddon} implementation for the Axoniq Framework Micrometer distributed tracing binding. Allows the module
 * to be detected and logged at startup, and to be included in the license entitlement system.
 * <p>
 * The identifier covers distributed tracing as a capability rather than the Micrometer binding specifically, so that
 * every way of producing traces is entitled through a single addon. The name stays binding-specific, as it identifies
 * which module registered the addon in the start-up log.
 *
 * @author Mateusz Nowak
 * @since 5.3.0
 */
public class MicrometerTracingAxoniqAddon implements AxoniqAddon {

    static final String IDENTIFIER = "framework.distributed_tracing";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public String name() {
        return "Axoniq Framework - Distributed Tracing (Micrometer)";
    }
}
