/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
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
