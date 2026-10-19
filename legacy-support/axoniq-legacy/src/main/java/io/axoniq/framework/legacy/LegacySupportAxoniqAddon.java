/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.legacy;

import io.axoniq.license.entitlement.AxoniqAddon;

/**
 * {@link AxoniqAddon} implementation for the Axoniq Framework Legacy Support module. Allows the module to be detected
 * and logged at startup, and to be included in the license entitlement system.
 * <p>
 * A single addon covers the entire module, which ports Axon Framework 4 solutions such as sagas and deadlines. It is
 * registered with the {@link io.axoniq.license.entitlement.EntitlementManager} whenever an
 * {@link org.axonframework.modelling.saga.AbstractSagaManager} or an
 * {@link org.axonframework.deadline.AbstractDeadlineManager} is constructed.
 *
 * @author Mateusz Nowak
 * @since 5.4.0
 */
public class LegacySupportAxoniqAddon implements AxoniqAddon {

    static final String IDENTIFIER = "framework.legacy_support";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public String name() {
        return "Axoniq Framework - Legacy Support";
    }
}
