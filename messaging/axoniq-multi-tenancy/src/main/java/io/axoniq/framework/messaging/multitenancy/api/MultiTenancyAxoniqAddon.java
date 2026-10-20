/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.multitenancy.api;

import io.axoniq.license.entitlement.AxoniqAddon;

/**
 * {@link AxoniqAddon} implementation for the Axoniq Framework Multi-tenancy. Allows the module to be
 * detected and logged at startup, and to be included in the license entitlement system.
 *
 * @author Mitchell Herrijgers
 * @since 5.3.0
 */
public class MultiTenancyAxoniqAddon implements AxoniqAddon {

    static final String IDENTIFIER = "framework.multi_tenancy";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public String name() {
        return "Axoniq Framework - Multi-tenancy";
    }
}
