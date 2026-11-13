/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.postgresql;

import io.axoniq.license.entitlement.AxoniqAddon;

/**
 * AxoniqAddon implementation for the Axon Framework PostgreSQL Extension. This allows the extension to be detected
 * and logged at startup, and to be included in the license entitlement system.
 *
 * @author Mitchell Herrijgers
 * @since 1.0.0
 */
public class PostgresAxoniqAddon implements AxoniqAddon {
    static final String IDENTIFIER = "framework.postgres";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public String name() {
        return "Axon Framework PostgreSQL Extension";
    }
}
