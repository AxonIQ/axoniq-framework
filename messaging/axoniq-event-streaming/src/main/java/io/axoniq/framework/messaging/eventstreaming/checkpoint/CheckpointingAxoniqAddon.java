/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.eventstreaming.checkpoint;

import io.axoniq.license.entitlement.AxoniqAddon;

/**
 * {@link AxoniqAddon} implementation for the Axoniq Framework self-checkpointing event handlers. Allows the module to be
 * detected and logged at startup, and to be included in the license entitlement system.
 *
 * @author Allard Buijze
 * @since 5.3.0
 */
public class CheckpointingAxoniqAddon implements AxoniqAddon {

    static final String IDENTIFIER = "framework.checkpointing";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public String name() {
        return "Axoniq Framework - Self-Checkpointing Event Handlers";
    }
}
