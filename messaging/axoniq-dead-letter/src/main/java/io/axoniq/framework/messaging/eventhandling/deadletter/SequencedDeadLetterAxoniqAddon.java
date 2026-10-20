/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.eventhandling.deadletter;

import io.axoniq.license.entitlement.AxoniqAddon;

/**
 * {@link AxoniqAddon} implementation for the Axoniq Framework Sequenced Dead-Letter Queue. Allows the module to be
 * detected and logged at startup, and to be included in the license entitlement system.
 *
 * @author Mateusz Nowak
 * @since 5.1.0
 */
public class SequencedDeadLetterAxoniqAddon implements AxoniqAddon {

    static final String IDENTIFIER = "framework.dead_letter_queue";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public String name() {
        return "Axoniq Framework - Sequenced Dead-Letter Queue";
    }
}
