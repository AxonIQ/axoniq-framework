/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.eventhandling.deadletter;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SequencedDeadLetterAxoniqAddonTest {

    private final SequencedDeadLetterAxoniqAddon addon = new SequencedDeadLetterAxoniqAddon();

    @Test
    void identifierReturnsExpectedValue() {
        assertThat(addon.identifier()).isEqualTo("framework.dead_letter_queue");
    }

    @Test
    void nameReturnsExpectedValue() {
        assertThat(addon.name()).isEqualTo("Axoniq Framework - Sequenced Dead-Letter Queue");
    }
}
