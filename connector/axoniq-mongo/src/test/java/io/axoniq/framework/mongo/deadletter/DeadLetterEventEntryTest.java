/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.mongo.deadletter;

import org.junit.jupiter.api.*;

import static org.assertj.core.api.Assertions.assertThat;

class DeadLetterEventEntryTest {

    @Test
    void equalsHashcodeAndToStringShouldWorkCorrectly() {
        // given
        DeadLetterEventEntry original = testEntry();

        // when
        DeadLetterEventEntry copy = new DeadLetterEventEntry(original.asDocument());

        // then
        assertThat(copy).isEqualTo(original);
        assertThat(copy.hashCode()).isEqualTo(original.hashCode());
        assertThat(copy.toString()).isEqualTo(original.toString());
    }

    static DeadLetterEventEntry testEntry() {
        return new DeadLetterEventEntry("type#1.0.0",
                                        "identifier",
                                        "2026-01-01T00:00:00Z",
                                        new byte[]{1, 2, 3},
                                        new byte[]{4, 5},
                                        "aggregateType",
                                        "aggregateIdentifier",
                                        42L,
                                        "tokenType",
                                        new byte[]{6});
    }
}
