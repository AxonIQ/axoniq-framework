/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.mongo.deadletter;

import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.messaging.core.Metadata;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class DeadLetterEntryTest {

    @Test
    void equalsHashcodeAndToStringShouldWorkCorrectly() {
        // given
        DeadLetterEntry original = testEntry();

        // when
        DeadLetterEntry copy = new DeadLetterEntry(original.asDocument());

        // then
        assertThat(copy).isEqualTo(original);
        assertThat(copy.hashCode()).isEqualTo(original.hashCode());
        assertThat(copy.toString()).isEqualTo(original.toString());
    }

    static DeadLetterEntry testEntry() {
        return new DeadLetterEntry(
                "processingGroup",
                "seqIdentifier",
                0L,
                DeadLetterEventEntryTest.testEntry().asDocument(),
                Instant.now().minus(Duration.ofMinutes(3L)),
                Instant.now(),
                null,
                Metadata.emptyInstance(),
                new JacksonConverter()
        );
    }
}
