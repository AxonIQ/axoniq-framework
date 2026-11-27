/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.mongo.deadletter;

import org.bson.Document;
import org.junit.jupiter.api.*;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InstantEntryTest {

    @Test
    void throwsWhenDocumentKeysDoNotMatch() {
        // given
        Document document = new Document();

        // when / then
        assertThatThrownBy(() -> new InstantEntry(document)).isInstanceOf(NotAnInstantDocumentException.class);
    }

    @Test
    void instantConvertedToDocumentAndBackIsTheSame() {
        // given
        Instant given = Instant.now();

        // when
        Document asDocument = new InstantEntry(given).asDocument();

        // then
        assertThat(new InstantEntry(asDocument).getInstant()).isEqualTo(given);
    }
}
