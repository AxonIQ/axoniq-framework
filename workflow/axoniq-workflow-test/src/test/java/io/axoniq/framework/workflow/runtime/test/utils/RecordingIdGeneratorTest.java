/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.test.utils;

import org.junit.jupiter.api.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link RecordingIdGenerator}.
 *
 * @author Simon Zambrovski
 */
class RecordingIdGeneratorTest {

    @Test
    void nextRecordsGeneratedIds() {
        RecordingIdGenerator generator = new RecordingIdGenerator();

        String first = generator.next();
        String second = generator.next();

        assertThat(generator.getHistory()).containsExactly(first, second);
    }

    @Test
    void historyIsReadOnlyAndClearable() {
        RecordingIdGenerator generator = new RecordingIdGenerator();
        generator.next();

        assertThatThrownBy(() -> generator.getHistory().add("extra"))
                .isInstanceOf(UnsupportedOperationException.class);

        generator.clear();

        assertThat(generator.getHistory()).isEmpty();
    }
}
