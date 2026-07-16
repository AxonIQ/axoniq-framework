/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */
package io.axoniq.workflow.runtime.test.utils;

import org.junit.jupiter.api.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link RecordingIdGenerator}.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
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
