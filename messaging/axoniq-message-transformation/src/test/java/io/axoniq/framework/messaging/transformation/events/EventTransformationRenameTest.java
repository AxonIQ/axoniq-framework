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

package io.axoniq.framework.messaging.transformation.events;

import org.axonframework.messaging.core.MessageType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Contract of the {@code EventTransformation.rename(source, target)} factory entry point:
 * input validation today, full behavior once the rename implementation lands. The null
 * checks are permanent and continue to apply once the throws-stub is replaced.
 */
final class EventTransformationRenameTest {

    private static final MessageType COURSE_OPENED = new MessageType("com.example.CourseOpened", "1.0.0");
    private static final MessageType COURSE_CREATED = new MessageType("com.example.CourseCreated", "1.0.0");

    @Test
    void renameRejectsNullSource() {
        assertThatThrownBy(() -> EventTransformation.rename(null, COURSE_CREATED))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("source");
    }

    @Test
    void renameRejectsNullTarget() {
        assertThatThrownBy(() -> EventTransformation.rename(COURSE_OPENED, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("target");
    }

    @Test
    void renameThrowsUnsupportedOperationExceptionUntilTheRenameFactoryIsImplemented() {
        // Placeholder behavior. Delete this test when the rename factory lands; the null
        // checks above remain part of the implemented contract.
        assertThatThrownBy(() -> EventTransformation.rename(COURSE_OPENED, COURSE_CREATED))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("not yet implemented");
    }
}
