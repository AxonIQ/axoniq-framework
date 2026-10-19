/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.testcontainer;

import org.junit.jupiter.api.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests {@link AxonServerContainer}'s actual startup/reuse behavior against a real container. Pure builder/
 * configuration logic is covered without Docker by {@link AxonServerContainerTest}.
 *
 * @author Lucas Campos
 * @author Steven van Beelen
 */
@Tag("nightly")
@Tag("slow")
@Tag("flaky")
class AxonServerContainerIT {

    @Test
    void constructionWithReuseEnabledStartsMultipleTimesAsExpected() {
        try (AxonServerContainer testSubject = new AxonServerContainer().withReuse(true)) {

            testSubject.doStart();
            assertThat(testSubject.isRunning()).isTrue();

            testSubject.doStart();
            assertThat(testSubject.isRunning()).isTrue();
        }
    }
}
