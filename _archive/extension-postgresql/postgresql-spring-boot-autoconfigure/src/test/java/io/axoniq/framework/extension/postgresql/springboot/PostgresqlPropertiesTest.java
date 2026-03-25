/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */

package io.axoniq.framework.extension.postgresql.springboot;

import org.junit.jupiter.api.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test class validating the {@link PostgresqlProperties}.
 *
 * @author Steven van Beelen
 */
class PostgresqlPropertiesTest {

    private PostgresqlProperties testSubject;

    @BeforeEach
    void setUp() {
        testSubject = new PostgresqlProperties();
    }

    @Test
    void isEnabledDefaultsToTrue() {
        assertThat(testSubject.isEnabled()).isTrue();
    }

    @Test
    void isEnabledReturnsFalseWhenSetToFalse() {
        testSubject.setEnabled(false);

        assertThat(testSubject.isEnabled()).isFalse();
    }
}