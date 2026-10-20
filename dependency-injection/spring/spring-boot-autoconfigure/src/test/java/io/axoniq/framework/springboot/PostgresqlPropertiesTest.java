/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.springboot;

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