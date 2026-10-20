/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.test.utils;

import org.junit.jupiter.api.*;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link IdGenerator}.
 *
 * @author Simon Zambrovski
 */
class IdGeneratorTest {

    @Test
    void nextGeneratesUuidString() {
        IdGenerator generator = new IdGenerator() {
        };

        String id = generator.next();

        assertThat(UUID.fromString(id).toString()).isEqualTo(id);
    }
}
