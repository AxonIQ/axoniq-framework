/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.test.utils;

import org.axonframework.common.annotation.Internal;

import java.util.UUID;

/**
 * Id generator.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
@Internal
public interface IdGenerator {

    /**
     * Generates a new unique identifier.
     *
     * @return next id
     */
    default String next() {
        return UUID.randomUUID().toString();
    }
}
