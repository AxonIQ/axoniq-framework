/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.messaging.eventhandling.conversion.DelegatingEventConverter;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;

/**
 * Test utility for {@link EventConverter}.
 */
public class TestEventConverter {

    public static final EventConverter INSTANCE = new DelegatingEventConverter(new JacksonConverter());

    private TestEventConverter() {
        // test util
    }
}
