/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution.payload;

import io.axoniq.framework.workflow.dsl.api.PayloadReducer;
import org.jspecify.annotations.Nullable;

import java.util.Map;

/**
 * Simple {@code PayloadReducer} that will only pass along the {@code local} payload, ignoring the {@code global}
 * payload, without modification.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
public class LocalOnlyPayloadReducer implements PayloadReducer {

    /**
     * Name of {@link LocalOnlyPayloadReducer} reducer.
     */
    public static final String NAME = "local_only";

    /**
     * Singleton instance of {@link LocalOnlyPayloadReducer}.
     */
    public static final LocalOnlyPayloadReducer INSTANCE = new LocalOnlyPayloadReducer();

    @Override
    public Map<String, @Nullable Object> apply(Map<String, @Nullable Object> global,
                                               Map<String, @Nullable Object> local) {
        return local;
    }

    @Override
    public String name() {
        return NAME;
    }
}
