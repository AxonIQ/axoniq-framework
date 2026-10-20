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
 * Simple {@code PayloadReducer} that will only pass along the {@code global} payload, ignoring the {@code local}
 * payload, without modification.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
public class GlobalOnlyPayloadReducer implements PayloadReducer {

    /**
     * Name of {@link GlobalOnlyPayloadReducer} reducer.
     */
    public static final String NAME = "global_only";

    /**
     * Singleton instance of {@link GlobalOnlyPayloadReducer}.
     */
    public static final GlobalOnlyPayloadReducer INSTANCE = new GlobalOnlyPayloadReducer();

    @Override
    public Map<String, @Nullable Object> apply(Map<String, @Nullable Object> global,
                                               Map<String, @Nullable Object> local) {
        return global;
    }

    @Override
    public String name() {
        return NAME;
    }
}
