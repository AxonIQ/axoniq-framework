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
package io.axoniq.framework.workflow.runtime.execution.payload;

import org.jspecify.annotations.Nullable;

import io.axoniq.framework.workflow.runtime.api.payload.PayloadReducer;

import java.util.Map;

/**
 * Simple {@code PayloadReducer} that will only pass along the {@code global} payload, ignoring the {@code local} payload, without
 * modification.
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
