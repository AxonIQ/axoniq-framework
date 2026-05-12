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
package io.axoniq.workflow.runtime.execution.payload;

import io.axoniq.workflow.runtime.api.payload.PayloadReducer;
import jakarta.annotation.Nonnull;
import org.jspecify.annotations.NonNull;

import java.util.Map;

/**
 * Simple {@code PayloadReducer} that will only pass along the {@code local} payload, ignoring the {@code global}
 * payload, without modification.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
public class LocalOnlyPayloadReducer implements PayloadReducer {

    /**
     * Name of {@link LocalOnlyPayloadReducer} reducer.
     */
    public static final String NAME = "local_only";

    @Override
    public Map<String, Object> apply(@NonNull Map<String, Object> global,
                                     @Nonnull Map<String, Object> local) {
        return local;
    }

    @Override
    public @NonNull String name() {
        return NAME;
    }
}
