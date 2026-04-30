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
package io.axoniq.workflow.runtime.api.payload;

import jakarta.annotation.Nonnull;
import org.axonframework.common.annotation.Internal;

import java.util.Map;
import java.util.function.BiFunction;

/**
 * Payload reducer to combine two payloads into one. In general, there is a global payload (part of the state of the
 * workflow instance) and a local payload (part of the step execution). On the step invocation, the global and local
 * form the invocation parameters. After the step execution, the local result any global for, the resulting workflow
 * instance payload.
 *
 * @author Simon Zambrovski
 * @author Stefan Dragisic
 * @since 1.0.0
 */
@Internal
@FunctionalInterface
public interface PayloadReducer extends BiFunction<Map<String, Object>, Map<String, Object>, Map<String, Object>> {

    /**
     * Retrieves the name of the reducer.
     *
     * @return reducer name.
     */
    @Nonnull
    default String name() {
        return this.getClass().getName();
    }
}
