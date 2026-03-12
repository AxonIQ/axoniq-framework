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
 *    https://lp.axoniq.io/axoniq-software-subscription-agreement-terms
 *
 *
 */
package io.axoniq.workflow.runtime.api;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * Payload reducer to combine two payloads into one.
 *
 * @author Simon Zambrovski
 * @author Stefan Dragisic
 * @since 1.0.0
 */
@FunctionalInterface
public interface PayloadReducer extends BiFunction<Map<String, Object>, Map<String, Object>, Map<String, Object>> {

    /**
     * Combine two payloads into one. Takes all values from the first payload and adds the values of the second,
     * overwriting any duplicates.
     *
     * @return result payload.
     */
    static PayloadReducer all() {
        return (context, local) -> {
            var result = new HashMap<>(context);
            result.putAll(local);
            return result;
        };
    }

    /**
     * Combine two payloads into one. Takes only value from the first (context) payload.
     *
     * @return result payload.
     */
    static PayloadReducer context() {
        return (context, local) -> context;
    }

    /**
     * Combine two payloads into one. Takes only value from the second (local) payload.
     *
     * @return result payload.
     */
    static PayloadReducer local() {
        return (context, local) -> local;
    }

    /**
     * Combine two payloads into one. Takes no values.
     *
     * @return empty payload.
     */
    static PayloadReducer none() {
        return (global, local) -> Map.of();
    }
}
