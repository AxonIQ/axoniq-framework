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

import jakarta.annotation.Nonnull;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
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
     * Name of {@link #COMBINE} reducer.
     */
    String NAME_COMBINE = "combine";
    /**
     * Name of {@link #CONTEXT} reducer.
     */
    String NAME_CONTEXT = "context";
    /**
     * Name of {@link #LOCAL} reducer.
     */
    String NAME_LOCAL = "local";
    /**
     * Combines two payloads into one. Takes all values from the first payload and adds the values of the second,
     * overwriting any duplicates. Usage of this reducer as a parameter reducer allows accessing all workflow context
     * variables directly. Usage of this reducer as a result reducer writes all results back into the workflow context.
     */
    PayloadReducer COMBINE = (context, local) -> {
        var result = new HashMap<>(context);
        result.putAll(local);
        return result;
    };

    /**
     * Simple {@code PayloadReducer} that will only pass along the `context` payload, ignoring the `local` payload, without modification.
     */
    PayloadReducer CONTEXT = (context, local) -> context;


    /**
     * Simple {@code PayloadReducer} that will only pass along the `local` payload, ignoring the `context` payload, without modification.
     */
    PayloadReducer LOCAL = (context, local) -> local;

    /**
     * Constructs standard reducer by name.
     *
     * @param name reducer name.
     * @return payload reducer.
     */
    static PayloadReducer byName(@Nonnull String name) {
        return switch (Objects.requireNonNull(name, "Reducer name must not be null")) {
            case NAME_COMBINE -> COMBINE;
            case NAME_CONTEXT -> CONTEXT;
            case NAME_LOCAL -> LOCAL;
            default -> throw new IllegalArgumentException("Unknown reducer name: " + name);
        };
    }

    /**
     * Returns a name of the reducer.
     *
     * @param reducer reducer to get name for.
     * @return reducer name.
     */
    static String name(@Nonnull PayloadReducer reducer) {
        if (COMBINE == reducer) {
            return NAME_COMBINE;
        } else if (CONTEXT == reducer) {
            return NAME_CONTEXT;
        } else if (LOCAL == reducer) {
            return NAME_LOCAL;
        } else {
            throw new IllegalArgumentException("Unknown reducer: " + reducer);
        }
    }

    /**
     * Checks if a standard reducer is used.
     *
     * @param reducer reducer to check.
     * @return true, if a standard reducer is used.
     */
    static boolean isDefault(@Nonnull PayloadReducer reducer) {
        return CONTEXT == reducer || LOCAL == reducer || COMBINE == reducer;
    }

    /**
     * Checks if a standard reducer name is used.
     *
     * @param name name of the reducer.
     * @return true, if a standard reducer is used.
     */
    static boolean isDefault(@Nonnull String name) {
        return NAME_CONTEXT.equals(name) || NAME_LOCAL.equals(name) || NAME_COMBINE.equals(name);
    }
}
