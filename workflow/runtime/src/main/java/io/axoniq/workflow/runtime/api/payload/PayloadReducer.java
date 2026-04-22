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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
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
     * Name of {@link #COMBINE_GLOBAL_AND_LOCAL} reducer.
     */
    String NAME_COMBINE_LOCAL_AND_GLOBAL = "combine_local_and_global";
    /**
     * Name of {@link #GLOBAL_ONLY} reducer.
     */
    String NAME_GLOBAL_ONLY = "global_only";
    /**
     * Name of {@link #LOCAL_ONLY} reducer.
     */
    String NAME_LOCAL_ONLY = "local_only";
    /**
     * Combines two payloads into one. Takes all values from the first payload (workflow context) and adds the values of
     * the second (local), overwriting any duplicates. Usage of this reducer as a parameter reducer allows accessing all
     * workflow context variables directly. Usage of this reducer as a result reducer writes all results back into the
     * workflow context.
     */
    PayloadReducer COMBINE_GLOBAL_AND_LOCAL = (global, local) -> {
        var result = new HashMap<>(global);
        result.putAll(local);
        return result;
    };

    /**
     * Simple {@code PayloadReducer} that will only pass along the `global` payload, ignoring the `local` payload,
     * without modification.
     */
    PayloadReducer GLOBAL_ONLY = (global, local) -> global;


    /**
     * Simple {@code PayloadReducer} that will only pass along the `local` payload, ignoring the `global` payload,
     * without modification.
     */
    PayloadReducer LOCAL_ONLY = (global, local) -> local;

    /**
     * Constructs standard reducer by name.
     *
     * @param name reducer name.
     * @return payload reducer.
     */
    static PayloadReducer byName(@Nonnull String name) {
        return switch (Objects.requireNonNull(name, "Reducer name must not be null")) {
            case NAME_COMBINE_LOCAL_AND_GLOBAL -> COMBINE_GLOBAL_AND_LOCAL;
            case NAME_GLOBAL_ONLY -> GLOBAL_ONLY;
            case NAME_LOCAL_ONLY -> LOCAL_ONLY;
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
        if (COMBINE_GLOBAL_AND_LOCAL == reducer) {
            return NAME_COMBINE_LOCAL_AND_GLOBAL;
        } else if (GLOBAL_ONLY == reducer) {
            return NAME_GLOBAL_ONLY;
        } else if (LOCAL_ONLY == reducer) {
            return NAME_LOCAL_ONLY;
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
        return GLOBAL_ONLY == reducer || LOCAL_ONLY == reducer || COMBINE_GLOBAL_AND_LOCAL == reducer;
    }

    /**
     * Checks if a standard reducer name is used.
     *
     * @param name name of the reducer.
     * @return true, if a standard reducer is used.
     */
    static boolean isDefault(@Nonnull String name) {
        return NAME_GLOBAL_ONLY.equals(name) || NAME_LOCAL_ONLY.equals(name) || NAME_COMBINE_LOCAL_AND_GLOBAL.equals(
                name);
    }
}
