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

import org.jspecify.annotations.Nullable;

import io.axoniq.workflow.runtime.api.payload.PayloadReducer;

import java.util.HashMap;
import java.util.Map;

/**
 * Combines two payloads into one. Takes all values from the first payload (workflow context) and adds the values of the
 * second (local), overwriting any duplicates. Usage of this reducer as a parameter reducer allows accessing all
 * workflow context variables directly. Usage of this reducer as a result reducer writes all results back into the
 * workflow context.
 *
 * @author Simon Zambrovski
 * @since 0.2.0
 */
public class CombineGlobalAndLocalPayloadReducer implements PayloadReducer {

    /**
     * Name of {@link CombineGlobalAndLocalPayloadReducer} reducer.
     */
    public static final String NAME = "combine_local_and_global";

    /**
     * Singleton instance of {@link CombineGlobalAndLocalPayloadReducer}.
     */
    public static final CombineGlobalAndLocalPayloadReducer INSTANCE = new CombineGlobalAndLocalPayloadReducer();

    @Override
    public Map<String, @Nullable Object> apply(Map<String, @Nullable Object> global,
                                     Map<String, @Nullable Object> local) {
        var result = new HashMap<>(global);
        result.putAll(local);
        return result;
    }

    @Override
    public String name() {
        return NAME;
    }
}
