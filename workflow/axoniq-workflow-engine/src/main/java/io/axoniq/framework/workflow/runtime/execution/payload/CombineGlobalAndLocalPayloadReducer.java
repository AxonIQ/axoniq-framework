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

import java.util.HashMap;
import java.util.Map;

/**
 * Combines two payloads into one. Takes all values from the first payload (workflow context) and adds the values of the
 * second (local), overwriting any duplicates. Usage of this reducer as a parameter reducer allows accessing all
 * workflow context variables directly. Usage of this reducer as a result reducer writes all results back into the
 * workflow context.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
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
