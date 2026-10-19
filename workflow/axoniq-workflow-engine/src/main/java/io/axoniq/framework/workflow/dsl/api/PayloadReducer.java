/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.dsl.api;

import org.jspecify.annotations.Nullable;


import java.util.Map;
import java.util.function.BiFunction;

/**
 * Payload reducer to combine two payloads into one. In general, there is a global payload (part of the state of the
 * workflow instance) and a local payload (part of the step execution). On the step invocation, the global and local
 * form the invocation parameters. After the step execution, the local result and global payload form the resulting
 * workflow instance payload.
 *
 * @author Simon Zambrovski
 * @author Stefan Dragisic
 * @since 5.4.0
 */
@FunctionalInterface
public interface PayloadReducer extends
        BiFunction<Map<String, @Nullable Object>, Map<String, @Nullable Object>, Map<String, @Nullable Object>> {

    /**
     * Retrieves the name of the reducer.
     *
     * @return reducer name.
     */
    default String name() {
        return this.getClass().getName();
    }
}
