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
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */

package org.axonframework.messaging.core;

import java.util.Map;

/**
 * Implementation that represents an empty Context.
 * @since 5.0.0
 * @author Allard Buijze
 */
class EmptyContext implements Context {

    /**
     * Returns the singleton instance of the empty context.
     */
    public static final EmptyContext INSTANCE = new EmptyContext();

    private EmptyContext() {
    }

    @Override
    public boolean containsResource(ResourceKey<?> key) {
        return false;
    }

    @Override
    public <T> T getResource(ResourceKey<T> key) {
        return null;
    }

    @Override
    public <T> Context withResource(ResourceKey<T> key, T resource) {
        return Context.with(key, resource);
    }

    @Override
    public Map<ResourceKey<?>, Object> resources() {
        return Map.of();
    }
}
