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

package org.axonframework.common.infra;

import org.jspecify.annotations.NonNull;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A mock {@link ComponentDescriptor} implementation, used for testing.
 *
 * @author Mitchll Herrijgers
 */
public class MockComponentDescriptor implements ComponentDescriptor {

    private final Map<String, Object> properties = new ConcurrentHashMap<>();

    /**
     * Returns all described properties.
     *
     * @return All described properties.
     */
    public Map<String, Object> getDescribedProperties() {
        return properties;
    }

    /**
     * Returns a described property matching the given {@code name}, or {@code null} if this property does not exist.
     *
     * @param name The name for which to retrieve a described property.
     * @param <R>  The expected type of the described property.
     * @return The property described with the given {@code name}, or {@code null} if this property does not exist.
     */
    public <R> R getProperty(String name) {
        //noinspection unchecked
        return (R) properties.get(name);
    }

    @Override
    public void describeProperty(@NonNull String name, Object object) {
        properties.put(name, object);
    }

    @Override
    public void describeProperty(@NonNull String name, Collection<?> collection) {
        properties.put(name, collection);
    }

    @Override
    public void describeProperty(@NonNull String name, Map<?, ?> map) {
        properties.put(name, map);
    }

    @Override
    public void describeProperty(@NonNull String name, String value) {
        properties.put(name, value);
    }

    @Override
    public void describeProperty(@NonNull String name, Long value) {
        properties.put(name, value);
    }

    @Override
    public void describeProperty(@NonNull String name, Boolean value) {
        properties.put(name, value);
    }

    @Override
    public String describe() {
        throw new UnsupportedOperationException("This mock Component Descriptor cannot describe itself.");
    }
}
