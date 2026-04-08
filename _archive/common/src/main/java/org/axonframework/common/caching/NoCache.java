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

package org.axonframework.common.caching;

import org.axonframework.common.Registration;

import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * Cache implementation that does absolutely nothing. Objects aren't cached, making it a special case implementation for
 * the case when caching is disabled.
 *
 * @author Allard Buijze
 * @since 0.3
 */
public final class NoCache implements Cache {

    /**
     * Creates a singleton reference the NoCache implementation.
     */
    public static final NoCache INSTANCE = new NoCache();

    private NoCache() {
    }

    @Override
    public <K, V> V get(K key) {
        return null;
    }

    @Override
    public void put(Object key, Object value) {
    }

    @Override
    public boolean putIfAbsent(Object key, Object value) {
        return true;
    }

    @Override
    public <T> T computeIfAbsent(Object key, Supplier<T> valueSupplier) {
        return valueSupplier.get();
    }

    @Override
    public boolean remove(Object key) {
        return false;
    }

    @Override
    public void removeAll() {
        // Do nothing
    }

    @Override
    public boolean containsKey(Object key) {
        return false;
    }

    @Override
    public Registration registerCacheEntryListener(EntryListener cacheEntryListener) {
        return () -> true;
    }

    @Override
    public <V> void computeIfPresent(Object key, UnaryOperator<V> update) {
        // Do nothing
    }
}
