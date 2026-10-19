/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package org.axonframework.modelling.saga.repository;

import org.axonframework.common.caching.Cache;
import org.axonframework.common.caching.EhCacheAdapter;
import org.ehcache.CacheManager;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.core.Ehcache;
import org.ehcache.core.EhcacheManager;
import org.ehcache.core.config.DefaultConfiguration;
import org.junit.jupiter.api.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Concrete implementation of the {@link CachingSagaStoreTest} using the {@link EhCacheAdapter}.
 *
 * @author Steven van Beelen
 * @author Gerard Klijs
 */
class EhCacheSagaStoreTest extends CachingSagaStoreTest {

    private CacheManager cacheManager;
    private org.ehcache.core.Ehcache ehCache;

    @AfterEach
    void tearDown() {
        cacheManager.close();
    }

    @Override
    Cache sagaCache() {
        if (ehCache == null) {
            buildEhCache();
        }
        return new EhCacheAdapter(ehCache);
    }

    @Override
    Cache associationCache() {
        if (ehCache == null) {
            buildEhCache();
        }
        return new EhCacheAdapter(ehCache);
    }

    private void buildEhCache() {
        Map<String, CacheConfiguration<?, ?>> caches = new HashMap<>();
        DefaultConfiguration config = new DefaultConfiguration(caches, null);
        cacheManager = new EhcacheManager(config);
        cacheManager.init();
        ehCache = (Ehcache) cacheManager
                .createCache(
                        "test",
                        CacheConfigurationBuilder
                                .newCacheConfigurationBuilder(
                                        Object.class,
                                        Object.class,
                                        ResourcePoolsBuilder.heap(100L).build())
                                .build());
    }
}
