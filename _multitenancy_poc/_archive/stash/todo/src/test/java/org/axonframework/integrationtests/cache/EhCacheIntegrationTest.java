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

package org.axonframework.integrationtests.cache;

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
 * {@link Ehcache} specific implementation of the {@link CachingIntegrationTestSuite}.
 *
 * @author Steven van Beelen
 */
@Disabled("TODO #3727")
class EhCacheIntegrationTest extends CachingIntegrationTestSuite {

    private CacheManager cacheManager;

    @Override
    @BeforeEach
    void setUp() {
        Map<String, CacheConfiguration<?, ?>> caches = new HashMap<>();
        DefaultConfiguration config = new DefaultConfiguration(caches, null);
        cacheManager = new EhcacheManager(config);
        cacheManager.init();
        super.setUp();
    }

    @AfterEach
    void tearDown() {
        cacheManager.close();
    }

    @Override
    public Cache buildCache(String name) {
        return new EhCacheAdapter(createCache(name));
    }

    private Ehcache<Object, Object> createCache(String name) {
        return (Ehcache<Object, Object>) cacheManager.createCache(
                name,
                CacheConfigurationBuilder
                        .newCacheConfigurationBuilder(
                                Object.class,
                                Object.class,
                                ResourcePoolsBuilder.heap(100L).build()
                        )
                        .build());
    }
}
