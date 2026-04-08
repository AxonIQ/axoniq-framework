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

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.ExpiryPolicyBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.core.Ehcache;
import org.ehcache.core.EhcacheManager;
import org.ehcache.core.config.DefaultConfiguration;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Test class validating the {@link EhCacheAdapter}.
 *
 * @author Steven van Beelen
 * @author Gerard Klijs
 */
class EhCacheAdapterTest extends AbstractCacheAdapterTest {

    @Override
    @SuppressWarnings("rawtypes")
    AbstractCacheAdapterTest.TestSubjectWrapper getTestSubjectWrapper() {
        Map<String, CacheConfiguration<?, ?>> caches = new HashMap<>();
        DefaultConfiguration config = new DefaultConfiguration(caches, null);
        CacheManager cacheManager = new EhcacheManager(config);
        cacheManager.init();
        Cache cache = cacheManager
                .createCache(
                        "test",
                        CacheConfigurationBuilder
                                .newCacheConfigurationBuilder(
                                        Object.class,
                                        Object.class,
                                        ResourcePoolsBuilder.heap(10L).build())
                                .withExpiry(ExpiryPolicyBuilder.timeToIdleExpiration(Duration.ofMillis(500L)))
                                .build());
        EhCacheAdapter testSubject = new EhCacheAdapter((Ehcache) cache);
        return new TestSubjectWrapper(testSubject, cacheManager::close);
    }
}