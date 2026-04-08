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
import org.junit.jupiter.api.Test;

import javax.cache.CacheException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;

/**
 * @author Allard Buijze
 */
class NoCacheTest {

    @Test
    void cacheDoesNothing() throws CacheException {
        // this is pretty stupid, but we're testing that it does absolutely nothing
        NoCache cache = NoCache.INSTANCE;
        Registration registration = cache.registerCacheEntryListener(mock(Cache.EntryListener.class));
        assertFalse(cache.containsKey(new Object()));
        assertNull(cache.get(new Object()));
        cache.put(new Object(), new Object());
        Map<Object, Object> map = new HashMap<>();
        map.put(new Object(), new Object());
        assertFalse(cache.remove(new Object()));
        registration.cancel();
    }
}
