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

package org.axonframework.modelling.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Utility methods related to testing concurency
 */
public abstract class ConcurrencyUtils {

    /**
     * Will execute the runnable as many times as the given {@code threadCount} and assert none caused exceptions. The
     * used runnable should finish fast, not longer than a few seconds.
     *
     * @param threadCount the number of times of invocations at the 'same' time
     * @param runnable    something that is expected to be run concurrently
     */
    public static void testConcurrent(int threadCount, Runnable runnable) {
        ExecutorService service = Executors.newFixedThreadPool(threadCount);
        AtomicInteger successCounter = new AtomicInteger();
        List<Exception> exceptions = new ArrayList<>();
        for (int i = 0; i < threadCount; i++) {
            service.submit(() -> {
                try {
                    runnable.run();
                    successCounter.incrementAndGet();
                } catch (Exception e) {
                    exceptions.add(e);
                }
            });
        }
        service.shutdown();
        try {
            service.awaitTermination(5L, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            exceptions.add(e);
        }
        assertEquals(Collections.emptyList(), exceptions);
        assertEquals(threadCount, successCounter.get(), "Not all threads have completed successfully");
    }

    private ConcurrencyUtils() {

    }
}
