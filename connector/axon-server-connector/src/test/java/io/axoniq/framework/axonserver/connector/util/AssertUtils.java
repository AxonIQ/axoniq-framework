/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.axonserver.connector.util;

import java.util.concurrent.TimeUnit;

/**
 * Utility class for special assertions
 *
 * @author Sara Pellegrini
 * @author Steven van Beelen
 */
public final class AssertUtils {

    private AssertUtils() {
        // Utility class
    }

    /**
     * Assert that the given {@code assertion} succeeds with the given {@code time} and {@code unit}.
     *
     * @param time      an {@code int} which paired with the {@code unit} specifies the time in which the assertion must
     *                  pass
     * @param unit      a {@link TimeUnit} in which {@code time} is expressed
     * @param assertion a {@link Runnable} containing the assertion to succeed within the deadline
     */
    @SuppressWarnings("Duplicates")
    public static void assertWithin(int time, TimeUnit unit, Runnable assertion) {
        long now = System.currentTimeMillis();
        long deadline = now + unit.toMillis(time);
        do {
            try {
                assertion.run();
                break;
            } catch (AssertionError e) {
                if (now >= deadline) {
                    throw e;
                }
            }
            Thread.yield();
            now = System.currentTimeMillis();
        } while (true);
    }
}
