/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.test.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * Simple sleep utilities.
 *
 * @author Simon Zambrovski
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public class SleepUtils {

    private static final long PROGRESS_INTERVAL_MILLIS = 200;

    static Logger logger = LoggerFactory.getLogger(SleepUtils.class);

    /**
     * Waits for the requested duration and logs progress every 200 milliseconds.
     *
     * @param millis total timeout in milliseconds
     */
    public static void waitWithProgress(long millis) {
        try {
            for (long elapsed = 0; elapsed < millis; elapsed += PROGRESS_INTERVAL_MILLIS) {
                logger.info("Waiting for {} / {} millis.", elapsed, millis);
                Thread.sleep(Math.min(PROGRESS_INTERVAL_MILLIS, millis - elapsed));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    /**
     * Takes a nap.
     *
     * @param millis thread sleep timeout.
     */
    public static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Takes a nap.
     *
     * @param duration timeout to sleep.
     */
    public static void sleepQuietly(Duration duration) {
        sleepQuietly(duration.toMillis());
    }
}
