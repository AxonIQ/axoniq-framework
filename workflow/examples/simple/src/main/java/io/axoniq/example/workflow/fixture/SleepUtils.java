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
package io.axoniq.example.workflow.fixture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class SleepUtils {

    static Logger logger = LoggerFactory.getLogger(SleepUtils.class);

    public static void waitWithProgress(long millis) {
        try {
            for (long i = 0; i < millis; i = i + 200) {
                Thread.sleep(i);
                logger.info("Waiting for {} / {} millis.", i, millis);
            }
        } catch (InterruptedException e) {
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
