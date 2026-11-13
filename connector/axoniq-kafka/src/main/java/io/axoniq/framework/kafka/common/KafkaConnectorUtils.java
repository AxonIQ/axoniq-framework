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

package io.axoniq.framework.kafka.common;

import org.apache.kafka.clients.consumer.Consumer;
import org.axonframework.common.annotation.Internal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;

/**
 * Helpers shared by the components of the Kafka connector: closing Kafka consumers quietly, and validating configured
 * durations.
 * <p>
 * Internal, because these helpers only serve the implementation of the connector, across its packages.
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
@Internal
public final class KafkaConnectorUtils {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConnectorUtils.class);

    private KafkaConnectorUtils() {
        // static helpers
    }

    /**
     * Closes the given {@code consumer}, waiting at most the given {@code timeout}, and logs a failure to close it at
     * debug level instead of throwing it.
     *
     * @param consumer the consumer to close
     * @param timeout  the longest time to wait for the consumer to close
     */
    @SuppressWarnings("deprecation")
    public static void closeQuietly(Consumer<?, ?> consumer, Duration timeout) {
        try {
            // Consumer.close(Duration) is deprecated in kafka-clients 4, but its replacement, CloseOptions, does not
            // exist in the 3.x line that Spring Boot 3 applications use, so the deprecated method serves both.
            consumer.close(timeout);
        } catch (Exception e) {
            logger.debug("Closing the Kafka consumer failed.", e);
        }
    }

    /**
     * Returns the given {@code duration} after checking that it is positive.
     *
     * @param duration the duration to check
     * @param name     the name of the setting, for the exception messages
     * @return the given {@code duration}
     * @throws NullPointerException     if the {@code duration} is {@code null}
     * @throws IllegalArgumentException if the {@code duration} is zero or negative
     */
    public static Duration requirePositive(Duration duration, String name) {
        Objects.requireNonNull(duration, "The " + name + " must not be null.");
        if (duration.isNegative() || duration.isZero()) {
            throw new IllegalArgumentException("The " + name + " must be positive.");
        }
        return duration;
    }
}
