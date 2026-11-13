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

package io.axoniq.framework.kafka.subscribing;

import org.axonframework.common.annotation.Internal;

import java.time.Duration;

/**
 * The time limits of the blocking steps of the subscribing Kafka event source, in one place, together with the bounds
 * they add up to.
 * <p>
 * The public documentation of {@link KafkaSubscribableEventSource#shutdown()}, of
 * {@link KafkaSubscribableEventSourceConfiguration#withRevocationTimeout(Duration)} and
 * {@link KafkaSubscribableEventSourceConfiguration#withProducerProperties(java.util.Map)}, and the reference guide
 * state these numbers: stopping takes at most the revocation timeout plus 15 seconds ({@link #COMMIT_TIMEOUT} plus
 * {@link #CONSUMER_CLOSE_TIMEOUT}), the dead-letter producer's {@code max.block.ms} defaults to 10 seconds
 * ({@link #DEAD_LETTER_MAX_BLOCK}), which must not exceed the revocation timeout plus 10 seconds
 * ({@link #COMMIT_TIMEOUT}), and writing a dead-letter record waits 20 seconds for its acknowledgement
 * ({@link #DEAD_LETTER_ACK_TIMEOUT}). Change them together.
 * <p>
 * Internal, because these limits are implementation details of the source that its public documentation describes.
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
@Internal
final class ShutdownBudget {

    /**
     * The longest time a commit may take. Together with {@link #CONSUMER_CLOSE_TIMEOUT} and the default revocation
     * timeout, it keeps a stopping consumer within the 30 seconds that Spring Boot gives a lifecycle phase by default.
     */
    static final Duration COMMIT_TIMEOUT = Duration.ofSeconds(10);
    /**
     * The longest time closing a consumer, and leaving the consumer group, may take.
     */
    static final Duration CONSUMER_CLOSE_TIMEOUT = Duration.ofSeconds(5);
    /**
     * The longest time the polling thread waits for the acknowledgement of a dead-letter record.
     */
    static final Duration DEAD_LETTER_ACK_TIMEOUT = Duration.ofSeconds(20);
    /**
     * The longest time a send to a dead-letter topic may block the polling thread, unless configured otherwise with
     * the producer's {@code max.block.ms}.
     */
    static final Duration DEAD_LETTER_MAX_BLOCK = Duration.ofSeconds(10);
    /**
     * The longest time closing the dead-letter producer waits for the records it has not delivered yet, when the
     * consumers' stop timeout leaves that much.
     */
    static final Duration DEAD_LETTER_CLOSE_TIMEOUT = Duration.ofSeconds(5);

    private ShutdownBudget() {
        // constants only
    }

    /**
     * Returns how long a consumer may take to stop once asked to: the given {@code revocationTimeout} for the events
     * in progress, plus the time to commit their progress and to close the consumer. Closing the dead-letter producer
     * happens within what is left of it.
     * <p>
     * Once asked to stop, a consumer writes no more records to dead-letter topics, and stops waiting for the
     * acknowledgement of a record it is writing at the revocation timeout. A record it is handing to the dead-letter
     * producer at that moment can block it for the rest of the producer's {@code max.block.ms}, so the bound holds as
     * long as that setting does not exceed the revocation timeout plus the {@link #COMMIT_TIMEOUT}, which the default
     * {@link #DEAD_LETTER_MAX_BLOCK} never does.
     *
     * @param revocationTimeout the longest time a consumer waits for the events in progress
     * @return the longest time a consumer takes to stop
     */
    static Duration stopTimeout(Duration revocationTimeout) {
        return revocationTimeout.plus(COMMIT_TIMEOUT).plus(CONSUMER_CLOSE_TIMEOUT);
    }
}
