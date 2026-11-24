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

/**
 * Decides what a Kafka event source does when a position it resumes from no longer exists, because Kafka has already
 * deleted the records at that position under the topic's retention settings.
 * <p>
 * The {@link io.axoniq.framework.kafka.streaming.KafkaStreamableEventSource} applies it to the position in a
 * processor's token, including a position of {@code 0} in a partition whose first records were deleted, the
 * {@link io.axoniq.framework.kafka.subscribing.KafkaSubscribableEventSource} to the committed offset of its consumer
 * group.
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
public enum RetentionGapPolicy {

    /**
     * Stops at the gap, so that an operator decides how to continue: events were lost to retention. This is the
     * default.
     * <p>
     * The streamable source fails the event stream, so that the processor reports the problem and retries according
     * to its error handling, until the processor is reset. The subscribable source logs an error and stops consuming
     * the partition, without committing an offset; it hits the gap again when the partition is assigned again, for
     * example after a restart, until the consumer group's offsets are reset.
     */
    FAIL,

    /**
     * Continues from the earliest record still available in the partition, and logs a warning. The events that were
     * deleted are skipped. For the streamable source, this also makes a partition that a token holds at position
     * {@code 0} start at its earliest available record, whatever Kafka deleted before it.
     */
    RESET_TO_EARLIEST
}
