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

/**
 * Decides what the {@link KafkaSubscribableEventSource} does with a record whose event fails to be processed.
 * <p>
 * The records of a partition are processed in order, so a failing record holds up the records after it in the same
 * partition until the policy has dealt with it. Other partitions are not affected.
 * <p>
 * The policy only sees the failures that the subscribing processor's error handler propagates, as its default
 * {@link org.axonframework.messaging.eventhandling.processing.errorhandling.PropagatingErrorHandler} does. With an
 * error handler that logs failures instead, the event counts as processed and its offset is committed.
 * <p>
 * Records that carry no event, or that cannot be deserialized or decoded, are not subject to this policy, but to the
 * {@link io.axoniq.framework.kafka.codec.UndecodableRecordPolicy}.
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
public enum FailedRecordPolicy {

    /**
     * Processes the record again after a delay, which grows exponentially with every failed attempt up to the
     * configured maximum. The record's partition is paused in the meantime. Offsets are not committed past the record
     * until it is processed successfully. This is the default.
     */
    RETRY,

    /**
     * Logs a warning and continues with the next record. The failed event is not processed again.
     */
    SKIP,

    /**
     * Publishes the original record to a dead-letter topic, named after the record's topic with the configured
     * suffix, and continues with the next record. The dead-letter record carries headers describing the failure and
     * the record's original position. When publishing to the dead-letter topic fails, the record is retried instead.
     */
    DEAD_LETTER_TOPIC
}
