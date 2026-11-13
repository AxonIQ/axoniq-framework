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
 * The names of the headers that a {@link KafkaSubscribableEventSource} adds to a record it writes to a dead-letter
 * topic, under the {@link FailedRecordPolicy#DEAD_LETTER_TOPIC} policy.
 * <p>
 * A dead-letter record keeps the key, value, timestamp and headers of the original record, and adds these headers,
 * whose values are UTF-8 encoded strings. Applications that consume dead-letter topics, for example to inspect or
 * reprocess failed records, read them to find out where a record came from and why it failed:
 * <pre>{@code
 * Header topic = deadLetter.headers().lastHeader(DeadLetterHeaders.ORIGINAL_TOPIC);
 * String originalTopic = new String(topic.value(), StandardCharsets.UTF_8);
 * }</pre>
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
public final class DeadLetterHeaders {

    /**
     * Header naming the class of the exception that failed the original record.
     */
    public static final String EXCEPTION_CLASS = "axon-dlt-exception-class";
    /**
     * Header holding the message of the exception that failed the original record. Absent when the exception has no
     * message.
     */
    public static final String EXCEPTION_MESSAGE = "axon-dlt-exception-message";
    /**
     * Header naming the topic of the original record.
     */
    public static final String ORIGINAL_TOPIC = "axon-dlt-original-topic";
    /**
     * Header holding the partition of the original record, as a decimal number.
     */
    public static final String ORIGINAL_PARTITION = "axon-dlt-original-partition";
    /**
     * Header holding the offset of the original record within its partition, as a decimal number.
     */
    public static final String ORIGINAL_OFFSET = "axon-dlt-original-offset";

    private DeadLetterHeaders() {
        // constants only
    }
}
