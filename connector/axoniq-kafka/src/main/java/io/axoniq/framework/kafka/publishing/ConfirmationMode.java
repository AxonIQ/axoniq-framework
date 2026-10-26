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

package io.axoniq.framework.kafka.publishing;

/**
 * How a {@link KafkaEventSink} confirms that the events it publishes have reached Kafka, and how publishing relates
 * to the {@link org.axonframework.messaging.core.unitofwork.ProcessingContext} the events are published in.
 * <p>
 * When events are published outside of a processing context, the sink sends them right away, and the future returned
 * by {@link KafkaEventSink#publish} reflects the chosen confirmation.
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
public enum ConfirmationMode {

    /**
     * Sends records right away and does not wait for Kafka to acknowledge them. Failures to deliver a record are
     * logged, but do not affect the processing context. This is the fastest mode. A record that fails to be delivered
     * is lost.
     */
    NONE,

    /**
     * Sends records right away, and waits for Kafka to acknowledge them before the processing context commits. A
     * record that is not acknowledged within the configured acknowledgement timeout fails the processing context.
     * <p>
     * Records sent before the processing context fails are not withdrawn, so consumers may see events of a processing
     * context that did not commit. A failure, including a missing acknowledgement, is reported as a
     * {@link KafkaPublicationException}. The acknowledgement timeout only covers waiting for acknowledgements: how
     * long sending itself may block, for example while Kafka's metadata is not available, is bounded by the producer's
     * {@code max.block.ms} setting.
     */
    WAIT_FOR_ACK,

    /**
     * Sends the records of a processing context in a single Kafka transaction, which commits while the processing
     * context prepares to commit, and aborts when the processing context fails before that. Consumers reading with
     * {@code isolation.level=read_committed} only see events whose Kafka transaction committed.
     * <p>
     * The Kafka transaction commits with the context's other preparations, such as storing a processor's progress, and
     * before its resources, such as a database transaction, commit. A failure later in the commit leaves the events
     * published, so delivery is at least once. A failure is reported as a {@link KafkaPublicationException}.
     * <p>
     * The producer's {@code max.block.ms} setting bounds how long beginning, sending in and committing a transaction
     * may block. A commit that times out is ambiguous: the transaction may still commit on the broker. The processing
     * context then fails. The commit blocks the thread that commits the processing context, unless the sink has a
     * {@link KafkaEventSinkConfiguration#withCommitExecutor(java.util.concurrent.Executor) commit executor}.
     * <p>
     * Requires a transactional identifier prefix that is unique to each application instance.
     */
    TRANSACTIONAL
}
