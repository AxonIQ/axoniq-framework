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

import io.axoniq.framework.kafka.common.KafkaAxoniqAddon;
import io.axoniq.license.entitlement.EntitlementManager;
import io.axoniq.license.entitlement.EntitlementMessageType;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.axonframework.common.FutureUtils;
import org.axonframework.common.annotation.Internal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/**
 * Sending records and reporting the rejections and failures of publishing, shared by the confirmation modes of a
 * {@link KafkaEventSink}.
 * <p>
 * Internal, because it only serves the publishing of {@link KafkaEventSink}.
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
@Internal
final class PublishingSupport {

    private static final Logger logger = LoggerFactory.getLogger(PublishingSupport.class);

    private PublishingSupport() {
        // Utility class
    }

    /**
     * Runs the given {@code registration} of a processing context lifecycle handler, rejecting the publication
     * when the context is already committing.
     *
     * @param registration registers the handler with the processing context
     * @throws IllegalStateException if the context is already committing
     */
    static void registerLate(Runnable registration) {
        try {
            registration.run();
        } catch (IllegalStateException e) {
            throw new IllegalStateException(
                    "Events can no longer be published to Kafka in this processing context: it is already "
                            + "committing. Publish before the commit starts, or outside of the processing context.",
                    e
            );
        }
    }

    /**
     * Claims the given {@code count} of published events against the given {@code entitlementManager}. A failing claim
     * is logged at WARN level and does not fail the publication, as the events are already in Kafka.
     *
     * @param entitlementManager the entitlement manager to claim the events against
     * @param count              the number of published events
     */
    static void claim(EntitlementManager entitlementManager, int count) {
        try {
            entitlementManager.claimMessage(KafkaAxoniqAddon.IDENTIFIER, EntitlementMessageType.EVENT, count);
        } catch (RuntimeException e) {
            logger.warn("Claiming {} events published to Kafka against the entitlement failed. The events are "
                                + "published.", count, e);
        }
    }

    /**
     * Returns the exception that rejects a publication once the sink is shut down.
     *
     * @return the exception to throw
     */
    static IllegalStateException sinkShutDown() {
        return new IllegalStateException("The Kafka sink is shut down and no longer publishes events.");
    }

    /**
     * Sends the given {@code record} with the given {@code producer}, reporting its acknowledgement, or the failure
     * to send it, as a {@link KafkaPublicationException}, through the returned future.
     *
     * @param producer the producer to send with
     * @param record   the record to send
     * @param <V>      the type of record value
     * @return a future that completes when Kafka acknowledged the record
     */
    static <V> CompletableFuture<Void> send(Producer<String, V> producer, ProducerRecord<String, V> record) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        try {
            producer.send(record, (metadata, error) -> {
                if (error != null) {
                    result.completeExceptionally(publicationFailure(record, error));
                } else {
                    result.complete(FutureUtils.ignoreResult(metadata));
                }
            });
        } catch (RuntimeException e) {
            result.completeExceptionally(publicationFailure(record, e));
        }
        return result;
    }

    /**
     * Returns the exception reporting that publishing the given {@code record} failed with the given {@code cause}.
     *
     * @param record the record that was not published
     * @param cause  the reason publishing failed
     * @return the exception reporting the failure
     */
    static KafkaPublicationException publicationFailure(ProducerRecord<?, ?> record, Throwable cause) {
        return new KafkaPublicationException("Publishing an event to Kafka topic [" + record.topic()
                                                     + "] failed.", cause);
    }
}
