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

package io.axoniq.framework.kafka.streaming;

import io.axoniq.framework.kafka.common.KafkaResources;
import org.apache.kafka.common.TopicPartition;
import org.axonframework.messaging.core.sequencing.HierarchicalSequencingPolicy;
import org.axonframework.messaging.core.sequencing.SequencingPolicy;
import org.axonframework.messaging.core.sequencing.SequentialPerAggregatePolicy;
import org.axonframework.messaging.core.sequencing.SequentialPolicy;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.jspecify.annotations.Nullable;

import java.util.Optional;

/**
 * A {@link SequencingPolicy} that processes events of the same Kafka partition in order, and events of different
 * partitions concurrently.
 * <p>
 * It sequences events on the {@link KafkaResources#TOPIC_PARTITION topic and partition} they were consumed from. Events
 * that were not consumed from Kafka, for example in a processor that also reads from the event store, are sequenced
 * as the framework does by default: per aggregate, and otherwise one at a time.
 * <p>
 * Events from Kafka carry no aggregate identifier, so without this policy, a processor handles them strictly one at a
 * time. Use {@link KafkaPartitionSequencing @KafkaPartitionSequencing} on an event handler method or class, or
 * configure the policy for an event handling component:
 * <pre>{@code
 * SimpleEventHandlingComponent.create("parcel-projection", KafkaPartitionSequencingPolicy.INSTANCE);
 * }</pre>
 * A pooled streaming event processor assigns sequences to segments by hash. With fewer partitions than segments, some
 * segments stay idle.
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
public class KafkaPartitionSequencingPolicy implements SequencingPolicy<EventMessage> {

    /**
     * The shared instance of this policy.
     */
    public static final KafkaPartitionSequencingPolicy INSTANCE = new KafkaPartitionSequencingPolicy();

    private static final SequencingPolicy<EventMessage> FALLBACK =
            new HierarchicalSequencingPolicy<>(SequentialPerAggregatePolicy.INSTANCE, SequentialPolicy.INSTANCE);

    /**
     * Constructs the policy. Prefer the shared {@link #INSTANCE}.
     */
    public KafkaPartitionSequencingPolicy() {
        // stateless
    }

    @Override
    public Optional<Object> sequenceIdentifierFor(EventMessage message, @Nullable ProcessingContext context) {
        if (context == null) {
            // Without a context there is neither a partition nor an aggregate to sequence by.
            return SequentialPolicy.INSTANCE.sequenceIdentifierFor(message, null);
        }
        TopicPartition partition = context.getResource(KafkaResources.TOPIC_PARTITION);
        if (partition != null) {
            return Optional.of(partition);
        }
        return FALLBACK.sequenceIdentifierFor(message, context);
    }
}
