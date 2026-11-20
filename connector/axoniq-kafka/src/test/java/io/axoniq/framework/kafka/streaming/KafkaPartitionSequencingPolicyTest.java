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
import org.axonframework.messaging.core.LegacyResources;
import org.axonframework.messaging.core.sequencing.SequentialPolicy;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventTestUtils;
import org.junit.jupiter.api.*;

import static org.assertj.core.api.Assertions.*;

class KafkaPartitionSequencingPolicyTest {

    private final KafkaPartitionSequencingPolicy testSubject = KafkaPartitionSequencingPolicy.INSTANCE;
    private final EventMessage event = EventTestUtils.asEventMessage("parcel dispatched");

    @Test
    void sequencesEventsByTheirKafkaPartition() {
        // given
        TopicPartition partition = new TopicPartition("parcels", 3);
        ProcessingContext context = new StubProcessingContext()
                .withResource(KafkaResources.TOPIC_PARTITION, partition)
                .withResource(LegacyResources.AGGREGATE_IDENTIFIER_KEY, "parcel-1");

        // when / then
        assertThat(testSubject.sequenceIdentifierFor(event, context)).contains(partition);
    }

    @Test
    void sequencesEventsWithoutPartitionByTheirAggregate() {
        // given - an event from another source, such as the event store
        ProcessingContext context = new StubProcessingContext()
                .withResource(LegacyResources.AGGREGATE_IDENTIFIER_KEY, "parcel-1");

        // when / then
        assertThat(testSubject.sequenceIdentifierFor(event, context)).contains("parcel-1");
    }

    @Test
    void sequencesOtherEventsSequentially() {
        // given
        ProcessingContext context = new StubProcessingContext();

        // when / then
        assertThat(testSubject.sequenceIdentifierFor(event, context))
                .isEqualTo(SequentialPolicy.INSTANCE.sequenceIdentifierFor(event, context));
        assertThat(testSubject.sequenceIdentifierFor(event, null))
                .isEqualTo(SequentialPolicy.INSTANCE.sequenceIdentifierFor(event, null));
    }
}
