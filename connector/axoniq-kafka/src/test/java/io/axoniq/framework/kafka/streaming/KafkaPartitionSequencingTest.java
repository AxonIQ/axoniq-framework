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
import org.axonframework.conversion.PassThroughConverter;
import org.axonframework.messaging.core.annotation.AnnotationMessageTypeResolver;
import org.axonframework.messaging.core.annotation.ClasspathHandlerDefinition;
import org.axonframework.messaging.core.annotation.ClasspathParameterResolverFactory;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.eventhandling.EventHandlingComponent;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventTestUtils;
import org.axonframework.messaging.eventhandling.annotation.AnnotatedEventHandlingComponent;
import org.axonframework.messaging.eventhandling.annotation.EventHandler;
import org.axonframework.messaging.eventhandling.conversion.DelegatingEventConverter;
import org.junit.jupiter.api.*;

import static org.assertj.core.api.Assertions.*;

class KafkaPartitionSequencingTest {

    private final TopicPartition partition = new TopicPartition("parcels", 3);
    private final EventMessage event = EventTestUtils.asEventMessage("parcel dispatched");
    private final ProcessingContext context =
            new StubProcessingContext().withResource(KafkaResources.TOPIC_PARTITION, partition);

    @Test
    void sequencesTheEventsOfAnAnnotatedMethodByTheirKafkaPartition() {
        // given
        EventHandlingComponent component = annotatedComponent(new AnnotatedMethodHandler());

        // when / then
        assertThat(component.sequenceIdentifierFor(event, context)).isEqualTo(partition);
    }

    @Test
    void sequencesTheEventsOfAnAnnotatedClassByTheirKafkaPartition() {
        // given
        EventHandlingComponent component = annotatedComponent(new AnnotatedClassHandler());

        // when / then
        assertThat(component.sequenceIdentifierFor(event, context)).isEqualTo(partition);
    }

    private static <T> EventHandlingComponent annotatedComponent(T handler) {
        return new AnnotatedEventHandlingComponent<>(
                handler,
                ClasspathParameterResolverFactory.forClass(handler.getClass()),
                ClasspathHandlerDefinition.forClass(handler.getClass()),
                new AnnotationMessageTypeResolver(),
                new DelegatingEventConverter(PassThroughConverter.INSTANCE)
        );
    }

    static class AnnotatedMethodHandler {

        @KafkaPartitionSequencing
        @EventHandler
        void on(String event) {
        }
    }

    @KafkaPartitionSequencing
    static class AnnotatedClassHandler {

        @EventHandler
        void on(String event) {
        }
    }
}
