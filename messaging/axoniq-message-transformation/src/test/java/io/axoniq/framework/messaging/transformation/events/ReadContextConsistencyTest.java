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

package io.axoniq.framework.messaging.transformation.events;

import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.EventStoreTransaction;
import org.axonframework.eventsourcing.eventstore.SourcingCondition;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventstreaming.StreamingCondition;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;

import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.collectMessages;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

/**
 * All three event read contexts -- entity load via
 * {@code EventStore.transaction(...).source(...)}, DCB read via the same
 * {@code source(...)}, and tracking-processor read via {@code EventStore.open(...)}
 * -- observe the identical transformed result for the same stored event.
 */
final class ReadContextConsistencyTest {

    private static final MessageType V1 = new MessageType("com.example.CourseCreated", "1.0.0");
    private static final MessageType V2 = new MessageType("com.example.CourseCreated", "2.0.0");

    @Test
    @Disabled("Tests-first; impl lands in T030 (TransformingEventStore wraps transaction + open)")
    void entityLoadAndDcbReadAndTrackingProcessorAllObserveSameTransformedEvent() {
        EventTransformerChain chainWithV1ToV2 = EventTransformerChain.builder().build();
        MessageConverter converter = Mockito.mock(MessageConverter.class);
        EventStore delegateStore = Mockito.mock(EventStore.class);
        EventStoreTransaction delegateTransaction = Mockito.mock(EventStoreTransaction.class);
        ProcessingContext context = Mockito.mock(ProcessingContext.class);

        EventMessage storedV1Event = new GenericEventMessage(V1, "v1-payload");

        when(delegateStore.transaction(context)).thenReturn(delegateTransaction);
        // doReturn bypasses Mockito's generic-inference issue with the wildcard return type
        // of EventStoreTransaction.source(...).
        doReturn(MessageStream.fromIterable(List.of(storedV1Event)))
                .when(delegateTransaction).source(Mockito.any(SourcingCondition.class), Mockito.any());
        when(delegateStore.open(Mockito.any(StreamingCondition.class), Mockito.any()))
                .thenReturn(MessageStream.fromIterable(List.of(storedV1Event)));

        TransformingEventStore decoratedStore = new TransformingEventStore(delegateStore, chainWithV1ToV2, converter);

        SourcingCondition sourcingCondition = Mockito.mock(SourcingCondition.class);
        List<EventMessage> entityLoad = collectMessages(decoratedStore.transaction(context).source(sourcingCondition));
        List<EventMessage> dcbRead = collectMessages(decoratedStore.transaction(context).source(sourcingCondition));
        StreamingCondition streamingCondition = Mockito.mock(StreamingCondition.class);
        List<EventMessage> trackingProcessorRead = collectMessages(decoratedStore.open(streamingCondition, context));

        assertThat(entityLoad).hasSize(1);
        assertThat(entityLoad.getFirst().type()).isEqualTo(V2);
        assertThat(dcbRead).hasSize(1);
        assertThat(dcbRead.getFirst().type()).isEqualTo(V2);
        assertThat(trackingProcessorRead).hasSize(1);
        assertThat(trackingProcessorRead.getFirst().type()).isEqualTo(V2);
    }
}
