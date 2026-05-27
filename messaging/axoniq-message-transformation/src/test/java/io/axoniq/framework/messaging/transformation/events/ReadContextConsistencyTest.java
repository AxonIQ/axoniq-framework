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

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

/**
 * Verifies that all three event read contexts -- entity load via
 * {@code EventStore.transaction(...).source(...)}, DCB reads via the same
 * {@code source(...)}, and tracking-processor reads via {@code EventStore.open(...)}
 * -- observe the identical transformed result for the same stored event.
 */
class ReadContextConsistencyTest {

    private static final MessageType V1 = new MessageType("com.example.CourseCreated", "1.0.0");
    private static final MessageType V2 = new MessageType("com.example.CourseCreated", "2.0.0");

    @Test
    @Disabled("Tests-first; impl lands in T030 (TransformingEventStore wraps transaction + open)")
    void entity_load_and_dcb_read_and_tracking_processor_all_observe_the_same_transformed_event() {
        // given
        EventTransformerChain chain = EventTransformerChain.builder().build(); // populated with v1->v2 transformer
        MessageConverter converter = Mockito.mock(MessageConverter.class);
        EventStore delegate = Mockito.mock(EventStore.class);
        EventStoreTransaction tx = Mockito.mock(EventStoreTransaction.class);
        ProcessingContext ctx = Mockito.mock(ProcessingContext.class);

        EventMessage stored = new GenericEventMessage(V1, "v1-payload");

        when(delegate.transaction(ctx)).thenReturn(tx);
        // doReturn(...) bypasses Mockito's generic-inference issue with the wildcard
        // return type of EventStoreTransaction.source(...).
        doReturn(MessageStream.fromIterable(List.of(stored)))
                .when(tx).source(Mockito.any(SourcingCondition.class), Mockito.any());
        when(delegate.open(Mockito.any(StreamingCondition.class), Mockito.any()))
                .thenReturn(MessageStream.fromIterable(List.of(stored)));

        TransformingEventStore store = new TransformingEventStore(delegate, chain, converter);

        // when -- (a) entity load via transaction().source()
        SourcingCondition srcCond = Mockito.mock(SourcingCondition.class);
        List<EventMessage> entityLoad = drain(store.transaction(ctx).source(srcCond));

        // and -- (b) DCB read via the same source() entry point
        List<EventMessage> dcbRead = drain(store.transaction(ctx).source(srcCond));

        // and -- (c) tracking-processor read via open()
        StreamingCondition strCond = Mockito.mock(StreamingCondition.class);
        List<EventMessage> trackingRead = drain(store.open(strCond, ctx));

        // then -- all three see v2
        assertThat(entityLoad).hasSize(1);
        assertThat(entityLoad.get(0).type()).isEqualTo(V2);
        assertThat(dcbRead).hasSize(1);
        assertThat(dcbRead.get(0).type()).isEqualTo(V2);
        assertThat(trackingRead).hasSize(1);
        assertThat(trackingRead.get(0).type()).isEqualTo(V2);
    }

    private static List<EventMessage> drain(MessageStream<? extends EventMessage> stream) {
        return stream.<List<EventMessage>>reduce(new ArrayList<>(), (acc, entry) -> {
            acc.add(entry.message());
            return acc;
        }).join();
    }
}
