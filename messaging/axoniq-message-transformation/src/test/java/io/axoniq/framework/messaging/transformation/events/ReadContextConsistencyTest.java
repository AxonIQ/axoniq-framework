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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.EventStoreTransaction;
import org.axonframework.eventsourcing.eventstore.SourcingCondition;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.axonframework.messaging.eventstreaming.StreamingCondition;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;

import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.collectMessages;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
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
    void entityLoadAndDcbReadAndTrackingProcessorAllObserveSameTransformedEvent() {
        EventTransformer v1ToV2Transformer = EventTransformation.from(V1).to(V2)
                                                                .transform(JsonNode.class, (in, ctx) -> in.deepCopy());
        EventTransformerChain chain = EventTransformerChain.builder().register(v1ToV2Transformer).build();
        MessageConverter converter = Mockito.mock(MessageConverter.class);

        ObjectNode payload = JsonNodeFactory.instance.objectNode().put("k", "v");
        EventMessage storedV1Event = new GenericEventMessage(V1, payload);

        EventStore delegateStore = Mockito.mock(EventStore.class);
        EventStoreTransaction delegateTransaction = Mockito.mock(EventStoreTransaction.class);
        when(delegateStore.transaction(any())).thenReturn(delegateTransaction);
        // doAnswer creates a fresh stream per invocation (MessageStream is single-use after consumption).
        doAnswer(invocation -> MessageStream.fromIterable(List.of(storedV1Event)))
                .when(delegateTransaction).source(any(SourcingCondition.class), any());
        when(delegateStore.open(any(StreamingCondition.class), any()))
                .thenAnswer(invocation -> MessageStream.fromIterable(List.of(storedV1Event)));

        TransformingEventStore decoratedStore = new TransformingEventStore(delegateStore, chain, converter);

        ProcessingContext context = new StubProcessingContext();
        SourcingCondition sourcingCondition = SourcingCondition.conditionFor(EventCriteria.havingAnyTag());
        StreamingCondition streamingCondition = StreamingCondition.startingFrom(null);

        List<EventMessage> entityLoad = collectMessages(decoratedStore.transaction(context).source(sourcingCondition));
        List<EventMessage> dcbRead = collectMessages(decoratedStore.transaction(context).source(sourcingCondition));
        List<EventMessage> trackingProcessorRead = collectMessages(decoratedStore.open(streamingCondition, context));

        assertThat(entityLoad).extracting(EventMessage::type).containsExactly(V2);
        assertThat(dcbRead).extracting(EventMessage::type).containsExactly(V2);
        assertThat(trackingProcessorRead).extracting(EventMessage::type).containsExactly(V2);
    }
}
