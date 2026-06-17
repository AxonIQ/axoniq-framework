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
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.EventStoreTransaction;
import org.axonframework.eventsourcing.eventstore.SourcingCondition;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.axonframework.messaging.eventstreaming.StreamingCondition;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.function.Predicate;

import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.alwaysEmptyMessageTypeResolver;
import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.neverInvokedConverter;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The read-side decorators widen the condition's criteria before delegating, so a
 * type-filtering read still reaches the storage layer broadly enough to return every event the
 * chain can transform. This pins that {@code source(...)} and {@code open(...)} hand the
 * delegate a widened condition (preserving its start position / token), and that an unwidenable
 * chain passes the original condition through untouched.
 */
final class TransformingEventStoreCriteriaWideningTest {

    private static final QualifiedName CURRENT = new QualifiedName("com.example.CourseCreated");
    private static final QualifiedName LEGACY = new QualifiedName("com.example.LegacyCourseCreated");
    private static final MessageType CURRENT_V1 = new MessageType(CURRENT, "1.0.0");
    private static final MessageType CURRENT_V2 = new MessageType(CURRENT, "2.0.0");
    private static final MessageConverter CONVERTER = neverInvokedConverter();
    private static final MessageTypeResolver RESOLVER = alwaysEmptyMessageTypeResolver();

    private static EventTransformerChain wideningChain() {
        Predicate<MessageType> any = type -> true;
        return EventTransformerChain.builder()
                                    .register(EventTransformation.from(any)
                                                                 .declaringFromTypes(LEGACY)
                                                                 .to(CURRENT_V2)
                                                                 .transform(JsonNode.class, (in, ctx) -> in))
                                    .build();
    }

    private static EventTransformerChain versionOnlyChain() {
        return EventTransformerChain.builder()
                                    .register(EventTransformation.from(CURRENT_V1)
                                                                 .to(CURRENT_V2)
                                                                 .transform(JsonNode.class, (in, ctx) -> in))
                                    .build();
    }

    @Nested
    final class SourceWiring {

        @Test
        void sourceHandsTheDelegateAWidenedConditionPreservingItsStartPosition() {
            // given a transaction wrapping a delegate, over a chain that widens CourseCreated reads
            EventStoreTransaction delegate = mock(EventStoreTransaction.class);
            when(delegate.source(any(), any())).thenAnswer(invocation -> MessageStream.empty());
            TransformingEventStoreTransaction transaction = new TransformingEventStoreTransaction(
                    delegate, wideningChain(), new StubProcessingContext(), CONVERTER, RESOLVER);
            SourcingCondition condition = SourcingCondition.conditionFor(
                    EventCriteria.havingAnyTag().andBeingOneOfTypes(CURRENT));

            // when a type-filtering read is sourced
            transaction.source(condition, null);

            // then the delegate receives the widened criteria, with the sourcing strategy (start position) preserved
            ArgumentCaptor<SourcingCondition> captor = ArgumentCaptor.captor();
            verify(delegate).source(captor.capture(), isNull());
            SourcingCondition delegated = captor.getValue();
            assertThat(delegated.criteria().flatten())
                    .singleElement()
                    .satisfies(criterion -> assertThat(criterion.types()).containsExactlyInAnyOrder(CURRENT, LEGACY));
            assertThat(delegated.strategy()).isEqualTo(condition.strategy());
        }

        @Test
        void sourceLeavesTheConditionUntouchedWhenTheChainCannotWiden() {
            // given a transaction over a version-only chain that widens nothing
            EventStoreTransaction delegate = mock(EventStoreTransaction.class);
            when(delegate.source(any(), any())).thenAnswer(invocation -> MessageStream.empty());
            TransformingEventStoreTransaction transaction = new TransformingEventStoreTransaction(
                    delegate, versionOnlyChain(), new StubProcessingContext(), CONVERTER, RESOLVER);
            SourcingCondition condition = SourcingCondition.conditionFor(
                    EventCriteria.havingAnyTag().andBeingOneOfTypes(CURRENT));

            // when sourcing the read
            transaction.source(condition, null);

            // then the exact same condition instance is delegated, unchanged (zero-cost)
            ArgumentCaptor<SourcingCondition> captor = ArgumentCaptor.captor();
            verify(delegate).source(captor.capture(), isNull());
            assertThat(captor.getValue()).isSameAs(condition);
        }
    }

    @Nested
    final class OpenWiring {

        @Test
        void openHandsTheDelegateAWidenedConditionPreservingItsToken() {
            // given a store wrapping a delegate, over a chain that widens CourseCreated reads
            EventStore delegate = mock(EventStore.class);
            when(delegate.open(any(), any())).thenAnswer(invocation -> MessageStream.empty());
            TransformingEventStore store = new TransformingEventStore(delegate, wideningChain(), CONVERTER, RESOLVER);
            StreamingCondition condition = StreamingCondition.conditionFor(
                    TrackingToken.FIRST, EventCriteria.havingAnyTag().andBeingOneOfTypes(CURRENT));

            // when a type-filtering stream is opened
            store.open(condition, null);

            // then the delegate receives the widened criteria, with the token preserved
            ArgumentCaptor<StreamingCondition> captor = ArgumentCaptor.captor();
            verify(delegate).open(captor.capture(), isNull());
            StreamingCondition delegated = captor.getValue();
            assertThat(delegated.criteria().flatten())
                    .singleElement()
                    .satisfies(criterion -> assertThat(criterion.types()).containsExactlyInAnyOrder(CURRENT, LEGACY));
            assertThat(delegated.position()).isEqualTo(condition.position());
        }

        @Test
        void openLeavesTheConditionUntouchedWhenTheChainCannotWiden() {
            // given a store over a version-only chain that widens nothing
            EventStore delegate = mock(EventStore.class);
            when(delegate.open(any(), any())).thenAnswer(invocation -> MessageStream.empty());
            TransformingEventStore store = new TransformingEventStore(delegate, versionOnlyChain(), CONVERTER, RESOLVER);
            StreamingCondition condition = StreamingCondition.conditionFor(
                    TrackingToken.FIRST, EventCriteria.havingAnyTag().andBeingOneOfTypes(CURRENT));

            // when opening the stream
            store.open(condition, null);

            // then the exact same condition instance is delegated, unchanged (zero-cost)
            ArgumentCaptor<StreamingCondition> captor = ArgumentCaptor.captor();
            verify(delegate).open(captor.capture(), isNull());
            assertThat(captor.getValue()).isSameAs(condition);
        }
    }
}
