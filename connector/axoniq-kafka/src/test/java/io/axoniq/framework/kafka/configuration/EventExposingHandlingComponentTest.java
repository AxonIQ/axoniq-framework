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

package io.axoniq.framework.kafka.configuration;

import io.axoniq.framework.kafka.publishing.RecordKeyResolver;
import org.axonframework.common.FutureUtils;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.core.LegacyResources;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventSink;
import org.axonframework.messaging.eventhandling.EventTestUtils;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.*;

class EventExposingHandlingComponentTest {

    private final RecordKeyResolver fallback = RecordKeyResolver.fromMetadata("parcelId");
    private final RecordKeyResolver testSubject = EventExposingHandlingComponent.keyingByAggregate(fallback);
    private final EventMessage event = EventTestUtils.asEventMessage("parcel dispatched")
                                                     .andMetadata(Map.of("parcelId", "p-1"));

    /**
     * Returns the context in which the exposing component publishes the event, when it handles it in the given
     * {@code context}.
     */
    private ProcessingContext exposedIn(ProcessingContext context) {
        RecordingSink sink = new RecordingSink();
        new EventExposingHandlingComponent("exposure", sink, Set.of(event.type().qualifiedName()))
                .handle(event, context);
        return sink.context.get();
    }

    @Test
    void keysAnExposedEventByItsAggregateIdentifier() {
        // given
        ProcessingContext handling = new StubProcessingContext()
                .withResource(LegacyResources.AGGREGATE_IDENTIFIER_KEY, "aggregate-1");

        // when
        String key = testSubject.resolve(event, exposedIn(handling));

        // then
        assertThat(key).isEqualTo("aggregate-1");
    }

    @Test
    void keysAnExposedEventWithoutAggregateWithTheFallback() {
        // when
        String key = testSubject.resolve(event, exposedIn(new StubProcessingContext()));

        // then
        assertThat(key).isEqualTo("p-1");
    }

    @Test
    void keysAnExplicitlyPublishedEventWithTheFallbackEvenWhileHandlingAnAggregateEvent() {
        // given - a handler publishes in the context of the aggregate event it handles
        ProcessingContext handling = new StubProcessingContext()
                .withResource(LegacyResources.AGGREGATE_IDENTIFIER_KEY, "aggregate-1");

        // when
        String key = testSubject.resolve(event, handling);

        // then
        assertThat(key).isEqualTo("p-1");
    }

    @Test
    void keysAnEventPublishedOutsideAContextWithTheFallback() {
        // when
        String key = testSubject.resolve(event, null);

        // then
        assertThat(key).isEqualTo("p-1");
    }

    @Test
    void refusesResets() {
        // when
        EventExposingHandlingComponent component =
                new EventExposingHandlingComponent("exposure", new RecordingSink(), Set.of(new QualifiedName("x")));

        // then
        assertThat(component.supportsReset()).isFalse();
    }

    private static final class RecordingSink implements EventSink {

        private final AtomicReference<ProcessingContext> context = new AtomicReference<>();

        @Override
        public CompletableFuture<Void> publish(@Nullable ProcessingContext context,
                                               List<? extends EventMessage> events) {
            this.context.set(context);
            return FutureUtils.emptyCompletedFuture();
        }

        @Override
        public void describeTo(ComponentDescriptor descriptor) {
            // nothing to describe
        }
    }
}
