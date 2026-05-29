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

package io.axoniq.framework.messaging.eventstreaming.safepoint;

import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.DelegatingEventHandlingComponent;
import org.axonframework.messaging.eventhandling.EventHandlingComponent;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventhandling.replay.ReplayStatusChanged;
import org.axonframework.messaging.eventhandling.replay.ResetContext;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

class SafePointConfigurationEnhancerTest {

    private static final Segment SEGMENT = Segment.ROOT_SEGMENT;

    @Nested
    class Discovery {

        @Test
        void findsProviderImplementedDirectlyOnHandler() {
            // given
            ProviderComponent provider = new ProviderComponent(new GlobalSequenceTrackingToken(3));

            // when
            List<SegmentSafePointProvider> discovered =
                    SafePointConfigurationEnhancer.discoverProviders(List.of(provider));

            // then
            assertThat(discovered).containsExactly(provider);
        }

        @Test
        void findsProviderWrappedByDecorator() {
            // given
            ProviderComponent provider = new ProviderComponent(new GlobalSequenceTrackingToken(3));
            TestDecorator wrapped = new TestDecorator(provider);

            // when
            List<SegmentSafePointProvider> discovered =
                    SafePointConfigurationEnhancer.discoverProviders(List.of(wrapped));

            // then
            assertThat(discovered).containsExactly(provider);
        }

        @Test
        void findsProviderWrappedByMultipleDecorators() {
            // given
            ProviderComponent provider = new ProviderComponent(new GlobalSequenceTrackingToken(3));
            EventHandlingComponent wrapped = new TestDecorator(new TestDecorator(new TestDecorator(provider)));

            // when
            List<SegmentSafePointProvider> discovered =
                    SafePointConfigurationEnhancer.discoverProviders(List.of(wrapped));

            // then
            assertThat(discovered).containsExactly(provider);
        }

        @Test
        void returnsEmptyWhenNoProvidersPresent() {
            // given
            EventHandlingComponent plain = new StubEventHandlingComponent();
            EventHandlingComponent wrapped = new TestDecorator(new StubEventHandlingComponent());

            // when
            List<SegmentSafePointProvider> discovered =
                    SafePointConfigurationEnhancer.discoverProviders(List.of(plain, wrapped));

            // then
            assertThat(discovered).isEmpty();
        }

        @Test
        void discoversMultipleProvidersAcrossHandlers() {
            // given
            ProviderComponent providerA = new ProviderComponent(new GlobalSequenceTrackingToken(3));
            ProviderComponent providerB = new ProviderComponent(new GlobalSequenceTrackingToken(7));

            // when
            List<SegmentSafePointProvider> discovered =
                    SafePointConfigurationEnhancer.discoverProviders(List.of(providerA, providerB));

            // then
            assertThat(discovered).containsExactlyInAnyOrder(providerA, providerB);
        }
    }

    @Nested
    class MergedSafePoint {

        @Test
        void returnsNullWhenProviderListIsEmpty() {
            // when
            TrackingToken result =
                    SafePointConfigurationEnhancer.mergedSafePoint(List.of(), SEGMENT).join();

            // then
            assertThat(result).isNull();
        }

        @Test
        void returnsSingleProviderSafePoint() {
            // given
            GlobalSequenceTrackingToken token = new GlobalSequenceTrackingToken(5);
            ProviderComponent provider = new ProviderComponent(token);

            // when
            TrackingToken result =
                    SafePointConfigurationEnhancer.mergedSafePoint(List.of(provider), SEGMENT).join();

            // then
            assertThat(result).isEqualTo(token);
        }

        @Test
        void mergesMultipleSafePointsViaLowerBound() {
            // given - position 5 and position 2; lower wins
            ProviderComponent later = new ProviderComponent(new GlobalSequenceTrackingToken(5));
            ProviderComponent earlier = new ProviderComponent(new GlobalSequenceTrackingToken(2));

            // when
            TrackingToken result =
                    SafePointConfigurationEnhancer.mergedSafePoint(List.of(later, earlier), SEGMENT).join();

            // then
            assertThat(result).isEqualTo(new GlobalSequenceTrackingToken(2));
        }

        @Test
        void nullSafePointFromOneProviderIsTreatedAsNoConstraint() {
            // given
            ProviderComponent reporting = new ProviderComponent(new GlobalSequenceTrackingToken(5));
            ProviderComponent empty = new ProviderComponent(null);

            // when
            TrackingToken result =
                    SafePointConfigurationEnhancer.mergedSafePoint(List.of(reporting, empty), SEGMENT).join();

            // then
            assertThat(result).isEqualTo(new GlobalSequenceTrackingToken(5));
        }
    }

    /**
     * Provider implementation: an {@link EventHandlingComponent} that also reports a safe point.
     */
    private static class ProviderComponent implements EventHandlingComponent, SegmentSafePointProvider {

        private final @Nullable TrackingToken safePoint;

        ProviderComponent(@Nullable TrackingToken safePoint) {
            this.safePoint = safePoint;
        }

        @Override
        public CompletableFuture<@Nullable TrackingToken> onSegmentClaimed(@NonNull Segment segment) {
            return CompletableFuture.completedFuture(safePoint);
        }

        @Override
        public MessageStream.@NonNull Empty<Message> handle(@NonNull EventMessage event,
                                                            @NonNull ProcessingContext context) {
            return MessageStream.empty();
        }

        @NonNull
        @Override
        public Object sequenceIdentifierFor(@NonNull EventMessage event, @NonNull ProcessingContext context) {
            return "provider";
        }

        @Override
        public MessageStream.@NonNull Empty<Message> handle(@NonNull ResetContext resetContext,
                                                            @NonNull ProcessingContext context) {
            return MessageStream.empty();
        }

        @Override
        public MessageStream.@NonNull Empty<Message> handle(@NonNull ReplayStatusChanged statusChange,
                                                            @NonNull ProcessingContext context) {
            return MessageStream.empty();
        }

        @NonNull
        @Override
        public Set<QualifiedName> supportedEvents() {
            return Set.of();
        }

        @Override
        public void describeTo(@NonNull ComponentDescriptor descriptor) {
            // leaf; no decorator chain
        }
    }

    private static class StubEventHandlingComponent implements EventHandlingComponent {

        @Override
        public MessageStream.@NonNull Empty<Message> handle(@NonNull EventMessage event,
                                                            @NonNull ProcessingContext context) {
            return MessageStream.empty();
        }

        @NonNull
        @Override
        public Object sequenceIdentifierFor(@NonNull EventMessage event, @NonNull ProcessingContext context) {
            return "stub";
        }

        @Override
        public MessageStream.@NonNull Empty<Message> handle(@NonNull ResetContext resetContext,
                                                            @NonNull ProcessingContext context) {
            return MessageStream.empty();
        }

        @Override
        public MessageStream.@NonNull Empty<Message> handle(@NonNull ReplayStatusChanged statusChange,
                                                            @NonNull ProcessingContext context) {
            return MessageStream.empty();
        }

        @NonNull
        @Override
        public Set<QualifiedName> supportedEvents() {
            return Set.of();
        }

        @Override
        public void describeTo(@NonNull ComponentDescriptor descriptor) {
            // leaf
        }
    }

    private static class TestDecorator extends DelegatingEventHandlingComponent {

        TestDecorator(EventHandlingComponent delegate) {
            super(delegate);
        }
    }
}
