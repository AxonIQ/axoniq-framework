/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.eventstreaming.checkpoint;

import org.axonframework.messaging.eventhandling.DelegatingEventHandlingComponent;
import org.axonframework.messaging.eventhandling.EventHandlingComponent;
import org.axonframework.messaging.eventhandling.SimpleEventHandlingComponent;
import org.axonframework.messaging.eventhandling.processing.streaming.progress.SegmentProgressStrategyFactory;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.inmemory.InMemoryTokenStore;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests whether a {@link CheckpointingProgressStrategyFactory} reports that its processor runs fully-deferred.
 *
 * @author Allard Buijze
 */
class CheckpointingProgressStrategyFactoryTest {

    @Nested
    class IsFullyDeferred {

        @Test
        void isFullyDeferredWithoutAutoCheckpointing() {
            // given
            var factory = new CheckpointingProgressStrategyFactory(List.of(new CheckpointingComponent()), false,
                                                                   new InMemoryTokenStore(), "processor");

            // when
            boolean fullyDeferred = factory.isFullyDeferred();

            // then
            assertThat(fullyDeferred).isTrue();
        }

        @Test
        void isNotFullyDeferredWithAutoCheckpointing() {
            // given
            var factory = new CheckpointingProgressStrategyFactory(List.of(new CheckpointingComponent()), true,
                                                                   new InMemoryTokenStore(), "processor");

            // when
            boolean fullyDeferred = factory.isFullyDeferred();

            // then
            assertThat(fullyDeferred).isFalse();
        }

        @Test
        void isFullyDeferredWhenEveryDetectedComponentCheckpointsOnItsOwn() {
            // given
            List<EventHandlingComponent> components = List.of(new CheckpointingComponent(),
                                                              new CheckpointingComponent());

            // when
            SegmentProgressStrategyFactory factory =
                    CheckpointingProgressStrategyFactory.detecting("processor", InMemoryTokenStore::new)
                                                        .apply(components);

            // then
            assertThat(factory).isInstanceOfSatisfying(
                    CheckpointingProgressStrategyFactory.class,
                    checkpointing -> assertThat(checkpointing.isFullyDeferred()).isTrue()
            );
        }

        @Test
        void isNotFullyDeferredWhenAnOrdinaryComponentIsDetectedAsWell() {
            // given
            List<EventHandlingComponent> components = List.of(new CheckpointingComponent(),
                                                              SimpleEventHandlingComponent.create("ordinary"));

            // when
            SegmentProgressStrategyFactory factory =
                    CheckpointingProgressStrategyFactory.detecting("processor", InMemoryTokenStore::new)
                                                        .apply(components);

            // then
            assertThat(factory).isInstanceOfSatisfying(
                    CheckpointingProgressStrategyFactory.class,
                    checkpointing -> assertThat(checkpointing.isFullyDeferred()).isFalse()
            );
        }
    }

    /**
     * An event handling component that checkpoints on its own, completing every checkpoint at the requested token.
     */
    private static final class CheckpointingComponent extends DelegatingEventHandlingComponent
            implements Checkpointing {

        private CheckpointingComponent() {
            super(SimpleEventHandlingComponent.create("checkpointing"));
        }

        @Override
        public CompletableFuture<TrackingToken> onCheckpointAdvanced(Segment segment, TrackingToken requested) {
            return CompletableFuture.completedFuture(requested);
        }
    }
}
