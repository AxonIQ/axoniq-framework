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

package io.axoniq.framework.messaging.eventstreaming.checkpoint;

import org.axonframework.common.FutureUtils;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.eventhandling.AsyncInMemoryStreamableEventSource;
import org.axonframework.messaging.eventhandling.EventTestUtils;
import org.axonframework.messaging.eventhandling.annotation.EventHandler;
import org.axonframework.messaging.eventhandling.configuration.EventProcessorModule;
import org.axonframework.messaging.eventhandling.processing.streaming.pooled.PooledStreamingEventProcessorModule;
import org.axonframework.messaging.eventhandling.processing.streaming.progress.SegmentProgressStrategyFactory;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.inmemory.InMemoryTokenStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Tests for the {@link CheckpointingConfigurationEnhancer}'s interplay with user-supplied customizations: the
 * enhancer must install checkpointing detection <em>without</em> displacing the application's
 * {@link PooledStreamingEventProcessorModule.Customization} component (which carries the user's
 * {@code pooledStreaming(...).defaults(...)}), so user defaults run after it and can override the
 * progress-strategy selection. The default (enhancer-only) path is covered end-to-end by
 * {@link AnnotatedCheckpointingProjectionTest}.
 */
class CheckpointingConfigurationEnhancerTest {

    private static final String PROCESSOR_NAME = "enhancer-test";
    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    private AxonConfiguration configuration;

    @AfterEach
    void tearDown() {
        if (configuration != null) {
            configuration.shutdown();
        }
    }

    @Test
    void userConfiguredDefaultsArePreservedAndOverrideCheckpointingDetection() {
        // given -- a Checkpointing projection, but user defaults forcing the token-storing strategy
        AsyncInMemoryStreamableEventSource eventSource = new AsyncInMemoryStreamableEventSource();
        InMemoryTokenStore tokenStore = new InMemoryTokenStore();
        NeverFlushingProjection projection = new NeverFlushingProjection();

        var module =
                EventProcessorModule.pooledStreaming(PROCESSOR_NAME)
                                    .eventHandlingComponents(components -> components.autodetected(
                                            "projection", cfg -> projection
                                    ))
                                    .customized((cfg, c) -> c.eventSource(eventSource)
                                                             .tokenStore(tokenStore)
                                                             .initialSegmentCount(1));

        configuration = MessagingConfigurer.create()
                                           .eventProcessing(ep -> ep.pooledStreaming(
                                                   ps -> ps.defaults((cfg, c) -> c.progressStrategyFactoryBuilder(
                                                                   components -> SegmentProgressStrategyFactory.tokenStoring()
                                                           ))
                                                           .processor(module)
                                           ))
                                           .build();
        configuration.start();

        // when -- events are handled while the projection never requests a checkpoint
        eventSource.publishMessage(EventTestUtils.asEventMessage("event-0"));
        eventSource.publishMessage(EventTestUtils.asEventMessage("event-1"));
        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(projection.handled).hasSize(2));

        // then -- the user's token-storing choice won: the stored token advances without any checkpoint request
        await().atMost(TIMEOUT).untilAsserted(
                () -> assertThat(storedPosition(tokenStore)).isEqualTo(lastPositionOf(projection))
        );
    }

    private static long storedPosition(TokenStore tokenStore) {
        TrackingToken token = FutureUtils.joinAndUnwrap(tokenStore.fetchToken(PROCESSOR_NAME, 0, null));
        return token == null ? -1L : token.position().orElse(-1L);
    }

    private static long lastPositionOf(NeverFlushingProjection projection) {
        TrackingToken lastHandled = projection.lastHandled;
        return lastHandled == null ? Long.MIN_VALUE : lastHandled.position().orElse(Long.MIN_VALUE);
    }

    /**
     * A {@link Checkpointing} projection that never requests a checkpoint: under checkpointing detection its
     * stored token would never advance, so an advancing token proves the strategy was overridden.
     */
    static class NeverFlushingProjection implements Checkpointing {

        private final List<String> handled = new CopyOnWriteArrayList<>();
        private volatile TrackingToken lastHandled;

        @EventHandler
        void on(String event, TrackingToken token) {
            this.lastHandled = token;
            handled.add(event);
        }

        @Override
        public CompletableFuture<TrackingToken> onCheckpointAdvanced(Segment segment, TrackingToken requested) {
            return CompletableFuture.completedFuture(requested);
        }
    }
}
