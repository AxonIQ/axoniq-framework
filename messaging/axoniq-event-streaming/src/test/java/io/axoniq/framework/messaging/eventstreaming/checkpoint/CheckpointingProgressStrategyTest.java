/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.eventstreaming.checkpoint;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.test.appender.ListAppender;
import org.axonframework.messaging.eventhandling.processing.streaming.progress.SegmentProgressStrategyFactory;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Test class running the {@link CheckpointingProgressStrategyTestSuite} against the framework's
 * {@link CheckpointingProgressStrategy}.
 *
 * @author Allard Buijze
 */
class CheckpointingProgressStrategyTest extends CheckpointingProgressStrategyTestSuite {

    @Override
    protected SegmentProgressStrategyFactory checkpointingStrategy(boolean autoCheckpointing,
                                                                   List<Checkpointing> participants) {
        return context -> new CheckpointingProgressStrategy(context, participants, autoCheckpointing);
    }

    @Nested
    class AgreementWarning {

        private static final Duration WARNING_THRESHOLD = Duration.ofMillis(200);

        private Logger strategyLogger;
        private Level previousLevel;
        private ListAppender appender;

        @BeforeEach
        void attachAppender() {
            strategyLogger = (Logger) LogManager.getLogger(CheckpointingProgressStrategy.class);
            previousLevel = strategyLogger.getLevel();
            strategyLogger.setLevel(Level.WARN);
            appender = new ListAppender("CheckpointingProgressStrategyAgreementWarning");
            appender.start();
            strategyLogger.addAppender(appender);
        }

        @AfterEach
        void detachAppender() {
            strategyLogger.removeAppender(appender);
            appender.stop();
            strategyLogger.setLevel(previousLevel);
        }

        @Test
        void anAgreementThatNeverCompletesLogsOneWarningWhileTheWorkerKeepsHandingOverEvents() {
            // given -- a participant that never reaches the requested position
            NeverAgreeingCheckpointing participant = new NeverAgreeingCheckpointing();
            WorkPackageHarness testSubject = harness(context -> new CheckpointingProgressStrategy(
                    context, List.of(participant), false, WARNING_THRESHOLD
            ));
            testSubject.onSegmentClaimed();
            participant.trigger.get().requestCheckpoint(new GlobalSequenceTrackingToken(5L));

            // when -- events keep arriving while the agreement stays open
            testSubject.scheduleEvent(eventAt(6L));
            testSubject.scheduleEvent(eventAt(7L));

            // then -- the worker hands them over, logs one warning once the threshold passes, and stores nothing
            await().atMost(Duration.ofSeconds(2))
                   .untilAsserted(() -> assertThat(testSubject.batchProcessor().processed()).hasSize(2));
            await().during(WARNING_THRESHOLD.multipliedBy(2)).atMost(Duration.ofSeconds(2))
                   .untilAsserted(() -> assertThat(warnings()).hasSize(1));
            assertThat(participant.asked).hasValue(1);
            verify(tokenStore(), never()).storeToken(any(), anyString(), anyInt(), any());
        }

        private List<LogEvent> warnings() {
            return appender.getEvents()
                           .stream()
                           .filter(event -> event.getLevel() == Level.WARN)
                           .toList();
        }
    }

    private static class NeverAgreeingCheckpointing implements Checkpointing {

        private final AtomicReference<CheckpointTrigger> trigger = new AtomicReference<>();
        private final AtomicInteger asked = new AtomicInteger();

        @Override
        public void onSegmentClaimed(Segment segment, @Nullable TrackingToken from, CheckpointTrigger trigger) {
            this.trigger.set(trigger);
        }

        @Override
        public CompletableFuture<TrackingToken> onCheckpointAdvanced(Segment segment, TrackingToken requested) {
            asked.incrementAndGet();
            return new CompletableFuture<>();
        }
    }
}
