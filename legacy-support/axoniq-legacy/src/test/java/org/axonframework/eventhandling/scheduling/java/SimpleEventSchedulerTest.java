/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package org.axonframework.eventhandling.scheduling.java;

import org.axonframework.common.AxonConfigurationException;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.eventhandling.scheduling.ScheduleToken;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkTestUtils;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventSink;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.SimpleEventBus;
import org.axonframework.messaging.eventhandling.annotation.Event;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests validating the {@link SimpleEventScheduler}.
 *
 * @author Allard Buijze
 * @author Nakul Mishra
 * @author Jakob Hatzl
 */
@SuppressWarnings("deprecation")
class SimpleEventSchedulerTest {

    private static final Duration SHORT = Duration.ofMillis(20);
    /**
     * The delay of an event a test cancels or replaces before it fires, long enough that a slow test thread still gets
     * to it first.
     */
    private static final Duration CANCEL_WINDOW = Duration.ofMillis(500);
    /**
     * The executor waits on {@link System#nanoTime()}, which drifts slightly from the wall clock the timestamps are
     * taken from.
     */
    private static final Duration CLOCK_TOLERANCE = Duration.ofMillis(5);

    private ScheduledExecutorService scheduledExecutorService;
    private BlockingQueue<Published> published;
    private SimpleEventBus eventBus;

    private SimpleEventScheduler testSubject;

    @BeforeEach
    void setUp() {
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        published = new LinkedBlockingQueue<>();
        eventBus = new SimpleEventBus();
        eventBus.subscribe((events, context) -> {
            events.forEach(event -> published.add(new Published(event, context)));
            return CompletableFuture.completedFuture(null);
        });
        testSubject = SimpleEventScheduler.builder()
                                          .scheduledExecutorService(scheduledExecutorService)
                                          .eventSink(eventBus)
                                          .unitOfWorkFactory(UnitOfWorkTestUtils.SIMPLE_FACTORY)
                                          .build();
    }

    @AfterEach
    void tearDown() {
        scheduledExecutorService.shutdownNow();
    }

    @Nested
    class Schedule {

        @Test
        void publishesPayloadAsNewEventInUnitOfWork() throws InterruptedException {
            // given
            Instant scheduledAt = Instant.now();

            // when
            testSubject.schedule(SHORT, "payload");

            // then
            Published result = awaitPublished();
            assertThat(result.context()).isNotNull();
            assertThat(result.event().payload()).isEqualTo("payload");
            assertThat(result.event().type()).isEqualTo(new MessageType(String.class));
            assertThat(result.event().metadata()).isEmpty();
            assertThat(result.event().timestamp()).isAfterOrEqualTo(scheduledAt.plus(SHORT).minus(CLOCK_TOLERANCE));
        }

        @Test
        void publishesEventMessageWithNewIdentifierAndTimestamp() throws InterruptedException {
            // given
            EventMessage scheduled = new GenericEventMessage(new MessageType("custom.Event", "2"),
                                                             "payload",
                                                             Metadata.with("key", "value"));

            // when
            testSubject.schedule(SHORT, scheduled);

            // then
            EventMessage result = awaitPublished().event();
            assertThat(result.identifier()).isNotEqualTo(scheduled.identifier());
            assertThat(result.timestamp()).isAfter(scheduled.timestamp());
            assertThat(result.type()).isEqualTo(scheduled.type());
            assertThat(result.payload()).isEqualTo("payload");
            assertThat(result.metadata()).isEqualTo(scheduled.metadata());
        }

        @Test
        void publishesAtGivenInstant() throws InterruptedException {
            // given
            Instant triggerDateTime = Instant.now().plus(SHORT);

            // when
            testSubject.schedule(triggerDateTime, "payload");

            // then
            assertThat(awaitPublished().event().timestamp()).isAfterOrEqualTo(triggerDateTime.minus(CLOCK_TOLERANCE));
        }

        @Test
        void resolvesMessageTypeFromEventAnnotationByDefault() throws InterruptedException {
            // when
            testSubject.schedule(SHORT, new NamedEvent("payload"));

            // then
            assertThat(awaitPublished().event().type()).isEqualTo(new MessageType("scheduling.NamedEvent", "2"));
        }

        @Test
        void resolvesMessageTypeWithConfiguredResolver() throws InterruptedException {
            // given
            MessageType customType = new MessageType("custom.Event");
            SimpleEventScheduler customSubject =
                    SimpleEventScheduler.builder()
                                        .scheduledExecutorService(scheduledExecutorService)
                                        .eventSink(eventBus)
                                        .unitOfWorkFactory(UnitOfWorkTestUtils.SIMPLE_FACTORY)
                                        .messageTypeResolver(payloadType -> Optional.of(customType))
                                        .build();

            // when
            customSubject.schedule(SHORT, "payload");

            // then
            assertThat(awaitPublished().event().type()).isEqualTo(customType);
        }

        @Test
        void unresolvableMessageTypePublishesNothing() throws InterruptedException {
            // given
            SimpleEventScheduler customSubject =
                    SimpleEventScheduler.builder()
                                        .scheduledExecutorService(scheduledExecutorService)
                                        .eventSink(eventBus)
                                        .unitOfWorkFactory(UnitOfWorkTestUtils.SIMPLE_FACTORY)
                                        .messageTypeResolver(payloadType -> Optional.empty())
                                        .build();

            // when
            customSubject.schedule(SHORT, "payload");
            // an event scheduled after the failing one fires once the failing one has been handled
            testSubject.schedule(SHORT.multipliedBy(2), "next");

            // then
            assertThat(awaitPublished().event().payload()).isEqualTo("next");
            assertThat(published).isEmpty();
        }

        @Test
        void failingPublicationDoesNotStopLaterEvents() throws InterruptedException {
            // given
            SimpleEventScheduler customSubject =
                    SimpleEventScheduler.builder()
                                        .scheduledExecutorService(scheduledExecutorService)
                                        .eventSink(new FailingEventSink("failing", eventBus))
                                        .unitOfWorkFactory(UnitOfWorkTestUtils.SIMPLE_FACTORY)
                                        .build();

            // when
            customSubject.schedule(SHORT, "failing");
            // an event scheduled after the failing one fires once the failing one has been handled
            customSubject.schedule(SHORT.multipliedBy(2), "next");

            // then
            assertThat(awaitPublished().event().payload()).isEqualTo("next");
            assertThat(published).isEmpty();
        }

        @SuppressWarnings("DataFlowIssue")
        @Test
        void rejectsNullEvent() {
            // when / then
            assertThatThrownBy(() -> testSubject.schedule(SHORT, null))
                    .isInstanceOf(NullPointerException.class);
            assertThatThrownBy(() -> testSubject.schedule(Instant.now(), null))
                    .isInstanceOf(NullPointerException.class);
        }

        @SuppressWarnings("DataFlowIssue")
        @Test
        void rejectsNullTrigger() {
            // when / then
            assertThatThrownBy(() -> testSubject.schedule((Duration) null, "payload"))
                    .isInstanceOf(NullPointerException.class);
            assertThatThrownBy(() -> testSubject.schedule((Instant) null, "payload"))
                    .isInstanceOf(NullPointerException.class);
        }
    }

    @Nested
    class CancelSchedule {

        @Test
        void cancelledEventIsNotPublished() throws InterruptedException {
            // given
            ScheduleToken cancelled = testSubject.schedule(CANCEL_WINDOW, "cancelled");
            testSubject.schedule(CANCEL_WINDOW.plus(SHORT), "kept");

            // when
            testSubject.cancelSchedule(cancelled);

            // then
            assertThat(awaitPublished().event().payload()).isEqualTo("kept");
            assertThat(published).isEmpty();
        }

        @Test
        void tokenRestoredThroughJavaSerializationCancelsEvent()
                throws IOException, ClassNotFoundException, InterruptedException {
            // given
            ScheduleToken cancelled = testSubject.schedule(CANCEL_WINDOW, "cancelled");
            testSubject.schedule(CANCEL_WINDOW.plus(SHORT), "kept");
            // a Saga holding the token in its state stores and restores it, as with Java serialization
            ScheduleToken restored = javaSerializationRoundTrip(cancelled);

            // when
            testSubject.cancelSchedule(restored);

            // then
            assertThat(awaitPublished().event().payload()).isEqualTo("kept");
            assertThat(published).isEmpty();
        }

        @Test
        void cancellingPublishedEventDoesNothing() throws InterruptedException {
            // given
            ScheduleToken token = testSubject.schedule(SHORT, "payload");
            awaitPublished();

            // when / then
            assertThatCode(() -> testSubject.cancelSchedule(token)).doesNotThrowAnyException();
        }

        @Test
        void cancellingForeignTokenThrowsIllegalArgumentException() {
            // given
            ScheduleToken foreignToken = new ScheduleToken() {
            };

            // when / then
            assertThatThrownBy(() -> testSubject.cancelSchedule(foreignToken))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Nested
    class Reschedule {

        @Test
        void replacesScheduledEvent() throws InterruptedException {
            // given
            ScheduleToken original = testSubject.schedule(CANCEL_WINDOW, "original");

            // when
            ScheduleToken replacement = testSubject.reschedule(original, CANCEL_WINDOW.plus(SHORT), "replacement");

            // then
            assertThat(replacement).isNotEqualTo(original);
            assertThat(awaitPublished().event().payload()).isEqualTo("replacement");
            assertThat(published.poll(SHORT.multipliedBy(5).toMillis(), TimeUnit.MILLISECONDS)).isNull();
        }

        @Test
        void schedulesWithoutToken() throws InterruptedException {
            // when
            testSubject.reschedule(null, Instant.now().plus(SHORT), "payload");

            // then
            assertThat(awaitPublished().event().payload()).isEqualTo("payload");
        }
    }

    @Nested
    class Shutdown {

        @Test
        void shutsDownExecutorAndAllowsRepeatedCalls() {
            // when
            testSubject.shutdown();
            testSubject.shutdown();

            // then
            assertThat(scheduledExecutorService.isShutdown()).isTrue();
        }
    }

    @Nested
    class Builder {

        @Test
        void requiresEventSink() {
            // given
            SimpleEventScheduler.Builder builder =
                    SimpleEventScheduler.builder().unitOfWorkFactory(UnitOfWorkTestUtils.SIMPLE_FACTORY);

            // when / then
            assertThatThrownBy(builder::build).isInstanceOf(AxonConfigurationException.class);
        }

        @Test
        void requiresUnitOfWorkFactory() {
            // given
            SimpleEventScheduler.Builder builder = SimpleEventScheduler.builder().eventSink(eventBus);

            // when / then
            assertThatThrownBy(builder::build).isInstanceOf(AxonConfigurationException.class);
        }

        @Test
        void defaultsScheduledExecutorService() throws InterruptedException {
            // given
            SimpleEventScheduler defaultSubject =
                    SimpleEventScheduler.builder()
                                        .eventSink(eventBus)
                                        .unitOfWorkFactory(UnitOfWorkTestUtils.SIMPLE_FACTORY)
                                        .build();

            try {
                // when
                defaultSubject.schedule(SHORT, "payload");

                // then
                assertThat(awaitPublished().event().payload()).isEqualTo("payload");
            } finally {
                defaultSubject.shutdown();
            }
        }
    }

    private Published awaitPublished() throws InterruptedException {
        Published result = published.poll(5, TimeUnit.SECONDS);
        assertThat(result).as("published event").isNotNull();
        return result;
    }

    private static ScheduleToken javaSerializationRoundTrip(ScheduleToken token)
            throws IOException, ClassNotFoundException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(token);
        }
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            return (ScheduleToken) in.readObject();
        }
    }

    private record Published(EventMessage event, ProcessingContext context) {

    }

    /**
     * An {@link EventSink} failing to publish events with the given payload, and handing every other event to its
     * delegate.
     */
    private record FailingEventSink(Object failingPayload, EventSink delegate) implements EventSink {

        @Override
        public CompletableFuture<Void> publish(@Nullable ProcessingContext context,
                                               List<? extends EventMessage> events) {
            if (events.stream().anyMatch(event -> failingPayload.equals(event.payload()))) {
                return CompletableFuture.failedFuture(new IllegalStateException("Publication failed"));
            }
            return delegate.publish(context, events);
        }

        @Override
        public void describeTo(ComponentDescriptor descriptor) {
            descriptor.describeWrapperOf(delegate);
        }
    }

    @Event(namespace = "scheduling", name = "NamedEvent", version = "2")
    private record NamedEvent(String value) {

    }
}
