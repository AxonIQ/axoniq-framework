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

package org.axonframework.test.deadline;

import org.axonframework.deadline.DeadlineMessage;
import org.axonframework.deadline.GenericDeadlineMessage;
import org.axonframework.messaging.Scope;
import org.axonframework.messaging.ScopeDescriptor;
import org.axonframework.messaging.core.EmptyApplicationContext;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.SimpleUnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.modelling.saga.SagaScopeDescriptor;
import org.axonframework.test.FixtureExecutionException;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StubDeadlineManagerTest {

    private static final Instant START = Instant.parse("2026-10-10T10:00:00Z");
    private static final ScopeDescriptor SCOPE = new SagaScopeDescriptor("MySaga", "saga-1");
    private static final ScopeDescriptor OTHER_SCOPE = new SagaScopeDescriptor("MySaga", "saga-2");

    private StubDeadlineManager testSubject;

    @BeforeEach
    void setUp() {
        testSubject = new StubDeadlineManager();
    }

    // Moved from Axon Framework 4 as it was written.
    @Test
    void messagesCarryTriggerTimestamp() throws Exception {
        Instant triggerTime = Instant.now().plusSeconds(60);
        MockScope.execute(() ->
                                  testSubject.schedule(triggerTime, "gone")
        );
        List<DeadlineMessage> triggered = new ArrayList<>();
        testSubject.advanceTimeBy(Duration.ofMinutes(75), (s, message) -> triggered.add(message));

        assertThat(triggered).hasSize(1);
        assertThat(triggered.get(0).timestamp()).isEqualTo(triggerTime);
    }

    @Nested
    class Scheduling {

        @Test
        void aDurationIsCountedFromTheStubsCurrentTimeRatherThanTheWallClock() {
            // given
            testSubject.initializeAt(START);

            // when
            testSubject.schedule(Duration.ofMinutes(5), "reminder", "payload", SCOPE);

            // then
            assertThat(testSubject.getScheduledDeadlines())
                    .singleElement()
                    .satisfies(deadline -> {
                        assertThat(deadline.getScheduleTime()).isEqualTo(START.plus(Duration.ofMinutes(5)));
                        assertThat(deadline.getDeadlineName()).isEqualTo("reminder");
                        assertThat(deadline.getDeadlineScope()).isEqualTo(SCOPE);
                        assertThat(deadline.deadlineMessage().payload()).isEqualTo("payload");
                    });
        }

        @Test
        void theScheduleIdIsTheIdentifierOfTheScheduledMessage() {
            // when
            String scheduleId = testSubject.schedule(Duration.ofMinutes(5), "reminder", "payload", SCOPE);

            // then
            assertThat(testSubject.getScheduledDeadlines())
                    .singleElement()
                    .satisfies(deadline -> {
                        assertThat(deadline.getScheduleId()).isEqualTo(scheduleId);
                        assertThat(deadline.deadlineMessage().identifier()).isEqualTo(scheduleId);
                    });
        }

        @Test
        void aGivenMessageIsScheduledWithItsOwnTypeAndMetadata() {
            // given
            Message message = new GenericMessage(new MessageType("custom.Reminder"), "payload")
                    .andMetadata(java.util.Map.of("key", "value"));

            // when
            testSubject.schedule(Duration.ofMinutes(5), "reminder", message, SCOPE);

            // then
            DeadlineMessage scheduled = testSubject.getScheduledDeadlines().getFirst().deadlineMessage();
            assertThat(scheduled.type()).isEqualTo(new MessageType("custom.Reminder"));
            assertThat(scheduled.metadata()).containsEntry("key", "value");
        }

        @Test
        void theDispatchInterceptorsDecideWhatIsScheduled() {
            // given
            testSubject.registerDispatchInterceptor(
                    (message, context, chain) -> chain.proceed(withPayload(message, "intercepted"), context)
            );

            // when
            testSubject.schedule(Duration.ofMinutes(5), "reminder", "payload", SCOPE);

            // then
            assertThat(testSubject.getScheduledDeadlines().getFirst().deadlineMessage().payload())
                    .isEqualTo("intercepted");
        }

        @Test
        void aRemovedDispatchInterceptorNoLongerRuns() {
            // given
            testSubject.registerDispatchInterceptor(
                    (message, context, chain) -> chain.proceed(withPayload(message, "intercepted"), context)
            ).cancel();

            // when
            testSubject.schedule(Duration.ofMinutes(5), "reminder", "payload", SCOPE);

            // then
            assertThat(testSubject.getScheduledDeadlines().getFirst().deadlineMessage().payload())
                    .isEqualTo("payload");
        }

        @Test
        void initializingTheTimeFailsOnceADeadlineIsScheduled() {
            // given
            testSubject.schedule(Duration.ofMinutes(5), "reminder", "payload", SCOPE);

            // when / then
            assertThatThrownBy(() -> testSubject.initializeAt(START)).isInstanceOf(IllegalStateException.class);
        }
    }

    @Nested
    class Cancelling {

        @Test
        void cancelScheduleRemovesOnlyThatSchedule() {
            // given
            String cancelled = testSubject.schedule(Duration.ofMinutes(5), "reminder", "first", SCOPE);
            testSubject.schedule(Duration.ofMinutes(5), "reminder", "second", SCOPE);

            // when
            testSubject.cancelSchedule("reminder", cancelled);

            // then
            assertThat(testSubject.getScheduledDeadlines())
                    .extracting(deadline -> deadline.deadlineMessage().payload())
                    .containsExactly("second");
        }

        @Test
        void cancelAllRemovesEveryDeadlineOfThatName() {
            // given
            testSubject.schedule(Duration.ofMinutes(5), "reminder", "first", SCOPE);
            testSubject.schedule(Duration.ofMinutes(5), "reminder", "second", OTHER_SCOPE);
            testSubject.schedule(Duration.ofMinutes(5), "other", "third", SCOPE);

            // when
            testSubject.cancelAll("reminder");

            // then
            assertThat(testSubject.getScheduledDeadlines())
                    .extracting(ScheduledDeadlineInfo::getDeadlineName)
                    .containsExactly("other");
        }

        @Test
        void cancelAllWithinScopeRemovesTheDeadlinesOfThatNameAndScopeOnly() {
            // given
            testSubject.schedule(Duration.ofMinutes(5), "reminder", "first", SCOPE);
            testSubject.schedule(Duration.ofMinutes(5), "reminder", "second", OTHER_SCOPE);
            testSubject.schedule(Duration.ofMinutes(5), "other", "third", SCOPE);

            // when
            testSubject.cancelAllWithinScope("reminder", SCOPE);

            // then
            assertThat(testSubject.getScheduledDeadlines())
                    .extracting(deadline -> deadline.deadlineMessage().payload())
                    .containsExactlyInAnyOrder("second", "third");
        }
    }

    @Nested
    class Firing {

        @Test
        void onlyTheDeadlinesDueByTheNewTimeFireAndTheyFireInScheduleOrder() {
            // given
            testSubject.initializeAt(START);
            testSubject.schedule(Duration.ofMinutes(10), "reminder", "late", SCOPE);
            testSubject.schedule(Duration.ofMinutes(5), "reminder", "early", SCOPE);
            testSubject.schedule(Duration.ofMinutes(30), "reminder", "not-yet", SCOPE);
            List<Object> fired = new ArrayList<>();

            // when
            testSubject.advanceTimeBy(Duration.ofMinutes(10), (scope, message) -> fired.add(message.payload()));

            // then
            assertThat(fired).containsExactly("early", "late");
            assertThat(testSubject.getCurrentDateTime()).isEqualTo(START.plus(Duration.ofMinutes(10)));
            assertThat(testSubject.getScheduledDeadlines())
                    .extracting(deadline -> deadline.deadlineMessage().payload())
                    .containsExactly("not-yet");
            assertThat(testSubject.getTriggeredDeadlines())
                    .extracting(deadline -> deadline.deadlineMessage().payload())
                    .containsExactly("early", "late");
        }

        @Test
        void advancingToAnEarlierTimeDoesNotMoveTheClockBack() {
            // given
            testSubject.initializeAt(START);

            // when
            testSubject.advanceTimeTo(START.minusSeconds(60), (scope, message) -> {
            });

            // then
            assertThat(testSubject.getCurrentDateTime()).isEqualTo(START);
        }

        @Test
        void aDeadlineFiresWithTheScopeItWasScheduledFor() {
            // given
            testSubject.schedule(Duration.ofMinutes(5), "reminder", "payload", SCOPE);
            AtomicReference<ScopeDescriptor> firedScope = new AtomicReference<>();

            // when
            testSubject.advanceTimeBy(Duration.ofMinutes(5), (scope, message) -> firedScope.set(scope));

            // then
            assertThat(firedScope).hasValue(SCOPE);
        }

        @Test
        void aDeadlineScheduledWhileFiringFiresInTheSameAdvanceWhenItIsDueByThen() {
            // given
            testSubject.initializeAt(START);
            testSubject.schedule(Duration.ofSeconds(5), "retry", 1, SCOPE);
            List<Object> fired = new ArrayList<>();

            // when
            testSubject.advanceTimeBy(Duration.ofSeconds(12), (scope, message) -> {
                fired.add(message.payload());
                testSubject.schedule(Duration.ofSeconds(5), "retry", ((Integer) message.payload()) + 1, scope);
            });

            // then
            assertThat(fired).containsExactly(1, 2);
            assertThat(testSubject.getScheduledDeadlines())
                    .extracting(deadline -> deadline.deadlineMessage().payload())
                    .containsExactly(3);
        }

        @Test
        void aDeadlineFiresInAUnitOfWorkOfTheGivenFactoryWhoseContextCarriesTheDeadline() {
            // given
            AtomicInteger createdUnitsOfWork = new AtomicInteger();
            UnitOfWorkFactory delegate = new SimpleUnitOfWorkFactory(EmptyApplicationContext.INSTANCE);
            UnitOfWorkFactory countingFactory = (identifier, customization) -> {
                createdUnitsOfWork.incrementAndGet();
                return delegate.create(identifier, customization);
            };
            testSubject = new StubDeadlineManager(START, countingFactory);
            testSubject.schedule(Duration.ofMinutes(5), "reminder", "payload", SCOPE);
            AtomicReference<Message> messageInContext = new AtomicReference<>();
            AtomicReference<DeadlineMessage> consumedMessage = new AtomicReference<>();

            // when
            testSubject.advanceTimeBy(Duration.ofMinutes(5), new DeadlineConsumer() {
                @Override
                public void consume(ScopeDescriptor deadlineScope, DeadlineMessage deadlineMessage) {
                    throw new AssertionError("The stub should hand the deadline over with its processing context");
                }

                @Override
                public void consume(ScopeDescriptor deadlineScope,
                                    DeadlineMessage deadlineMessage,
                                    ProcessingContext context) {
                    consumedMessage.set(deadlineMessage);
                    messageInContext.set(Message.fromContext(context));
                }
            });

            // then
            assertThat(createdUnitsOfWork).hasValue(1);
            assertThat(messageInContext.get()).isSameAs(consumedMessage.get());
        }

        @Test
        void theHandlerInterceptorsRunAroundTheConsumerAndTheTriggeredDeadlineIsTheOneTheyPassedOn() {
            // given
            List<String> invocations = new ArrayList<>();
            testSubject.registerHandlerInterceptor((message, context, chain) -> {
                invocations.add("interceptor saw " + message.payload());
                assertThat(Message.fromContext(context)).isSameAs(message);
                return chain.proceed(withPayload(message, "intercepted"), context);
            });
            testSubject.schedule(Duration.ofMinutes(5), "reminder", "payload", SCOPE);

            // when
            testSubject.advanceTimeBy(Duration.ofMinutes(5),
                                      (scope, message) -> invocations.add("consumer saw " + message.payload()));

            // then
            assertThat(invocations).containsExactly("interceptor saw payload", "consumer saw intercepted");
            assertThat(testSubject.getTriggeredDeadlines())
                    .singleElement()
                    .satisfies(deadline -> assertThat(deadline.deadlineMessage().payload())
                            .isEqualTo("intercepted"));
        }

        @Test
        void aRemovedHandlerInterceptorNoLongerRuns() {
            // given
            List<String> invocations = new ArrayList<>();
            testSubject.registerHandlerInterceptor((message, context, chain) -> {
                invocations.add("interceptor");
                return chain.proceed(message, context);
            }).cancel();
            testSubject.schedule(Duration.ofMinutes(5), "reminder", "payload", SCOPE);

            // when
            testSubject.advanceTimeBy(Duration.ofMinutes(5), (scope, message) -> invocations.add("consumer"));

            // then
            assertThat(invocations).containsExactly("consumer");
        }

        @Test
        void aFailingConsumerIsReportedAsAFixtureExecutionException() {
            // given
            IllegalStateException failure = new IllegalStateException("handler failed");
            testSubject.schedule(Duration.ofMinutes(5), "reminder", "payload", SCOPE);

            // when / then
            assertThatThrownBy(() -> testSubject.advanceTimeBy(Duration.ofMinutes(5), (scope, message) -> {
                throw failure;
            }))
                    .isInstanceOf(FixtureExecutionException.class)
                    .hasMessage("Exception occurred while handling the deadline")
                    .cause().isSameAs(failure);
        }

        @Test
        void advanceToNextTriggerMovesTheClockToTheNextDeadlineWithoutFiringIt() {
            // given
            testSubject.initializeAt(START);
            testSubject.schedule(Duration.ofMinutes(5), "reminder", "payload", SCOPE);

            // when
            ScheduledDeadlineInfo next = testSubject.advanceToNextTrigger();

            // then
            assertThat(next.deadlineMessage().payload()).isEqualTo("payload");
            assertThat(testSubject.getCurrentDateTime()).isEqualTo(START.plus(Duration.ofMinutes(5)));
            assertThat(testSubject.getScheduledDeadlines()).isEmpty();
            assertThat(testSubject.getTriggeredDeadlines()).containsExactly(next);
        }
    }

    private static DeadlineMessage withPayload(DeadlineMessage message, Object payload) {
        return new GenericDeadlineMessage(message.getDeadlineName(),
                                          new GenericMessage(message.type(), payload, message.metadata()),
                                          message::timestamp);
    }

    private static class MockScope extends Scope {

        private static final MockScope instance = new MockScope();

        public static void execute(Runnable task) throws Exception {
            instance.executeWithResult(() -> {
                task.run();
                return null;
            });
        }

        @Override
        public ScopeDescriptor describeScope() {
            return (ScopeDescriptor) () -> "Mock";
        }
    }
}
