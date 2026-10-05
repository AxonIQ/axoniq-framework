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

package org.axonframework.deadline;

import org.axonframework.deadline.RecordingDeadlineManager.ScheduledCall;
import org.axonframework.messaging.ContextAwareScope;
import org.axonframework.messaging.ScopeDescriptor;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWork;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkTestUtils;
import org.axonframework.modelling.saga.repository.AnnotatedSagaRepository;
import org.junit.jupiter.api.*;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test class validating {@link AbstractDeadlineManager#runOnPrepareCommitOrNow(Runnable)} and
 * {@link AbstractDeadlineManager#processDispatchInterceptors(DeadlineMessage)}, driven through a recording subclass
 * shaped like the Axon Framework 4 {@code SimpleDeadlineManager}: it creates the message and schedule id up front and
 * defers the interception and the actual call.
 */
class AbstractDeadlineManagerTest {

    private static final ScopeDescriptor EXPLICIT_SCOPE = () -> "explicitScope";

    private List<String> timeline;
    private RecordingDeadlineManager testSubject;

    @BeforeEach
    void setUp() {
        timeline = new CopyOnWriteArrayList<>();
        testSubject = new RecordingDeadlineManager("manager", timeline);
    }

    @Nested
    class WithoutAnActiveScope {

        @Test
        void scheduleRunsImmediately() {
            // when
            testSubject.schedule(Instant.now(), "deadlineName", "payload", EXPLICIT_SCOPE);

            // then
            assertThat(testSubject.scheduled).hasSize(1);
        }

        @Test
        void cancelCallsRunImmediately() {
            // when
            testSubject.cancelSchedule("deadlineName", "scheduleId");
            testSubject.cancelAll("deadlineName");
            testSubject.cancelAllWithinScope("deadlineName", EXPLICIT_SCOPE);

            // then
            assertThat(timeline).containsExactly("manager:cancelSchedule deadlineName/scheduleId",
                                                 "manager:cancelAll deadlineName",
                                                 "manager:cancelAllWithinScope deadlineName@explicitScope");
        }

        /**
         * Axon Framework 4 threw here as well: the scope-less overloads ask for the current scope, and there is none.
         * Failing is what keeps a deadline from being stored under a scope nothing can resolve.
         */
        @Test
        void scheduleWithoutAScopeDescriptorThrows() {
            // when / then
            assertThatThrownBy(() -> testSubject.schedule(Instant.now(), "deadlineName", "payload"))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("Cannot request current Scope if none is active");
            assertThat(testSubject.scheduled).isEmpty();
        }

        @Test
        void cancelAllWithinScopeWithoutAScopeDescriptorThrows() {
            // when / then
            assertThatThrownBy(() -> testSubject.cancelAllWithinScope("deadlineName"))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("Cannot request current Scope if none is active");
            assertThat(timeline).isEmpty();
        }

        /**
         * Axon Framework 4 deferred whenever a unit of work was active. Axon Framework 5 has no ambient one, so
         * without a scope carrying the context there is nothing to defer to, even while a context is running.
         */
        @Test
        void aCallMadeWhileAContextRunsButNoScopeIsActiveRunsImmediately() {
            // given
            AtomicInteger scheduledDuringInvocation = new AtomicInteger(-1);

            // when
            runInUnitOfWork(context -> {
                testSubject.schedule(Instant.now(), "deadlineName", "payload", EXPLICIT_SCOPE);
                scheduledDuringInvocation.set(testSubject.scheduled.size());
            });

            // then
            assertThat(scheduledDuringInvocation).hasValue(1);
        }
    }

    @Nested
    class WithinAContextAwareScope {

        @Test
        void cancelCallsAreDeferredUntilTheContextPreparesItsCommit() {
            // given
            AtomicInteger callsDuringInvocation = new AtomicInteger(-1);

            // when
            runInUnitOfWork(context -> new TestScope(context).run(() -> {
                testSubject.cancelSchedule("deadlineName", "scheduleId");
                testSubject.cancelAll("deadlineName");
                testSubject.cancelAllWithinScope("deadlineName");
                callsDuringInvocation.set(timeline.size());
            }));

            // then
            assertThat(callsDuringInvocation).hasValue(0);
            assertThat(timeline).containsExactly("manager:cancelSchedule deadlineName/scheduleId",
                                                 "manager:cancelAll deadlineName",
                                                 "manager:cancelAllWithinScope deadlineName@testScope");
        }

        @Test
        void scheduleWithoutAScopeDescriptorUsesTheDescriptorOfTheCurrentScope() {
            // when
            runInUnitOfWork(context -> new TestScope(context).run(
                    () -> testSubject.schedule(Instant.now(), "deadlineName", "payload")
            ));

            // then
            assertThat(testSubject.scheduled).singleElement()
                                             .extracting(ScheduledCall::scope)
                                             .extracting(ScopeDescriptor::scopeDescription)
                                             .isEqualTo("testScope");
        }

        @Test
        void deferredCallsNeverRunWhenTheContextRollsBack() {
            // given
            UnitOfWork unitOfWork = UnitOfWorkTestUtils.aUnitOfWork();

            // when
            CompletableFuture<Object> result = unitOfWork.executeWithResult(context -> {
                new TestScope(context).run(() -> {
                    testSubject.schedule(Instant.now(), "deadlineName", "payload", EXPLICIT_SCOPE);
                    testSubject.cancelAll("deadlineName");
                });
                return CompletableFuture.failedFuture(new IllegalStateException("handler failure"));
            });

            // then
            assertThatThrownBy(() -> result.orTimeout(1, TimeUnit.SECONDS).join())
                    .hasRootCauseInstanceOf(IllegalStateException.class);
            assertThat(testSubject.scheduled).isEmpty();
            assertThat(timeline).isEmpty();
        }

        /**
         * A subscribing event processor fed by a {@code SimpleEventBus} invokes a Saga from within
         * {@code PREPARE_COMMIT}, where registering for {@code PREPARE_COMMIT} itself is rejected.
         */
        @Test
        void aCallMadeFromWithinPrepareCommitIsStillDeferredAndRuns() {
            // given
            UnitOfWork unitOfWork = UnitOfWorkTestUtils.aUnitOfWork();
            AtomicInteger scheduledDuringPrepareCommit = new AtomicInteger(-1);
            unitOfWork.runOnPrepareCommit(context -> new TestScope(context).run(() -> {
                testSubject.schedule(Instant.now(), "deadlineName", "payload", EXPLICIT_SCOPE);
                scheduledDuringPrepareCommit.set(testSubject.scheduled.size());
            }));

            // when
            unitOfWork.execute().orTimeout(1, TimeUnit.SECONDS).join();

            // then
            assertThat(scheduledDuringPrepareCommit).hasValue(0);
            assertThat(testSubject.scheduled).hasSize(1);
        }

        @Test
        void deferredCallsRunInTheOrderTheyWereMade() {
            // when
            runInUnitOfWork(context -> {
                new TestScope(context).run(() -> {
                    testSubject.schedule(Instant.now(), "first", "payload", EXPLICIT_SCOPE);
                    testSubject.cancelAll("first");
                });
                new TestScope(context).run(() -> testSubject.schedule(Instant.now(), "second", "payload",
                                                                     EXPLICIT_SCOPE));
            });

            // then
            assertThat(timeline).containsExactly("manager:schedule first",
                                                 "manager:cancelAll first",
                                                 "manager:schedule second");
        }

        @Test
        void deferredCallsRunAfterTheSagaWriteAndBeforeCommit() {
            // given
            UnitOfWork unitOfWork = UnitOfWorkTestUtils.aUnitOfWork();
            unitOfWork.runOn(AnnotatedSagaRepository.WRITE_SAGA, context -> timeline.add("sagaWrite"));
            unitOfWork.runOnCommit(context -> timeline.add("commit"));

            // when
            unitOfWork.executeWithResult(context -> {
                new TestScope(context).run(
                        () -> testSubject.schedule(Instant.now(), "deadlineName", "payload", EXPLICIT_SCOPE)
                );
                return CompletableFuture.completedFuture(null);
            }).orTimeout(1, TimeUnit.SECONDS).join();

            // then
            assertThat(timeline).containsExactly("sagaWrite", "manager:schedule deadlineName", "commit");
        }

        @Test
        void eachDeadlineManagerRunsItsOwnDeferredCalls() {
            // given
            RecordingDeadlineManager otherManager = new RecordingDeadlineManager("other", timeline);

            // when
            runInUnitOfWork(context -> new TestScope(context).run(() -> {
                testSubject.schedule(Instant.now(), "deadlineName", "payload", EXPLICIT_SCOPE);
                otherManager.schedule(Instant.now(), "deadlineName", "payload", EXPLICIT_SCOPE);
            }));

            // then
            assertThat(testSubject.scheduled).hasSize(1);
            assertThat(otherManager.scheduled).hasSize(1);
        }

        /**
         * As in an Axon Framework 4 prepare-commit, the first failing call stops the remaining ones and fails the
         * context. Calls that already ran are not undone.
         */
        @Test
        void aFailingDeferredCallStopsTheRemainingCallsAndFailsTheContext() {
            // given
            testSubject.registerDispatchInterceptor((message, context, chain) -> {
                if ("second".equals(message.getDeadlineName())) {
                    throw new IllegalStateException("second call failure");
                }
                return chain.proceed(message, context);
            });

            // when
            CompletableFuture<Object> result = UnitOfWorkTestUtils.aUnitOfWork().executeWithResult(context -> {
                new TestScope(context).run(() -> {
                    testSubject.schedule(Instant.now(), "first", "payload", EXPLICIT_SCOPE);
                    testSubject.schedule(Instant.now(), "second", "payload", EXPLICIT_SCOPE);
                    testSubject.schedule(Instant.now(), "third", "payload", EXPLICIT_SCOPE);
                });
                return CompletableFuture.completedFuture(null);
            });

            // then
            assertThatThrownBy(() -> result.orTimeout(1, TimeUnit.SECONDS).join())
                    .hasRootCauseInstanceOf(IllegalStateException.class)
                    .hasRootCauseMessage("second call failure");
            assertThat(timeline).containsExactly("manager:schedule first");
        }
    }

    /**
     * A call that reaches a context once the {@link AbstractDeadlineManager#RUN_DEADLINE_CALLS} phase started has
     * nothing left to run it. It must fail loudly instead of being dropped, whether or not earlier calls were deferred.
     */
    @Nested
    class AfterTheDeferralPhaseStarted {

        @Test
        void aLateCallFailsTheContextInsteadOfBeingDropped() {
            // given
            UnitOfWork unitOfWork = UnitOfWorkTestUtils.aUnitOfWork();

            // when
            CompletableFuture<Object> result = unitOfWork.executeWithResult(context -> {
                new TestScope(context).run(
                        () -> testSubject.schedule(Instant.now(), "first", "payload", EXPLICIT_SCOPE)
                );
                // Registered after the manager's own action, so direct execution runs it once the calls were drained.
                context.runOn(AbstractDeadlineManager.RUN_DEADLINE_CALLS,
                              phaseContext -> new TestScope(phaseContext).run(() -> testSubject.cancelAll("late")));
                return CompletableFuture.completedFuture(null);
            });

            // then
            assertThatThrownBy(() -> result.orTimeout(1, TimeUnit.SECONDS).join())
                    .hasRootCauseInstanceOf(IllegalStateException.class)
                    .hasRootCauseMessage("Cannot defer a deadline call to a ProcessingContext "
                                                 + "whose deferred deadline calls already ran");
            assertThat(timeline).containsExactly("manager:schedule first");
        }

        @Test
        void aFirstCallAfterTheDeferralPhaseStartedFailsTheContextWithTheSameMessage() {
            // given
            UnitOfWork unitOfWork = UnitOfWorkTestUtils.aUnitOfWork();
            unitOfWork.runOnCommit(context -> new TestScope(context).run(() -> testSubject.cancelAll("late")));

            // when
            CompletableFuture<Void> result = unitOfWork.execute();

            // then
            assertThatThrownBy(() -> result.orTimeout(1, TimeUnit.SECONDS).join())
                    .isInstanceOf(CompletionException.class)
                    .cause()
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("Cannot defer a deadline call to a ProcessingContext "
                                        + "whose deferred deadline calls already ran");
            assertThat(timeline).isEmpty();
        }
    }

    @Nested
    class DispatchInterceptors {

        @Test
        void interceptorsRunWithinTheDeferredCall() {
            // given
            AtomicInteger interceptions = new AtomicInteger();
            AtomicInteger interceptionsDuringInvocation = new AtomicInteger(-1);
            testSubject.registerDispatchInterceptor((message, context, chain) -> {
                interceptions.incrementAndGet();
                return chain.proceed(message, context);
            });

            // when
            runInUnitOfWork(context -> new TestScope(context).run(() -> {
                testSubject.schedule(Instant.now(), "deadlineName", "payload", EXPLICIT_SCOPE);
                interceptionsDuringInvocation.set(interceptions.get());
            }));

            // then
            assertThat(interceptionsDuringInvocation).hasValue(0);
            assertThat(interceptions).hasValue(1);
        }

        @Test
        void interceptorsDoNotRunWhenTheContextRollsBack() {
            // given
            AtomicInteger interceptions = new AtomicInteger();
            testSubject.registerDispatchInterceptor((message, context, chain) -> {
                interceptions.incrementAndGet();
                return chain.proceed(message, context);
            });

            // when
            CompletableFuture<Object> result = UnitOfWorkTestUtils.aUnitOfWork().executeWithResult(context -> {
                new TestScope(context).run(
                        () -> testSubject.schedule(Instant.now(), "deadlineName", "payload", EXPLICIT_SCOPE)
                );
                return CompletableFuture.failedFuture(new IllegalStateException("handler failure"));
            });

            // then
            assertThatThrownBy(() -> result.orTimeout(1, TimeUnit.SECONDS).join())
                    .hasRootCauseInstanceOf(IllegalStateException.class);
            assertThat(interceptions).hasValue(0);
        }

        @Test
        void theScheduledMessageIsTheInterceptedOne() {
            // given
            testSubject.registerDispatchInterceptor(
                    (message, context, chain) -> chain.proceed(message.andMetadata(Map.of("key", "value")), context)
            );

            // when
            testSubject.schedule(Instant.now(), "deadlineName", "payload", EXPLICIT_SCOPE);

            // then
            assertThat(testSubject.scheduled).singleElement()
                                             .satisfies(call -> assertThat(call.message().metadata())
                                                     .containsEntry("key", "value"));
        }

        @Test
        void aFailingInterceptorSurfacesItsExceptionWhenTheCallRunsImmediately() {
            // given
            testSubject.registerDispatchInterceptor((message, context, chain) -> {
                throw new IllegalStateException("interceptor failure");
            });

            // when / then
            assertThatThrownBy(
                    () -> testSubject.schedule(Instant.now(), "deadlineName", "payload", EXPLICIT_SCOPE)
            ).isInstanceOf(IllegalStateException.class).hasMessage("interceptor failure");
            assertThat(testSubject.scheduled).isEmpty();
        }

        @Test
        void aFailingInterceptorFailsTheContextWhenTheCallIsDeferred() {
            // given
            testSubject.registerDispatchInterceptor((message, context, chain) -> {
                throw new IllegalStateException("interceptor failure");
            });

            // when
            CompletableFuture<Object> result = UnitOfWorkTestUtils.aUnitOfWork().executeWithResult(context -> {
                new TestScope(context).run(
                        () -> testSubject.schedule(Instant.now(), "deadlineName", "payload", EXPLICIT_SCOPE)
                );
                return CompletableFuture.completedFuture(null);
            });

            // then
            assertThatThrownBy(() -> result.orTimeout(1, TimeUnit.SECONDS).join())
                    .hasRootCauseInstanceOf(IllegalStateException.class)
                    .hasRootCauseMessage("interceptor failure");
            assertThat(testSubject.scheduled).isEmpty();
        }

        @Test
        void anInterceptorEndingTheChainWithoutAMessageFailsTheCall() {
            // given
            testSubject.registerDispatchInterceptor((message, context, chain) -> MessageStream.empty());

            // when / then
            assertThatThrownBy(() -> testSubject.schedule(Instant.now(), "deadlineName", "payload", EXPLICIT_SCOPE))
                    .isInstanceOf(IllegalStateException.class);
            assertThat(testSubject.scheduled).isEmpty();
        }
    }

    @Nested
    class DeadlineMessageCreation {

        private static final Instant TRIGGER_DATE_TIME = Instant.parse("2026-10-05T10:15:30Z");

        @Test
        void aGivenMessageDonatesItsIdentityPayloadAndMetadata() {
            // given
            Message original = new GenericMessage(new MessageType("test.Reminder"), "reminder", Map.of("key", "value"));

            // when
            testSubject.schedule(TRIGGER_DATE_TIME, "deadlineName", original, EXPLICIT_SCOPE);

            // then
            assertThat(testSubject.scheduled).singleElement()
                                             .extracting(ScheduledCall::message)
                                             .satisfies(message -> {
                                                 assertThat(message.getDeadlineName()).isEqualTo("deadlineName");
                                                 assertThat(message.identifier()).isEqualTo(original.identifier());
                                                 assertThat(message.type()).isEqualTo(original.type());
                                                 assertThat(message.payload()).isEqualTo("reminder");
                                                 assertThat(message.metadata()).containsEntry("key", "value");
                                                 assertThat(message.timestamp()).isEqualTo(TRIGGER_DATE_TIME);
                                             });
        }

        @Test
        void aPayloadIsWrappedInADeadlineMessageExpiringAtTheTriggerDateTime() {
            // when
            testSubject.schedule(TRIGGER_DATE_TIME, "deadlineName", "payload", EXPLICIT_SCOPE);

            // then
            assertThat(testSubject.scheduled).singleElement()
                                             .extracting(ScheduledCall::message)
                                             .satisfies(message -> {
                                                 assertThat(message.getDeadlineName()).isEqualTo("deadlineName");
                                                 assertThat(message.payload()).isEqualTo("payload");
                                                 assertThat(message.metadata()).isEmpty();
                                                 assertThat(message.timestamp()).isEqualTo(TRIGGER_DATE_TIME);
                                             });
        }

        @Test
        void withoutAPayloadTheDeadlineMessageHasANullPayload() {
            // when
            testSubject.schedule(TRIGGER_DATE_TIME, "deadlineName", null, EXPLICIT_SCOPE);

            // then
            assertThat(testSubject.scheduled).singleElement()
                                             .extracting(ScheduledCall::message)
                                             .satisfies(message -> {
                                                 assertThat(message.getDeadlineName()).isEqualTo("deadlineName");
                                                 assertThat(message.payload()).isNull();
                                                 assertThat(message.timestamp()).isEqualTo(TRIGGER_DATE_TIME);
                                             });
        }
    }

    private static void runInUnitOfWork(Consumer<ProcessingContext> invocation) {
        UnitOfWorkTestUtils.aUnitOfWork()
                           .executeWithResult(context -> {
                               invocation.accept(context);
                               return CompletableFuture.completedFuture(null);
                           })
                           .orTimeout(1, TimeUnit.SECONDS)
                           .join();
    }

    /**
     * Stands in for the scope a Saga starts around its handler invocation.
     */
    private static final class TestScope extends ContextAwareScope {

        private final ProcessingContext context;

        private TestScope(ProcessingContext context) {
            this.context = context;
        }

        private void run(Runnable task) {
            startScope();
            try {
                task.run();
            } finally {
                endScope();
            }
        }

        @Override
        public ProcessingContext processingContext() {
            return context;
        }

        @Override
        public ScopeDescriptor describeScope() {
            return () -> "testScope";
        }
    }
}
