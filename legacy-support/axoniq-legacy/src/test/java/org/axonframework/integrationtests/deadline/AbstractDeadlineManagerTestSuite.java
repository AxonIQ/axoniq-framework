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

package org.axonframework.integrationtests.deadline;

import org.axonframework.common.Registration;
import org.axonframework.deadline.AbstractDeadlineManager;
import org.axonframework.deadline.DeadlineMessage;
import org.axonframework.messaging.ContextAwareScope;
import org.axonframework.messaging.ScopeAware;
import org.axonframework.messaging.ScopeAwareProvider;
import org.axonframework.messaging.ScopeDescriptor;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkTestUtils;
import org.axonframework.modelling.command.AggregateScopeDescriptor;
import org.axonframework.modelling.saga.SagaScopeDescriptor;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.awaitility.Awaitility.await;

/**
 * Tests whether a {@link AbstractDeadlineManager} implementation schedules, fires and cancels deadlines as expected.
 * Each backend extends this suite, building its manager over a {@link RecordingScopeAware}, which records the deadlines
 * delivered to it.
 */
public abstract class AbstractDeadlineManagerTestSuite {

    protected static final Duration TRIGGER_DURATION = Duration.ofMillis(100);
    protected static final Duration FIRING_TIMEOUT = Duration.ofSeconds(10);
    protected static final Duration NOT_FIRING_PERIOD = Duration.ofSeconds(2);
    protected static final String DEADLINE_NAME = "deadlineName";
    protected static final ScopeDescriptor SAGA_SCOPE = new SagaScopeDescriptor("MyType", "myIdentifier");

    protected RecordingScopeAware scopeAware;
    protected AbstractDeadlineManager deadlineManager;

    /**
     * Builds the deadline manager under test, delivering through the given {@code scopeAwareProvider} and firing in
     * units of work from the given {@code unitOfWorkFactory}.
     *
     * @param scopeAwareProvider the provider of the components a fired deadline is delivered to
     * @param unitOfWorkFactory  the factory of the unit of work a fired deadline runs in
     * @return the deadline manager under test, ready to fire deadlines
     */
    protected abstract AbstractDeadlineManager buildDeadlineManager(ScopeAwareProvider scopeAwareProvider,
                                                                    UnitOfWorkFactory unitOfWorkFactory);

    /**
     * Whether the manager under test supports {@code cancelAll} and {@code cancelAllWithinScope}. The tests of those
     * methods are skipped for a manager that does not.
     *
     * @return {@code true} if the manager cancels deadlines by name and by scope, {@code false} otherwise
     */
    protected boolean supportsCancellingByNameAndScope() {
        return true;
    }

    @BeforeEach
    void setUpSuite() {
        scopeAware = new RecordingScopeAware();
        deadlineManager = buildDeadlineManager(scope -> Stream.of(scopeAware), UnitOfWorkTestUtils.SIMPLE_FACTORY);
    }

    @AfterEach
    void tearDownSuite() {
        deadlineManager.shutdown();
    }

    @Nested
    class Firing {

        @Test
        void aDeadlineFiresWithItsNamePayloadAndMetadata() {
            // given
            Message message = new GenericMessage(new MessageType(String.class), "payload", Map.of("key", "value"));

            // when
            deadlineManager.schedule(TRIGGER_DURATION, DEADLINE_NAME, message, SAGA_SCOPE);

            // then
            Delivery delivery = awaitSingleDelivery();
            assertThat(delivery.message().getDeadlineName()).isEqualTo(DEADLINE_NAME);
            assertThat(delivery.message().payload()).isEqualTo("payload");
            assertThat(delivery.message().metadata()).containsEntry("key", "value");
            assertThat(delivery.scope()).isEqualTo(SAGA_SCOPE);
        }

        @Test
        void aDeadlineWithoutPayloadFiresWithoutPayload() {
            // when
            deadlineManager.schedule(TRIGGER_DURATION, DEADLINE_NAME, null, SAGA_SCOPE);

            // then
            assertThat(awaitSingleDelivery().message().payload()).isNull();
        }

        @Test
        void aDeadlineWithANestedPayloadClassFiresWithThatClass() {
            // given
            NestedPayload payload = new NestedPayload("text", 3);

            // when
            deadlineManager.schedule(TRIGGER_DURATION, DEADLINE_NAME, payload, SAGA_SCOPE);

            // then
            assertThat(awaitSingleDelivery().message().payload()).isEqualTo(payload);
        }

        @Test
        void aDeadlineForAnAggregateScopeFiresWithThatScope() {
            // given
            ScopeDescriptor aggregateScope = new AggregateScopeDescriptor("MyAggregate", "aggregateId");

            // when
            deadlineManager.schedule(TRIGGER_DURATION, DEADLINE_NAME, "payload", aggregateScope);

            // then
            assertThat(awaitSingleDelivery().scope()).isEqualTo(aggregateScope);
        }

        @Test
        void aDeadlineFiresInAUnitOfWorkWhoseContextCarriesTheDeadline() {
            // when
            deadlineManager.schedule(TRIGGER_DURATION, DEADLINE_NAME, "payload", SAGA_SCOPE);

            // then
            Delivery delivery = awaitSingleDelivery();
            assertThat(Message.fromContext(delivery.context())).isEqualTo(delivery.message());
        }
    }

    @Nested
    class Cancelling {

        @Test
        void aCancelledScheduleDoesNotFire() {
            // given
            String scheduleId = deadlineManager.schedule(TRIGGER_DURATION, DEADLINE_NAME, "payload", SAGA_SCOPE);

            // when
            deadlineManager.cancelSchedule(DEADLINE_NAME, scheduleId);

            // then
            assertNothingFires();
        }

        @Test
        void aScheduleCancelledWithinTheContextThatScheduledItDoesNotFire() {
            // when
            runInScope(context -> {
                String scheduleId = deadlineManager.schedule(TRIGGER_DURATION, DEADLINE_NAME, "payload", SAGA_SCOPE);
                deadlineManager.cancelSchedule(DEADLINE_NAME, scheduleId);
            });

            // then
            assertNothingFires();
        }

        @Test
        void cancelAllCancelsEveryDeadlineOfThatNameOnly() {
            assumeThat(supportsCancellingByNameAndScope()).isTrue();
            // given
            deadlineManager.schedule(TRIGGER_DURATION, DEADLINE_NAME, "first", SAGA_SCOPE);
            deadlineManager.schedule(TRIGGER_DURATION, DEADLINE_NAME, "second", SAGA_SCOPE);
            deadlineManager.schedule(TRIGGER_DURATION, "otherDeadline", "other", SAGA_SCOPE);

            // when
            deadlineManager.cancelAll(DEADLINE_NAME);

            // then
            assertThat(awaitSingleDelivery().message().payload()).isEqualTo("other");
        }

        @Test
        void cancelAllWithinScopeCancelsTheDeadlinesOfThatScopeOnly() {
            assumeThat(supportsCancellingByNameAndScope()).isTrue();
            // given
            ScopeDescriptor otherScope = new SagaScopeDescriptor("MyType", "otherIdentifier");
            deadlineManager.schedule(TRIGGER_DURATION, DEADLINE_NAME, "cancelled", SAGA_SCOPE);
            deadlineManager.schedule(TRIGGER_DURATION, DEADLINE_NAME, "kept", otherScope);

            // when
            deadlineManager.cancelAllWithinScope(DEADLINE_NAME, SAGA_SCOPE);

            // then
            assertThat(awaitSingleDelivery().message().payload()).isEqualTo("kept");
        }

        @Test
        void cancelAllWithinScopeNeverMatchesAnAggregateAndASagaScopeOfEqualTypeAndIdentifier() {
            assumeThat(supportsCancellingByNameAndScope()).isTrue();
            // given
            ScopeDescriptor aggregateScope = new AggregateScopeDescriptor("MyType", "myIdentifier");
            deadlineManager.schedule(TRIGGER_DURATION, DEADLINE_NAME, "saga", SAGA_SCOPE);
            deadlineManager.schedule(TRIGGER_DURATION, DEADLINE_NAME, "aggregate", aggregateScope);

            // when
            deadlineManager.cancelAllWithinScope(DEADLINE_NAME, SAGA_SCOPE);

            // then
            assertThat(awaitSingleDelivery().message().payload()).isEqualTo("aggregate");
        }

        @Test
        void cancelAllWithinScopeMatchesAFreshlyBuiltScopeWithANonStringIdentifier() {
            assumeThat(supportsCancellingByNameAndScope()).isTrue();
            // given
            UUID uuid = UUID.randomUUID();
            deadlineManager.schedule(TRIGGER_DURATION, DEADLINE_NAME, "uuid",
                                     new AggregateScopeDescriptor("MyAggregate", uuid));
            deadlineManager.schedule(TRIGGER_DURATION, DEADLINE_NAME, "long",
                                     new AggregateScopeDescriptor("MyAggregate", 42L));

            // when
            deadlineManager.cancelAllWithinScope(DEADLINE_NAME, new AggregateScopeDescriptor("MyAggregate", uuid));
            deadlineManager.cancelAllWithinScope(DEADLINE_NAME, new AggregateScopeDescriptor("MyAggregate", 42L));

            // then
            assertNothingFires();
        }
    }

    @Nested
    class Interceptors {

        @Test
        void theScheduledDeadlineIsTheOneTheDispatchInterceptorsReturned() {
            // given
            deadlineManager.registerDispatchInterceptor(
                    (message, context, chain) -> chain.proceed(message.andMetadata(Map.of("added", "value")), context)
            );

            // when
            deadlineManager.schedule(TRIGGER_DURATION, DEADLINE_NAME, "payload", SAGA_SCOPE);

            // then
            assertThat(awaitSingleDelivery().message().metadata()).containsEntry("added", "value");
        }

        @Test
        void aCancelledDispatchInterceptorNoLongerRuns() {
            // given
            Registration registration = deadlineManager.registerDispatchInterceptor(
                    (message, context, chain) -> chain.proceed(message.andMetadata(Map.of("added", "value")), context)
            );

            // when
            assertThat(registration.cancel()).isTrue();
            deadlineManager.schedule(TRIGGER_DURATION, DEADLINE_NAME, "payload", SAGA_SCOPE);

            // then
            assertThat(awaitSingleDelivery().message().metadata()).doesNotContainKey("added");
        }

        @Test
        void theDispatchInterceptorsOfADeferredScheduleGetTheDeferringContext() {
            // given
            List<Optional<ProcessingContext>> interceptionContexts = new CopyOnWriteArrayList<>();
            deadlineManager.registerDispatchInterceptor((message, context, chain) -> {
                interceptionContexts.add(Optional.ofNullable(context));
                return chain.proceed(message, context);
            });
            AtomicReference<ProcessingContext> deferringContext = new AtomicReference<>();

            // when
            runInScope(context -> {
                deferringContext.set(context);
                deadlineManager.schedule(TRIGGER_DURATION, DEADLINE_NAME, "payload", SAGA_SCOPE);
            });

            // then
            assertThat(interceptionContexts).singleElement()
                                            .satisfies(context -> assertThat(context).containsSame(
                                                    deferringContext.get()
                                            ));
        }

        @Test
        void aHandlerInterceptorRunsAroundTheDeliveryWithinItsContext() {
            // given
            List<String> invocations = new CopyOnWriteArrayList<>();
            deadlineManager.registerHandlerInterceptor((message, context, chain) -> {
                invocations.add("before:" + message.getDeadlineName());
                return chain.proceed(message.andMetadata(Map.of("handled", "true")), context);
            });

            // when
            deadlineManager.schedule(TRIGGER_DURATION, DEADLINE_NAME, "payload", SAGA_SCOPE);

            // then
            Delivery delivery = awaitSingleDelivery();
            assertThat(invocations).containsExactly("before:" + DEADLINE_NAME);
            assertThat(delivery.message().metadata()).containsEntry("handled", "true");
        }

        @Test
        void aCancelledHandlerInterceptorNoLongerRuns() {
            // given
            List<String> invocations = new CopyOnWriteArrayList<>();
            Registration registration = deadlineManager.registerHandlerInterceptor((message, context, chain) -> {
                invocations.add("intercepted");
                return chain.proceed(message, context);
            });

            // when
            assertThat(registration.cancel()).isTrue();
            deadlineManager.schedule(TRIGGER_DURATION, DEADLINE_NAME, "payload", SAGA_SCOPE);

            // then
            awaitSingleDelivery();
            assertThat(invocations).isEmpty();
        }
    }

    /**
     * Waits until exactly one deadline was delivered, and asserts that no other one follows shortly after.
     *
     * @return the single delivery
     */
    protected Delivery awaitSingleDelivery() {
        await().atMost(FIRING_TIMEOUT).until(() -> !scopeAware.deliveries.isEmpty());
        await().during(Duration.ofMillis(500)).atMost(FIRING_TIMEOUT)
               .until(() -> scopeAware.deliveries.size() == 1);
        return scopeAware.deliveries.getFirst();
    }

    /**
     * Asserts that no deadline is delivered for a period well beyond the trigger duration.
     */
    protected void assertNothingFires() {
        await().during(NOT_FIRING_PERIOD).atMost(NOT_FIRING_PERIOD.plusSeconds(1))
               .until(() -> scopeAware.deliveries.isEmpty());
    }

    /**
     * Runs the given {@code invocation} within a unit of work, while a {@link ContextAwareScope} carrying its context
     * is current, as during the handling of a Saga. Deadline calls made then are deferred until the context prepares
     * its commit.
     *
     * @param invocation the invocation to run, receiving the unit of work's context
     */
    protected static void runInScope(Consumer<ProcessingContext> invocation) {
        UnitOfWorkTestUtils.aUnitOfWork()
                           .executeWithResult(context -> {
                               new TestScope(context).run(() -> invocation.accept(context));
                               return CompletableFuture.completedFuture(null);
                           })
                           .orTimeout(5, TimeUnit.SECONDS)
                           .join();
    }

    /**
     * A payload class nested in another class, as a deadline's stored type name has to keep its enclosing class.
     *
     * @param text   a text value
     * @param number a number value
     */
    public record NestedPayload(String text, int number) {

    }

    /**
     * A deadline delivered to the {@link RecordingScopeAware}.
     *
     * @param message the delivered deadline
     * @param context the context the deadline was delivered in
     * @param scope   the scope the deadline was delivered for
     */
    protected record Delivery(DeadlineMessage message, ProcessingContext context, ScopeDescriptor scope) {

    }

    /**
     * A {@link ScopeAware} resolving every scope, recording each deadline delivered to it. It fails the deliveries for
     * as long as a failure is set.
     */
    protected static final class RecordingScopeAware implements ScopeAware {

        private final List<Delivery> deliveries = new CopyOnWriteArrayList<>();
        private final List<Delivery> attempts = new CopyOnWriteArrayList<>();
        private volatile @Nullable RuntimeException failure;

        /**
         * Fails every following delivery with the given {@code failure}, or none if it is {@code null}.
         *
         * @param failure the failure to fail the deliveries with, or {@code null} to deliver again
         */
        public void failWith(@Nullable RuntimeException failure) {
            this.failure = failure;
        }

        /**
         * Returns the deliveries that succeeded.
         *
         * @return the deliveries that succeeded
         */
        public List<Delivery> deliveries() {
            return deliveries;
        }

        /**
         * Returns every delivery attempt, including the failed ones.
         *
         * @return every delivery attempt
         */
        public List<Delivery> attempts() {
            return attempts;
        }

        @Override
        public void send(Message message, ProcessingContext context, ScopeDescriptor scopeDescription) {
            Delivery delivery = new Delivery((DeadlineMessage) message, context, scopeDescription);
            attempts.add(delivery);
            RuntimeException currentFailure = failure;
            if (currentFailure != null) {
                throw currentFailure;
            }
            deliveries.add(delivery);
        }

        @Override
        public boolean canResolve(ScopeDescriptor scopeDescription) {
            return true;
        }
    }

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
            return SAGA_SCOPE;
        }
    }
}
