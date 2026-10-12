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

import org.axonframework.messaging.ScopeAware;
import org.axonframework.messaging.ScopeDescriptor;
import org.axonframework.messaging.core.ExecutionException;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageHandlerInterceptor;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkTestUtils;
import org.axonframework.modelling.saga.SagaScopeDescriptor;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.*;

/**
 * Test class validating the {@link DeadlineDelivery}.
 *
 * @author Jakob Hatzl
 */
class DeadlineDeliveryTest {

    private static final ScopeDescriptor SCOPE = new SagaScopeDescriptor("MySaga", "sagaId");

    private final List<String> events = new CopyOnWriteArrayList<>();
    private final List<MessageHandlerInterceptor<? super DeadlineMessage>> interceptors = new CopyOnWriteArrayList<>();
    private final AtomicInteger commits = new AtomicInteger();

    private RecordingScopeAware resolving;
    private RecordingScopeAware notResolving;
    private DeadlineDelivery delivery;

    @BeforeEach
    void setUp() {
        resolving = new RecordingScopeAware("resolving", true);
        notResolving = new RecordingScopeAware("notResolving", false);
        delivery = new DeadlineDelivery(UnitOfWorkTestUtils.SIMPLE_FACTORY,
                                        scope -> Stream.of(resolving, notResolving),
                                        interceptors);
    }

    @Nested
    class Delivering {

        @Test
        void theDeadlineReachesOnlyTheComponentsResolvingItsScope() {
            // given
            DeadlineMessage deadline = deadline();

            // when
            delivery.deliver(deadline, SCOPE);

            // then
            assertThat(resolving.received).containsExactly(deadline);
            assertThat(notResolving.received).isEmpty();
        }

        @Test
        void theComponentsGetAContextCarryingTheDeadline() {
            // given
            DeadlineMessage deadline = deadline();

            // when
            delivery.deliver(deadline, SCOPE);

            // then
            assertThat(resolving.contexts).singleElement()
                                          .satisfies(context -> assertThat(Message.fromContext(context))
                                                  .isEqualTo(deadline));
        }

        @Test
        void aSuccessfulDeliveryCommitsTheUnitOfWork() {
            // when
            delivery.deliver(deadline(), SCOPE);

            // then
            assertThat(commits).hasValue(1);
        }
    }

    @Nested
    class Interceptors {

        @Test
        void theHandlerInterceptorsRunInTheirOrderAroundTheDelivery() {
            // given
            interceptors.add(recording("first"));
            interceptors.add(recording("second"));

            // when
            delivery.deliver(deadline(), SCOPE);

            // then
            assertThat(events).containsExactly("first:before", "second:before", "resolving:delivered",
                                               "second:after", "first:after");
        }

        @Test
        void theComponentsGetTheDeadlineAnInterceptorPassedOn() {
            // given
            interceptors.add((message, context, chain) -> chain.proceed(
                    message.andMetadata(Map.of("intercepted", "true")), context
            ));

            // when
            delivery.deliver(deadline(), SCOPE);

            // then
            assertThat(resolving.received).singleElement()
                                          .satisfies(message -> assertThat(message.metadata())
                                                  .containsEntry("intercepted", "true"));
        }

        @Test
        void anInterceptorEndingTheChainKeepsTheDeadlineFromTheComponents() {
            // given
            interceptors.add((message, context, chain) -> MessageStream.empty());

            // when
            delivery.deliver(deadline(), SCOPE);

            // then
            assertThat(resolving.received).isEmpty();
        }
    }

    @Nested
    class Failing {

        @Test
        void aFailingComponentFailsTheDeliveryWithAnExecutionException() {
            // given
            IllegalStateException failure = new IllegalStateException("component failure");
            resolving.failWith(failure);

            // when / then
            assertThatThrownBy(() -> delivery.deliver(deadline(), SCOPE))
                    .isInstanceOf(ExecutionException.class)
                    .hasMessageContaining(SCOPE.scopeDescription())
                    .hasCause(failure);
        }

        @Test
        void aFailingComponentRollsTheUnitOfWorkBack() {
            // given
            resolving.failWith(new IllegalStateException("component failure"));

            // when
            assertThatThrownBy(() -> delivery.deliver(deadline(), SCOPE));

            // then
            assertThat(commits).hasValue(0);
        }

        @Test
        void aFailingInterceptorFailsTheDeliveryWithItsOwnException() {
            // given
            IllegalArgumentException failure = new IllegalArgumentException("interceptor failure");
            interceptors.add((message, context, chain) -> {
                throw failure;
            });

            // when / then
            assertThatThrownBy(() -> delivery.deliver(deadline(), SCOPE)).isSameAs(failure);
            assertThat(resolving.received).isEmpty();
        }

        /**
         * The deadline reached its components, but the interceptor fails the unit of work afterward, so what the
         * components registered for the commit, such as a Saga's write, never happens.
         */
        @Test
        void anInterceptorFailingAfterTheDeliveryRollsTheDeliveryBack() {
            // given
            IllegalArgumentException failure = new IllegalArgumentException("interceptor failure");
            interceptors.add((message, context, chain) -> {
                chain.proceed(message, context);
                throw failure;
            });

            // when
            assertThatThrownBy(() -> delivery.deliver(deadline(), SCOPE)).isSameAs(failure);

            // then
            assertThat(resolving.received).hasSize(1);
            assertThat(commits).hasValue(0);
        }
    }

    private static DeadlineMessage deadline() {
        return new GenericDeadlineMessage("deadlineName", new GenericMessage(new MessageType(String.class), "payload"),
                                          Instant::now);
    }

    private MessageHandlerInterceptor<DeadlineMessage> recording(String name) {
        return (message, context, chain) -> {
            events.add(name + ":before");
            MessageStream<?> result = chain.proceed(message, context);
            events.add(name + ":after");
            return result;
        };
    }

    /**
     * A {@link ScopeAware} recording the deadlines delivered to it, and the commits of the units of work it was
     * delivered in.
     */
    private final class RecordingScopeAware implements ScopeAware {

        private final String name;
        private final boolean resolves;
        private final List<Message> received = new CopyOnWriteArrayList<>();
        private final List<ProcessingContext> contexts = new CopyOnWriteArrayList<>();
        private volatile @Nullable RuntimeException failure;

        private RecordingScopeAware(String name, boolean resolves) {
            this.name = name;
            this.resolves = resolves;
        }

        private void failWith(RuntimeException failure) {
            this.failure = failure;
        }

        @Override
        public void send(Message message, ProcessingContext context, ScopeDescriptor scopeDescription) {
            context.runOnCommit(committed -> commits.incrementAndGet());
            RuntimeException currentFailure = failure;
            if (currentFailure != null) {
                throw currentFailure;
            }
            received.add(message);
            contexts.add(context);
            events.add(name + ":delivered");
        }

        @Override
        public boolean canResolve(ScopeDescriptor scopeDescription) {
            return resolves;
        }
    }
}
