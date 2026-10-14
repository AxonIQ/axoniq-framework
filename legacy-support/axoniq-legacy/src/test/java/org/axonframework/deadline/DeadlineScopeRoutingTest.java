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
import org.axonframework.messaging.ScopeAwareProvider;
import org.axonframework.messaging.ScopeDescriptor;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWork;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkConfiguration;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkTestUtils;
import org.axonframework.modelling.command.AggregateScopeDescriptor;
import org.axonframework.modelling.saga.SagaScopeDescriptor;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Test class validating that a fired deadline carries the scope it was scheduled for to the {@link ScopeAware}
 * components the {@link ScopeAwareProvider} provides, and only reaches those that resolve that scope.
 *
 * @author Mateusz Nowak
 * @author Jakob Hatzl
 */
class DeadlineScopeRoutingTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(5);
    private static final String IDENTIFIER = "target-id";

    private final RecordingScopeAware aggregateComponent =
            new RecordingScopeAware(AggregateScopeDescriptor.class::isInstance);
    private final RecordingScopeAware sagaComponent = new RecordingScopeAware(SagaScopeDescriptor.class::isInstance);

    private SimpleDeadlineManager deadlineManager;

    @BeforeEach
    void setUp() {
        deadlineManager = deadlineManager(UnitOfWorkTestUtils.SIMPLE_FACTORY,
                                          scope -> Stream.of(aggregateComponent, sagaComponent));
    }

    @AfterEach
    void tearDown() {
        deadlineManager.shutdown();
    }

    @Nested
    class Routing {

        @Test
        void deadlinesOfBothScopesEachReachTheirOwnComponent() {
            // given
            AggregateScopeDescriptor aggregateScope = new AggregateScopeDescriptor("Shared", IDENTIFIER);
            SagaScopeDescriptor sagaScope = new SagaScopeDescriptor("Shared", IDENTIFIER);

            // when
            deadlineManager.schedule(Duration.ZERO, "deadline", "aggregate", aggregateScope);
            deadlineManager.schedule(Duration.ZERO, "deadline", "saga", sagaScope);

            // then
            await().atMost(TIMEOUT).untilAsserted(() -> {
                assertThat(aggregateComponent.scopes).containsExactly(aggregateScope);
                assertThat(sagaComponent.scopes).containsExactly(sagaScope);
            });
        }

        @Test
        void aDeadlineNoComponentResolvesIsNotDelivered() {
            // when
            deadlineManager.schedule(Duration.ZERO, "deadline", "payload",
                                     new TestScopeDescriptor("Other", IDENTIFIER));

            // then
            await().during(Duration.ofMillis(200)).atMost(TIMEOUT).untilAsserted(() -> {
                assertThat(aggregateComponent.scopes).isEmpty();
                assertThat(sagaComponent.scopes).isEmpty();
            });
        }
    }

    /**
     * The configuration's provider may wait for the application to start, which must not keep a transaction open.
     */
    @Test
    void theComponentsAreProvidedBeforeTheUnitOfWorkStarts() {
        // given
        CountingUnitOfWorkFactory unitOfWorkFactory = new CountingUnitOfWorkFactory();
        List<Integer> unitsOfWorkWhenProvided = new CopyOnWriteArrayList<>();
        deadlineManager.shutdown();
        deadlineManager = deadlineManager(unitOfWorkFactory, scope -> {
            unitsOfWorkWhenProvided.add(unitOfWorkFactory.created.get());
            return Stream.of(sagaComponent);
        });

        // when
        deadlineManager.schedule(Duration.ZERO, "deadline", "payload", new SagaScopeDescriptor("MySaga", IDENTIFIER));

        // then
        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(sagaComponent.scopes).hasSize(1));
        assertThat(unitsOfWorkWhenProvided).containsExactly(0);
        assertThat(unitOfWorkFactory.created).hasValue(1);
    }

    private static SimpleDeadlineManager deadlineManager(UnitOfWorkFactory unitOfWorkFactory,
                                                         ScopeAwareProvider scopeAwareProvider) {
        return SimpleDeadlineManager.builder()
                                    .scopeAwareProvider(scopeAwareProvider)
                                    .unitOfWorkFactory(unitOfWorkFactory)
                                    .build();
    }

    /**
     * A {@link ScopeAware} resolving the scopes matching its predicate, recording each scope a deadline was delivered
     * for.
     */
    private static final class RecordingScopeAware implements ScopeAware {

        private final Predicate<ScopeDescriptor> resolves;
        private final List<ScopeDescriptor> scopes = new CopyOnWriteArrayList<>();

        private RecordingScopeAware(Predicate<ScopeDescriptor> resolves) {
            this.resolves = resolves;
        }

        @Override
        public void send(Message message, ProcessingContext context, ScopeDescriptor scopeDescription) {
            scopes.add(scopeDescription);
        }

        @Override
        public boolean canResolve(ScopeDescriptor scopeDescription) {
            return resolves.test(scopeDescription);
        }
    }

    /**
     * A {@link UnitOfWorkFactory} counting the units of work it created.
     */
    private static final class CountingUnitOfWorkFactory implements UnitOfWorkFactory {

        private final AtomicInteger created = new AtomicInteger();

        @Override
        public UnitOfWork create(String identifier,
                                 Function<UnitOfWorkConfiguration, UnitOfWorkConfiguration> customization) {
            created.incrementAndGet();
            return UnitOfWorkTestUtils.SIMPLE_FACTORY.create(identifier, customization);
        }
    }
}
