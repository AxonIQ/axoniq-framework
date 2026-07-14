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
package io.axoniq.example.workflow.workflow;

import io.axoniq.example.workflow.fixture.MagicHappenedEvent;
import io.axoniq.example.workflow.fixture.RegistrationReceivedEvent;
import io.axoniq.workflow.configuration.WorkflowConfigurer;
import io.axoniq.workflow.configuration.WorkflowModule;
import io.axoniq.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.workflow.dsl.simple.SimpleWorkflowContextFactory;
import io.axoniq.workflow.runtime.api.execution.context.EventConditions;
import io.axoniq.workflow.runtime.test.AbstractDeclarativeTestBase;
import org.axonframework.messaging.core.unitofwork.transaction.Transaction;
import org.axonframework.messaging.core.unitofwork.transaction.TransactionManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static io.axoniq.workflow.dsl.base.BaseWorkflowContext.equalsTo;
import static io.axoniq.workflow.runtime.association.Associations.associate;
import static io.axoniq.workflow.runtime.association.PayloadPropertyValueRetriever.payloadProperty;
import static io.axoniq.workflow.runtime.execution.DefaultEventNameCustomizer.Builder.namespace;
import static io.axoniq.workflow.runtime.execution.PayloadPropertyWorkflowIdProvider.fromPayloadAttribute;
import static io.axoniq.workflow.runtime.test.utils.DelayedPublisher.Schedule.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Regression test for issue #126 (HikariCP connection-pool exhaustion).
 * <p>
 * The workflow-body wrapper unit of work spans the ENTIRE body lifetime, including the time an instance spends
 * parked at {@code awaitEvent}/{@code sleep}/retry backoff. Before the fix it was created from the application-level
 * {@code UnitOfWorkFactory}, which Axon Framework defaults to {@code TransactionalUnitOfWorkFactory}: every
 * in-flight workflow instance held one open transaction (= one pooled JDBC connection in JPA deployments) for its
 * whole lifetime. More parked instances than pool capacity exhausted the pool and halted the processor.
 * <p>
 * This test registers a counting {@link TransactionManager} as the application-level component, drives a workflow to
 * park at {@code awaitEvent}, and asserts that the parked steady state holds ZERO open transactions, while durable
 * commits (event publishes) still run transactionally through short-lived child units of work.
 */
class WorkflowTransactionScopeIntegrationTest extends AbstractDeclarativeTestBase<SimpleWorkflowContext> {

    private final CountingTransactionManager countingTm = new CountingTransactionManager();

    public WorkflowTransactionScopeIntegrationTest() {
        super(SimpleWorkflowContext.class, c -> new SimpleWorkflowContextFactory());
    }

    @Override
    protected UnaryOperator<WorkflowConfigurer> configure() {
        return configurer -> configurer.componentRegistry(
                r -> r.registerComponent(TransactionManager.class, cfg -> countingTm));
    }

    @Override
    protected Function<WorkflowModule.WorkflowDefinitionPhase.DetectionPhase<SimpleWorkflowContext>,
            WorkflowModule.WorkflowDefinitionPhase.FinalizedPhase<SimpleWorkflowContext>> getDeclaredDefinition() {
        return d -> d
                .declarative(c -> ctx -> {
                    ctx.awaitExecute("prepare", Boolean.class, () -> true);
                    // Park with a generous timeout: the test asserts the parked steady state before waking it.
                    ctx.awaitEvent("waitForMagicToHappen",
                                   MagicHappenedEvent.class,
                                   associate(payloadProperty("magician"), equalsTo("Merlin")),
                                   step -> step.timeout(java.time.Duration.ofMinutes(10)));
                })
                .workflowName("Issue126Workflow")
                .on(EventConditions.fromType(
                        RegistrationReceivedEvent.class,
                        associate(payloadProperty("status"), equalsTo("vip"))
                ))
                .customized((c, w) -> w
                        .eventNameCustomizer(namespace("io.axoniq.dsl.wf.workflow"))
                        .workflowIdProvider(fromPayloadAttribute(c, "id", id -> "signup-" + id))
                );
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void parkedWorkflowInstanceHoldsNoOpenTransaction() {
        delayedPublisher.addSchedules(List.of(
                ofMillis(500, new RegistrationReceivedEvent("126", "kermit@muppets.biz", "vip")), // start
                ofMillis(6000, new MagicHappenedEvent("Merlin")) // wake, after the parked-state assertions
        ));
        delayedPublisher.start();

        // The instance runs its prepare step and parks at the awaitEvent step "waitForMagicToHappen".
        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(workflowEngine.workflowExecutions()).hasSize(1);
            assertThat(workflowEngine.workflowExecutions().iterator().next().state().workflowStepNames())
                    .contains("waitForMagicToHappen");
        });

        // THE #126 SIGNATURE. While parked, the instance must hold no open transaction. Before the fix, the
        // whole-body transactional unit of work kept exactly one open per in-flight instance (= one pinned JDBC
        // connection in a JPA deployment).
        await().atMost(4, TimeUnit.SECONDS).untilAsserted(() ->
                assertThat(countingTm.open.get())
                        .as("a parked workflow instance must not hold an open transaction (each open transaction "
                                    + "is one pinned pooled connection in production)")
                        .isZero());

        // Sanity: the probe is wired. Durable commits (event publishes) DID run transactionally.
        assertThat(countingTm.maxOpen.get())
                .as("short-lived transactional units of work were observed")
                .isPositive();

        // The scheduled wake arrives; the instance completes and no transaction leaks.
        await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(workflowHistoryRepository.findAll()).isNotEmpty();
            assertThat(workflowHistoryRepository.findAll())
                    .allMatch(h -> h.state().workflowStatus().isTerminal());
        });
        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() ->
                assertThat(countingTm.open.get())
                        .as("no transaction leaked after completion")
                        .isZero());
    }

    /**
     * Counts transactions currently open (started, not yet committed or rolled back) and the high-water mark.
     */
    private static final class CountingTransactionManager implements TransactionManager {

        private final AtomicInteger open = new AtomicInteger();
        private final AtomicInteger maxOpen = new AtomicInteger();

        @Override
        public Transaction startTransaction() {
            int now = open.incrementAndGet();
            maxOpen.accumulateAndGet(now, Math::max);
            return new Transaction() {
                @Override
                public void commit() {
                    open.decrementAndGet();
                }

                @Override
                public void rollback() {
                    open.decrementAndGet();
                }
            };
        }
    }
}
