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
package io.axoniq.workflow.itest;

import io.axoniq.workflow.configuration.WorkflowConfigurer;
import io.axoniq.workflow.configuration.WorkflowModule.WorkflowDefinitionPhase.DetectionPhase;
import io.axoniq.workflow.configuration.WorkflowModule.WorkflowDefinitionPhase.FinalizedPhase;
import io.axoniq.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.workflow.dsl.simple.SimpleWorkflowContextFactory;
import io.axoniq.workflow.runtime.api.annotation.Workflow;
import io.axoniq.workflow.runtime.test.AbstractWorkflowTestBase;
import jakarta.annotation.Nonnull;
import org.axonframework.messaging.core.unitofwork.transaction.Transaction;
import org.axonframework.messaging.core.unitofwork.transaction.TransactionManager;
import org.axonframework.messaging.eventhandling.annotation.Event;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static io.axoniq.workflow.dsl.base.BaseWorkflowContext.equalsTo;
import static io.axoniq.workflow.runtime.association.Associations.associate;
import static io.axoniq.workflow.runtime.association.PayloadPropertyValueRetriever.payloadProperty;
import static io.axoniq.workflow.runtime.test.utils.DelayedPublisher.Schedule.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Verifies the transaction scope of workflow execution: a workflow instance parked at {@code awaitEvent} must not
 * hold an open transaction.
 * <p>
 * The workflow-body wrapper unit of work stays open for the instance's entire lifetime, so it is deliberately
 * non-transactional; durable commits run in short-lived child units of work created from the transactional
 * application-level factory. This test registers a counting {@link TransactionManager} as the application-level
 * component and asserts that the parked steady state holds zero open transactions while short-lived transactional
 * units of work are still observed.
 *
 * @author Stefan Dragisic
 * @since 1.0.0
 */
class TransactionScopeWorkflowTest extends AbstractWorkflowTestBase<SimpleWorkflowContext> {

    private CountingTransactionManager countingTm;

    public TransactionScopeWorkflowTest() {
        super(SimpleWorkflowContext.class, c -> new SimpleWorkflowContextFactory());
    }

    @Override
    protected UnaryOperator<WorkflowConfigurer> configure() {
        countingTm = new CountingTransactionManager();
        return configurer -> configurer.componentRegistry(
                r -> r.registerComponent(TransactionManager.class, cfg -> countingTm));
    }

    @Override
    protected Function<DetectionPhase<SimpleWorkflowContext>, FinalizedPhase<SimpleWorkflowContext>> getDeclaredDefinition() {
        return d -> d.autodetected(c -> new TransactionScopeWorkflow());
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void parkedWorkflowInstanceHoldsNoOpenTransaction() {
        delayedPublisher.addSchedules(List.of(
                ofMillis(500, new RegistrationReceivedEvent("t1", "kermit@muppets.biz", "vip")), // start
                ofMillis(6000, new MagicHappenedEvent("Merlin")) // wake, after the parked-state assertions
        ));
        delayedPublisher.start();

        // The instance runs its prepare step and parks at the awaitEvent step "waitForMagicToHappen".
        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(workflowEngine.workflowExecutions()).hasSize(1);
            assertThat(workflowEngine.workflowExecutions().iterator().next().state().workflowStepNames())
                    .contains("waitForMagicToHappen");
        });

        // While parked, the instance must hold no open transaction.
        await().atMost(4, TimeUnit.SECONDS).untilAsserted(() ->
                assertThat(countingTm.open.get())
                        .as("a parked workflow instance must not hold an open transaction")
                        .isZero());

        // Sanity: the probe is wired. Durable commits (event publishes) did run transactionally.
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

    public static class TransactionScopeWorkflow {

        @Workflow(
                workflowName = "TransactionScopeWorkflow",
                workflowNamespace = "io.axoniq.dsl.transactionScope",
                idProperty = "id",
                startOnEventClass = RegistrationReceivedEvent.class
        )
        public void execute(@Nonnull SimpleWorkflowContext ctx) {
            ctx.awaitExecute("prepare", Boolean.class, () -> true);
            // Park with a generous timeout: the test asserts the parked steady state before waking it.
            ctx.awaitEvent("waitForMagicToHappen",
                           MagicHappenedEvent.class,
                           associate(payloadProperty("magician"), equalsTo("Merlin")),
                           step -> step.timeout(Duration.ofMinutes(10)));
        }
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

    @Event(namespace = "my.custom", name = "RegistrationReceived")
    public record RegistrationReceivedEvent(String id, String email, String status) {

    }

    @Event(namespace = "my.custom", name = "MagicHappened")
    public record MagicHappenedEvent(String magician) {

    }
}
