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
package io.axoniq.workflow.configuration;

import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.workflow.runtime.execution.ConsistencyMarkerSupport;
import io.axoniq.workflow.runtime.execution.DefaultEventNameCustomizer;
import io.axoniq.workflow.runtime.execution.EventSourcedWorkflowState;
import io.axoniq.workflow.runtime.execution.WorkflowAppendConditions;
import io.axoniq.workflow.runtime.util.EventMessageUtils;
import org.axonframework.eventsourcing.eventstore.ConsistencyMarker;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Checks the marker a segment claim seeds its restored executions with, sourced the way the engine sources it: through
 * the workflow state repository, on the claim's shared processing context.
 * <p>
 * A previous owner that never learned it lost the segment keeps appending while the claim is still reading. The
 * question these tests answer is what the marker that comes out of that read allows the restored execution to do.
 */
class ClaimRestoreMarkerFencingTest extends AbstractEventSourcedEntityRepositoryTestBase {

    private static final MessageType DEFINITION_ID = new MessageType(new QualifiedName("OrderWorkflow"), "1.0.0");
    private static final Executor DIRECT = Runnable::run;

    private final DefaultEventNameCustomizer customizer = DefaultEventNameCustomizer.Builder.defaults();

    @Test
    void aPreviousOwnerAppendingDuringTheClaimsSourcingStillFencesTheRestoredExecution() {
        configuration = configuration("claim-restore-fencing");
        configuration.start();
        var instanceA = workflowContext("wf-a", "1.0.0");
        var instanceB = workflowContext("wf-b", "1.0.0");
        publish(EventMessageUtils.startedWorkflow(instanceA, "OrderWorkflow", DEFINITION_ID, customizer));
        publish(EventMessageUtils.startedWorkflow(instanceB, "OrderWorkflow", DEFINITION_ID, customizer));

        var seededMarker = claimSourcing(claim -> {
            load("wf-a", claim);
            // The previous owner still runs 'wf-a' and records a step while this claim reads on.
            publish(step(instanceA, "approveOrder"));
            load("wf-b", claim);
        });

        assertThat(seededMarker)
                .as("a marker at the origin rejects every append, which would pass this test for the wrong reason")
                .isNotNull()
                .isNotEqualTo(ConsistencyMarker.ORIGIN);

        var failure = catchThrowable(() -> append(instanceA, seededMarker).join());
        assertThat(WorkflowAppendConditions.isAppendRejected(failure))
                .as("this claim read 'wf-a' before the previous owner's write, so the restored execution is fenced")
                .isTrue();

        assertThatCode(() -> append(instanceB, seededMarker).join())
                .as("nothing was written for 'wf-b', so its restored execution appends")
                .doesNotThrowAnyException();
    }

    @Test
    void anInstanceReadAfterAPreviousOwnersWriteFencesItselfOnTheClaimsSharedMarker() {
        configuration = configuration("claim-restore-self-fence");
        configuration.start();
        var instanceA = workflowContext("wf-a", "1.0.0");
        var instanceB = workflowContext("wf-b", "1.0.0");
        publish(EventMessageUtils.startedWorkflow(instanceA, "OrderWorkflow", DEFINITION_ID, customizer));
        publish(EventMessageUtils.startedWorkflow(instanceB, "OrderWorkflow", DEFINITION_ID, customizer));

        var seededMarker = claimSourcing(claim -> {
            // Reading another instance first fixes the shared marker low.
            load("wf-b", claim);
            publish(step(instanceA, "approveOrder"));
            load("wf-a", claim);
        });

        var failure = catchThrowable(() -> append(instanceA, seededMarker).join());
        assertThat(WorkflowAppendConditions.isAppendRejected(failure))
                .as("the shared marker is the lowest read, so it sits before an event this claim itself sourced")
                .isTrue();
    }

    @Test
    void controlAForeignWriteAfterTheReadFencesTheRestoredExecution() {
        configuration = configuration("claim-restore-control");
        configuration.start();
        var instanceA = workflowContext("wf-a", "1.0.0");
        publish(EventMessageUtils.startedWorkflow(instanceA, "OrderWorkflow", DEFINITION_ID, customizer));

        var seededMarker = claimSourcing(claim -> load("wf-a", claim));
        publish(step(instanceA, "approveOrder"));

        var failure = catchThrowable(() -> append(instanceA, seededMarker).join());
        assertThat(WorkflowAppendConditions.isAppendRejected(failure))
                .as("a write after the read must be a conflict, or this harness cannot detect conflicts at all")
                .isTrue();
    }

    /**
     * Runs the given {@code sourcing} in one unit of work, the way a segment claim sources every instance it owns, and
     * returns the append position that unit of work ends up with.
     */
    private ConsistencyMarker claimSourcing(java.util.function.Consumer<ProcessingContext> sourcing) {
        var eventStore = configuration.getComponent(EventStore.class);
        var seeded = new AtomicReference<ConsistencyMarker>();
        configuration.getComponent(UnitOfWorkFactory.class)
                     .create("claim-sourcing")
                     .executeWithResult(claim -> {
                         sourcing.accept(claim);
                         seeded.set(eventStore.transaction(claim).appendPosition());
                         return CompletableFuture.completedFuture(null);
                     })
                     .join();
        return seeded.get();
    }

    private void load(String workflowId, ProcessingContext claim) {
        repository(EventSourcedWorkflowState.class).loadOrCreate(workflowId, claim).join();
    }

    private EventMessage step(WorkflowContext context, String stepName) {
        return EventMessageUtils.completedStep(context, stepName, Map.of("approved", true), null, customizer);
    }

    /**
     * Appends one event for the given instance through the engine's own append path, from an execution restored at the
     * given {@code restoredAt} position.
     */
    private CompletableFuture<Void> append(WorkflowContext context, ConsistencyMarker restoredAt) {
        var appendCondition = new ConsistencyMarkerSupport();
        appendCondition.updateAppendPosition(restoredAt);
        // Read off the context before stubbing: a mock call inside when(...) is nested stubbing.
        var workflowId = context.workflowId();
        var event = step(context, "shipOrder");
        var execution = mock(WorkflowExecution.class);
        when(execution.workflowId()).thenReturn(workflowId);
        when(execution.appendCondition()).thenReturn(appendCondition);
        return WorkflowAppendConditions.append(configuration.getComponent(EventStore.class),
                                               configuration.getComponent(UnitOfWorkFactory.class),
                                               DIRECT,
                                               Context.empty(),
                                               event,
                                               execution);
    }
}
