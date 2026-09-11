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
package io.axoniq.framework.workflow.runtime.execution;

import org.jspecify.annotations.Nullable;

import io.axoniq.framework.workflow.runtime.api.execution.state.StepRetryInfo;
import io.axoniq.framework.workflow.runtime.api.execution.state.WorkflowError;
import io.axoniq.framework.workflow.runtime.api.execution.state.WorkflowExecutionException;
import io.axoniq.framework.workflow.runtime.api.execution.state.WorkflowStep;
import io.axoniq.framework.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.framework.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowStatusChangeListener;
import io.axoniq.framework.workflow.runtime.execution.payload.PayloadReducerRegistry;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.common.TypeReference;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.junit.jupiter.api.*;

import java.time.Instant;
import java.util.Map;

import static io.axoniq.framework.workflow.runtime.execution.payload.CombineGlobalAndLocalPayloadReducer.NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class EventSourcedWorkflowStateTest {

    private static final String WORKFLOW_ID = "workflowId";
    private static final MessageType DEFINITION_ID = new MessageType(new QualifiedName("TestWorkflow"), "0.0.1");

    private EventSourcedWorkflowState state;
    private ProcessingContext processingContext;

    @BeforeEach
    void setUp() {
        state = new EventSourcedWorkflowState(WORKFLOW_ID, DEFINITION_ID);
        processingContext = mock(ProcessingContext.class);
        when(processingContext.component(PayloadReducerRegistry.class)).thenReturn(new PayloadReducerRegistry());
    }

    @Test
    void evolveStepStarted() {
        String stepName = "testStep";
        Object payload = "payload";
        Metadata metadata = MetadataUtils.create("workflowId", stepName, StepStatus.STARTED);
        EventMessage eventMessage = mock(EventMessage.class);
        when(eventMessage.metadata()).thenReturn(metadata);
        when(eventMessage.timestamp()).thenReturn(Instant.now());
        when(eventMessage.payloadAs(Object.class)).thenReturn(payload);

        state.evolve(eventMessage, processingContext);

        assertThat(state.containsStep(stepName)).isTrue();
        WorkflowStep step = state.getStep(stepName);
        assertThat(step.status()).isEqualTo(StepStatus.STARTED);
        assertThat(step.result()).isEqualTo(payload);
    }

    @Test
    void rehydratedStateRetainsSourcedDataAndUsesLiveStatusListeners() {
        var sourcedState = new EventSourcedWorkflowState(
                WORKFLOW_ID,
                Map.of("key", "value"),
                DEFINITION_ID
        );
        sourcedState.setStatus(WorkflowStatus.STARTED, null, true);
        var workflowContext = mock(WorkflowContext.class);
        var listener = mock(WorkflowStatusChangeListener.class);

        var rehydratedState = new EventSourcedWorkflowState(
                sourcedState,
                workflowContext,
                Map.of(WorkflowStatus.COMPLETED, listener)
        );

        assertThat(rehydratedState.workflowId()).isEqualTo(WORKFLOW_ID);
        assertThat(rehydratedState.payload()).containsEntry("key", "value");
        assertThat(rehydratedState.workflowStatus()).isEqualTo(WorkflowStatus.STARTED);

        rehydratedState.setStatus(WorkflowStatus.COMPLETED, null, true);

        verify(listener).onWorkflowStatus(WorkflowStatus.COMPLETED, workflowContext);
    }

    @Test
    void evolveStepTimedOut() {
        String stepName = "testStep";
        Object payload = "timeout-payload";
        Metadata metadata = MetadataUtils.create("workflowId", stepName, StepStatus.TIMED_OUT);
        EventMessage eventMessage = mock(EventMessage.class);
        when(eventMessage.metadata()).thenReturn(metadata);
        when(eventMessage.timestamp()).thenReturn(Instant.now());
        when(eventMessage.payloadAs(Object.class)).thenReturn(payload);

        state.evolve(eventMessage, processingContext);

        assertThat(state.containsStep(stepName)).isTrue();
        WorkflowStep step = state.getStep(stepName);
        assertThat(step.status()).isEqualTo(StepStatus.TIMED_OUT);
        assertThat(step.result()).isEqualTo(payload);
    }

    @Test
    void evolveStepCompleted() {
        String stepName = "testStep";
        Object payload = "completed-result";
        Metadata metadata = MetadataUtils.create("workflowId", stepName, StepStatus.COMPLETED);
        EventMessage eventMessage = mock(EventMessage.class);
        when(eventMessage.metadata()).thenReturn(metadata);
        when(eventMessage.timestamp()).thenReturn(Instant.now());
        when(eventMessage.payloadAs(Object.class)).thenReturn(payload);

        state.evolve(eventMessage, processingContext);

        assertThat(state.containsStep(stepName)).isTrue();
        WorkflowStep step = state.getStep(stepName);
        assertThat(step.status()).isEqualTo(StepStatus.COMPLETED);
        assertThat(step.result()).isEqualTo(payload);
    }

    @Test
    void evolveStepCancelled() {
        String stepName = "testStep";
        Metadata metadata = MetadataUtils.create("workflowId", stepName, StepStatus.CANCELLED);
        EventMessage eventMessage = mock(EventMessage.class);
        when(eventMessage.metadata()).thenReturn(metadata);
        when(eventMessage.timestamp()).thenReturn(Instant.now());

        state.evolve(eventMessage, processingContext);

        assertThat(state.containsStep(stepName)).isTrue();
        WorkflowStep step = state.getStep(stepName);
        assertThat(step.status()).isEqualTo(StepStatus.CANCELLED);
    }

    @Test
    void evolveStepRetrying() {
        String stepName = "testStep";
        WorkflowError error = WorkflowError.from(new RuntimeException("retry error"));
        StepRetryInfo retryInfo = mock(StepRetryInfo.class);
        when(retryInfo.error()).thenReturn(error);
        Metadata metadata = MetadataUtils.create("workflowId", stepName, StepStatus.RETRYING);
        EventMessage eventMessage = mock(EventMessage.class);
        when(eventMessage.metadata()).thenReturn(metadata);
        when(eventMessage.timestamp()).thenReturn(Instant.now());
        when(eventMessage.payloadAs(StepRetryInfo.class)).thenReturn(retryInfo);

        state.evolve(eventMessage, processingContext);

        assertThat(state.containsStep(stepName)).isTrue();
        WorkflowStep step = state.getStep(stepName);
        assertThat(step.status()).isEqualTo(StepStatus.RETRYING);
        assertThat(step.result()).isEqualTo(retryInfo);
        assertThat(step.error()).isInstanceOfSatisfying(WorkflowExecutionException.class, e -> {
            assertThat(e.type()).isEqualTo(RuntimeException.class.getName());
            assertThat(e.getMessage()).isEqualTo("retry error");
            assertThat(e.getStackTrace()).isEmpty();
        });
    }

    @Test
    void evolveStepCompletedAndEvolvePayload() {
        String stepName = "testStep";
        Map<String, @Nullable Object> initialPayload = Map.of("key1", "value1");
        state = new EventSourcedWorkflowState(WORKFLOW_ID, initialPayload, DEFINITION_ID);

        Map<String, @Nullable Object> stepResult = Map.of("key2", "value2");
        Metadata metadata = MetadataUtils.create("workflowId", stepName, StepStatus.COMPLETED)
                                         .and(MetadataUtils.METADATA_KEY_MODIFY_PAYLOAD,
                                           NAME);

        EventMessage eventMessage = mock(EventMessage.class);
        when(eventMessage.metadata()).thenReturn(metadata);
        when(eventMessage.timestamp()).thenReturn(Instant.now());
        when(eventMessage.payloadAs(any(TypeReference.class))).thenReturn(stepResult);
        when(eventMessage.payloadAs(Object.class)).thenReturn(stepResult);

        state.evolve(eventMessage, processingContext);

        assertThat(state.containsStep(stepName)).isTrue();
        assertThat(state.getStep(stepName).status()).isEqualTo(StepStatus.COMPLETED);
        assertThat(state.payload()).containsEntry("key1", "value1")
                                   .containsEntry("key2", "value2");
    }

    @Test
    void ignoresCompletedStepAndPayloadUpdateAfterWorkflowBecomesTerminal() {
        state = new EventSourcedWorkflowState(WORKFLOW_ID, Map.of("before", "terminal"), DEFINITION_ID);
        state.setStatus(WorkflowStatus.COMPLETED, null, false);
        var metadata = MetadataUtils.create(WORKFLOW_ID, "late-step", StepStatus.COMPLETED)
                                    .and(MetadataUtils.METADATA_KEY_MODIFY_PAYLOAD, NAME);
        var eventMessage = mock(EventMessage.class);
        when(eventMessage.type()).thenReturn(new MessageType("TestWorkflow.LateStep", "0.0.1"));
        when(eventMessage.metadata()).thenReturn(metadata);

        state.evolve(eventMessage, processingContext);

        assertThat(state.workflowStatus()).isEqualTo(WorkflowStatus.COMPLETED);
        assertThat(state.containsStep("late-step")).isFalse();
        assertThat(state.payload()).containsExactly(Map.entry("before", "terminal"));
        verify(eventMessage, never()).payloadAs(Object.class);
    }

    @Test
    void ignoresVersionMigrationAfterWorkflowBecomesTerminal() {
        state.setStatus(WorkflowStatus.COMPLETED, null, false);
        var eventMessage = mock(EventMessage.class);
        when(eventMessage.type()).thenReturn(new MessageType("TestWorkflow.Versioned", "0.0.2"));
        when(eventMessage.metadata()).thenReturn(
                MetadataUtils.createVersionMigrationStep(WORKFLOW_ID, "late-migration", "0.0.2")
        );

        state.evolve(eventMessage, processingContext);

        assertThat(state.workflowDefinitionId()).isEqualTo(DEFINITION_ID);
        assertThat(state.versionFor("late-migration")).contains(DEFINITION_ID.version());
        verify(eventMessage, never()).payloadAs(Object.class);
    }

    @Test
    void evolvePayloadConvertsNonMapPayloadsToMaps() {
        String stepName = "testStep";
        Map<String, @Nullable Object> stepResult = Map.of("key", "value");
        Metadata metadata = MetadataUtils.create("workflowId", stepName, StepStatus.COMPLETED)
                                         .and(MetadataUtils.METADATA_KEY_MODIFY_PAYLOAD, NAME);
        EventMessage eventMessage = mock(EventMessage.class);
        when(eventMessage.metadata()).thenReturn(metadata);
        when(eventMessage.timestamp()).thenReturn(Instant.now());
        when(eventMessage.payloadAs(Object.class)).thenReturn(new byte[]{1, 2, 3});
        when(eventMessage.payloadAs(EventSourcedWorkflowState.PAYLOAD_TYPE)).thenReturn(stepResult);

        state.evolve(eventMessage, processingContext);

        assertThat(state.payload()).containsEntry("key", "value");
        verify(eventMessage).payloadAs(EventSourcedWorkflowState.PAYLOAD_TYPE);
    }

    @Test
    void evolveStepFailed() {
        String stepName = "testStep";
        WorkflowError error = WorkflowError.from(new RuntimeException("Test error"));
        Metadata metadata = MetadataUtils.create("workflowId", stepName, StepStatus.FAILED);
        EventMessage eventMessage = mock(EventMessage.class);
        when(eventMessage.metadata()).thenReturn(metadata);
        when(eventMessage.timestamp()).thenReturn(Instant.now());
        // Verify that the payload is retrieved as WorkflowError before accessing it.
        // Even if payloadAs(Object.class) returns something else, payloadAs(WorkflowError.class) must be used.
        when(eventMessage.payloadAs(Object.class)).thenReturn("not-an-error");
        when(eventMessage.payloadAs(WorkflowError.class)).thenReturn(error);

        state.evolve(eventMessage, processingContext);

        assertThat(state.containsStep(stepName)).isTrue();
        WorkflowStep step = state.getStep(stepName);
        assertThat(step.status()).isEqualTo(StepStatus.FAILED);
        assertThat(step.error()).isInstanceOfSatisfying(WorkflowExecutionException.class, e -> {
            assertThat(e.type()).isEqualTo(RuntimeException.class.getName());
            assertThat(e.getMessage()).isEqualTo("Test error");
            assertThat(e.getStackTrace()).isEmpty();
        });
    }

    @Test
    void seededInitialWorkflowVersionIsReflectedByCurrentWorkflowVersion() {
        // Seed state with the workflow definition's configured version so that, when SimpleWorkflowExecution
        // constructs the STARTED event via startedWorkflow(...), the event's MessageType.version() reflects
        // the configured version (not the implicit "0.0.1" default).
        var seeded = new EventSourcedWorkflowState("wf-1",
                                                   Map.of(),
                                                   new MessageType(new QualifiedName("OrderWorkflow"), "1.2.3"),
                                                   mock(WorkflowContext.class),
                                                   Map.of());
        assertThat(seeded.workflowDefinitionId().version()).isEqualTo("1.2.3");
    }

    @Test
    void startedEventOverridesSeededVersionOnReplayOfOlderInstance() {
        // For replays, the workflow may have been started with the latest config (e.g. "2.0.0") but the
        // started event in history carries the original version it was launched under (e.g. "1.0.0").
        // evolve() must adopt the started event's MessageType.version() so resolveVersionedDefinition can
        // route to the matching sibling.
        var seeded = new EventSourcedWorkflowState("wf-1",
                                                   Map.of(),
                                                   new MessageType(new QualifiedName("OrderWorkflow"), "2.0.0"),
                                                   mock(WorkflowContext.class),
                                                   Map.of());

        EventMessage started = mock(EventMessage.class);
        when(started.metadata()).thenReturn(MetadataUtils.create("wf-1",
                                                                 WorkflowStatus.STARTED));
        when(started.timestamp()).thenReturn(Instant.now());
        when(started.payloadAs(Object.class)).thenReturn(Map.of());
        when(started.type()).thenReturn(new MessageType("OrderWorkflow.Started", "1.0.0"));

        seeded.evolve(started, processingContext);

        assertThat(seeded.workflowDefinitionId().version()).isEqualTo("1.0.0");
    }

    @Test
    void versionReturnsCurrentWorkflowVersionWhenNoMarkerRecorded() {
        // Default before any STARTED event applies is MessageType.DEFAULT_VERSION ("0.0.1").
        assertThat(state.versionFor("payment-redesign"))
                .isEqualTo(MessageType.DEFAULT_VERSION);
        assertThat(state.hasVersionMigrationStep("payment-redesign")).isFalse();
    }

    @Test
    void evolveMigrationStepPopulatesMap() {
        Metadata metadata = MetadataUtils.createVersionMigrationStep("workflowId", "payment-redesign", "0.0.2");
        EventMessage eventMessage = mockMigrationStepEvent(metadata);

        state.evolve(eventMessage, processingContext);

        assertThat(state.hasVersionMigrationStep("payment-redesign")).isTrue();
        assertThat(state.versionFor("payment-redesign")).isEqualTo("0.0.2");
        // Migration also bumps the workflow's current version (new > previous).
        assertThat(state.workflowDefinitionId().version()).isEqualTo("0.0.2");
        // Migration is registered as a real step under the changeId as stepName.
        assertThat(state.containsStep("payment-redesign")).isTrue();
    }

    @Test
    void evolveSecondMarkerForSameChangeIdIsIgnored() {
        Metadata first = MetadataUtils.createVersionMigrationStep("workflowId", "shipping-redesign", "0.0.3");
        Metadata second = MetadataUtils.createVersionMigrationStep("workflowId", "shipping-redesign", "0.0.7");

        state.evolve(mockMigrationStepEvent(first), processingContext);
        state.evolve(mockMigrationStepEvent(second), processingContext);

        // First-writer wins - replay must stay deterministic.
        assertThat(state.versionFor("shipping-redesign")).isEqualTo("0.0.3");
    }

    @Test
    void evolveIndependentChangeIdsAreTrackedSeparately() {
        Metadata a = MetadataUtils.createVersionMigrationStep("workflowId", "change-a", "0.0.2");
        Metadata b = MetadataUtils.createVersionMigrationStep("workflowId", "change-b", "0.0.5");

        state.evolve(mockMigrationStepEvent(a), processingContext);
        state.evolve(mockMigrationStepEvent(b), processingContext);

        assertThat(state.versionFor("change-a")).isEqualTo("0.0.2");
        assertThat(state.versionFor("change-b")).isEqualTo("0.0.5");
    }

    private EventMessage mockMigrationStepEvent(Metadata metadata) {
        EventMessage eventMessage = mock(EventMessage.class);
        when(eventMessage.metadata()).thenReturn(metadata);
        when(eventMessage.timestamp()).thenReturn(Instant.now());
        when(eventMessage.payloadAs(Object.class)).thenReturn(Map.of());
        return eventMessage;
    }

    @Test
    void ignoresStepEventsCarryingAnotherWorkflowsId() {
        // given: a step published by another instance. SimpleWorkflowExecution.onEvent already keeps such an event away
        // from this state; this is the safety net for a routing mistake that lets one through
        Metadata metadata = MetadataUtils.create("another-workflow", "notifyApproved", StepStatus.COMPLETED)
                                         .and(MetadataUtils.METADATA_KEY_STEP_PRIMITIVE,
                                              MetadataUtils.STEP_PRIMITIVE_PUBLISH);
        EventMessage eventMessage = mock(EventMessage.class);
        when(eventMessage.metadata()).thenReturn(metadata);
        when(eventMessage.timestamp()).thenReturn(Instant.now());
        when(eventMessage.payloadAs(Object.class)).thenReturn("payload");

        // when
        state.evolve(eventMessage, processingContext);

        // then: it is a business event for this instance, never one of its steps
        assertThat(state.containsStep("notifyApproved")).isFalse();
        assertThat(state.workflowStepNames()).isEmpty();
    }
}
