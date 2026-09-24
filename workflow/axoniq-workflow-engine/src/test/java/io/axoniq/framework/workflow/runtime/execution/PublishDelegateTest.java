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

import io.axoniq.framework.workflow.dsl.api.StepStatus;
import io.axoniq.framework.workflow.dsl.api.WorkflowState;
import io.axoniq.framework.workflow.dsl.api.WorkflowStep;
import io.axoniq.framework.workflow.dsl.api.WorkflowStepResult;
import io.axoniq.framework.workflow.runtime.api.execution.context.PrimitiveCommands;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowReplayDriftException;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.junit.jupiter.api.*;
import org.mockito.*;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static io.axoniq.framework.workflow.runtime.util.MetadataUtils.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.eq;

/**
 * Tests for {@link PublishDelegate}: the user's event is appended once, enriched with workflow metadata, and never
 * re-published for a step already in state.
 *
 * @author Stefan Dragisic
 */
class PublishDelegateTest {

    private static final String WORKFLOW_ID = "wf-1";
    private static final String STEP_NAME = "notifyApproved";
    private static final MessageType USER_TYPE = new MessageType(new QualifiedName("io.acme", "OrderApproved"));
    private final ReachedSteps reachedSteps = new ReachedSteps();
    private WorkflowExecution workflowExecution;
    private WorkflowState state;
    private ProcessingContext processingContext;
    private PublishDelegate delegate;

    private static EventMessage userEvent(Metadata metadata) {
        return new GenericEventMessage(USER_TYPE, Map.of("orderId", "order-1"), metadata);
    }

    @BeforeEach
    void setUp() {
        workflowExecution = mock(WorkflowExecution.class);
        state = mock(WorkflowState.class);
        processingContext = mock(ProcessingContext.class);

        when(workflowExecution.workflowId()).thenReturn(WORKFLOW_ID);
        when(workflowExecution.state()).thenReturn(state);
        when(workflowExecution.processingContext()).thenReturn(processingContext);
        when(processingContext.component(EventConverter.class)).thenReturn(TestEventConverter.INSTANCE);
        when(workflowExecution.appendWorkflowEvent(any(EventMessage.class), any(ProcessingContext.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        delegate = new PublishDelegate(workflowExecution, reachedSteps);
    }

    @SuppressWarnings("unchecked")
    private EventMessage publishAndCaptureAppendedEvent(EventMessage userEvent) {
        ArgumentCaptor<Consumer<WorkflowExecution>> taskCaptor = ArgumentCaptor.forClass(Consumer.class);
        delegate.publish(PrimitiveCommands.publish(STEP_NAME, userEvent));
        verify(workflowExecution).appendTask(taskCaptor.capture());
        taskCaptor.getValue().accept(workflowExecution);

        ArgumentCaptor<EventMessage> eventCaptor = ArgumentCaptor.forClass(EventMessage.class);
        verify(workflowExecution, times(1)).appendWorkflowEvent(eventCaptor.capture(), eq(processingContext));
        return eventCaptor.getValue();
    }

    @Nested
    class FirstLiveRun {

        @BeforeEach
        void stepNotYetInState() {
            when(state.containsStep(STEP_NAME)).thenReturn(false);
        }

        @Test
        void publishesTheUserEventOnceWithWorkflowMetadataAdded() {
            // given
            var userEvent = userEvent(Metadata.with("userKey", "userValue"));

            // when
            var appended = publishAndCaptureAppendedEvent(userEvent);

            // then: the event is the event, only metadata is added
            assertThat(appended.type()).isEqualTo(USER_TYPE);
            assertThat(appended.identifier()).isEqualTo(userEvent.identifier());
            assertThat(appended.timestamp()).isEqualTo(userEvent.timestamp());
            assertThat(appended.payload()).isEqualTo(userEvent.payload());
            assertThat(appended.metadata())
                    .containsEntry("userKey", "userValue")
                    .containsEntry(METADATA_KEY_WORKFLOW_ID, WORKFLOW_ID)
                    .containsEntry(METADATA_KEY_STEP_NAME, STEP_NAME)
                    .containsEntry(METADATA_KEY_TYPE, StepStatus.COMPLETED.name())
                    .containsEntry(METADATA_KEY_STEP_PRIMITIVE, STEP_PRIMITIVE_PUBLISH);
            assertThat(isPublishStep(appended.metadata())).isTrue();
        }

        @Test
        void workflowMetadataWinsOverUserMetadataOfTheSameKey() {
            // given
            var userEvent = userEvent(Metadata.with(METADATA_KEY_WORKFLOW_ID, "someone-else")
                                              .and(METADATA_KEY_STEP_NAME, "otherStep"));

            // when
            var appended = publishAndCaptureAppendedEvent(userEvent);

            // then
            assertThat(appended.metadata())
                    .containsEntry(METADATA_KEY_WORKFLOW_ID, WORKFLOW_ID)
                    .containsEntry(METADATA_KEY_STEP_NAME, STEP_NAME);
        }

        @Test
        @SuppressWarnings("unchecked")
        void waitsUntilThePublishedStepIsCompletedInState() throws InterruptedException {
            // when
            WorkflowStepResult result = delegate.publish(PrimitiveCommands.publish(STEP_NAME,
                                                                                   userEvent(Metadata.emptyInstance())));

            // then
            assertThat(result.getStepName()).isEqualTo(STEP_NAME);
            assertThat(result.success()).isTrue();
            ArgumentCaptor<Predicate<WorkflowState>> predicateCaptor = ArgumentCaptor.forClass(Predicate.class);
            verify(workflowExecution).awaitStateChange(predicateCaptor.capture());
            var predicate = predicateCaptor.getValue();

            WorkflowState awaited = mock(WorkflowState.class);
            WorkflowStep step = mock(WorkflowStep.class);
            when(awaited.containsStep(STEP_NAME)).thenReturn(false);
            assertThat(predicate.test(awaited)).isFalse();
            when(awaited.containsStep(STEP_NAME)).thenReturn(true);
            when(awaited.getStep(STEP_NAME)).thenReturn(step);
            when(step.status()).thenReturn(StepStatus.COMPLETED);
            assertThat(predicate.test(awaited)).isTrue();
        }
    }

    @Nested
    class StepAlreadyInState {

        @Test
        void doesNotPublishAgainAndReturnsCompleted() {
            // given: replay or post-crash re-run, the published step is already durable
            when(state.containsStep(STEP_NAME)).thenReturn(true);

            // when
            WorkflowStepResult result = delegate.publish(PrimitiveCommands.publish(STEP_NAME,
                                                                                   userEvent(Metadata.emptyInstance())));

            // then
            verify(workflowExecution, never()).appendTask(any());
            verify(workflowExecution, never()).appendWorkflowEvent(any(), any());
            assertThat(result.success()).isTrue();
        }
    }

    @Nested
    class ReplayDrift {

        @Test
        void throwsDriftExceptionWhenUnreferencedTerminalStepsAreInState() {
            // given: state holds a terminal step "B" the current body invocation never reached
            reachedSteps.record("A");
            when(state.workflowStepNames()).thenReturn(List.of("A", "B"));
            when(state.getStep("A")).thenReturn(terminalStep("A"));
            when(state.getStep("B")).thenReturn(terminalStep("B"));
            when(state.containsStep(STEP_NAME)).thenReturn(false);

            // when / then
            assertThatThrownBy(() -> delegate.publish(PrimitiveCommands.publish(STEP_NAME,
                                                                                userEvent(Metadata.emptyInstance()))))
                    .isInstanceOf(WorkflowReplayDriftException.class)
                    .satisfies(ex -> {
                        var drift = (WorkflowReplayDriftException) ex;
                        assertThat(drift.aboutToExecute()).isEqualTo(STEP_NAME);
                        assertThat(drift.orphans()).containsExactly("B");
                    });
            verify(workflowExecution, never()).appendTask(any());
        }

        private WorkflowStep terminalStep(String name) {
            return new WorkflowStep(name, StepStatus.COMPLETED, null, null, Instant.now(), null);
        }
    }

    @Nested
    class InterruptedAwait {

        @Test
        void returnsCanceledAndKeepsTheInterruptFlag() throws InterruptedException {
            // given
            when(state.containsStep(STEP_NAME)).thenReturn(false);
            doThrow(new InterruptedException()).when(workflowExecution).awaitStateChange(any());

            // when
            WorkflowStepResult result = delegate.publish(PrimitiveCommands.publish(STEP_NAME,
                                                                                   userEvent(Metadata.emptyInstance())));

            // then
            assertThat(result.canceled()).isTrue();
            assertThat(Thread.interrupted()).as("interrupt flag must be re-set for the caller").isTrue();
        }
    }
}
