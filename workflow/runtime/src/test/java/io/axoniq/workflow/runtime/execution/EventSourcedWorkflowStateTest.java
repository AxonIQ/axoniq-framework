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
package io.axoniq.workflow.runtime.execution;

import io.axoniq.workflow.runtime.api.execution.state.StepRetryInfo;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowError;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowExecutionException;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowStep;
import io.axoniq.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.workflow.runtime.api.payload.PayloadReducer;
import io.axoniq.workflow.runtime.util.MetadataUtils;
import org.axonframework.common.TypeReference;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class EventSourcedWorkflowStateTest {

    private EventSourcedWorkflowState state;
    private ProcessingContext processingContext;
    private Converter converter;

    @BeforeEach
    void setUp() {
        state = new EventSourcedWorkflowState();
        processingContext = mock(ProcessingContext.class);
        converter = mock(Converter.class);
        when(processingContext.component(Converter.class)).thenReturn(converter);
    }

    @Test
    void testEvolveStepStarted() {
        String stepName = "testStep";
        Object payload = "payload";
        Metadata metadata = MetadataUtils.create("workflowId", stepName, StepStatus.STARTED);
        EventMessage eventMessage = mock(EventMessage.class);
        when(eventMessage.metadata()).thenReturn(metadata);
        when(eventMessage.timestamp()).thenReturn(Instant.now());
        when(eventMessage.payloadAs(eq(Object.class), any())).thenReturn(payload);

        state.evolve(eventMessage, processingContext);

        assertThat(state.containsStep(stepName)).isTrue();
        WorkflowStep step = state.getStep(stepName);
        assertThat(step.status()).isEqualTo(StepStatus.STARTED);
        assertThat(step.result()).isEqualTo(payload);
    }

    @Test
    void testEvolveStepTimedOut() {
        String stepName = "testStep";
        Object payload = "timeout-payload";
        Metadata metadata = MetadataUtils.create("workflowId", stepName, StepStatus.TIMED_OUT);
        EventMessage eventMessage = mock(EventMessage.class);
        when(eventMessage.metadata()).thenReturn(metadata);
        when(eventMessage.timestamp()).thenReturn(Instant.now());
        when(eventMessage.payloadAs(eq(Object.class), any())).thenReturn(payload);

        state.evolve(eventMessage, processingContext);

        assertThat(state.containsStep(stepName)).isTrue();
        WorkflowStep step = state.getStep(stepName);
        assertThat(step.status()).isEqualTo(StepStatus.TIMED_OUT);
        assertThat(step.result()).isEqualTo(payload);
    }

    @Test
    void testEvolveStepCompleted() {
        String stepName = "testStep";
        Object payload = "completed-result";
        Metadata metadata = MetadataUtils.create("workflowId", stepName, StepStatus.COMPLETED);
        EventMessage eventMessage = mock(EventMessage.class);
        when(eventMessage.metadata()).thenReturn(metadata);
        when(eventMessage.timestamp()).thenReturn(Instant.now());
        when(eventMessage.payloadAs(eq(Object.class), any())).thenReturn(payload);

        state.evolve(eventMessage, processingContext);

        assertThat(state.containsStep(stepName)).isTrue();
        WorkflowStep step = state.getStep(stepName);
        assertThat(step.status()).isEqualTo(StepStatus.COMPLETED);
        assertThat(step.result()).isEqualTo(payload);
    }

    @Test
    void testEvolveStepCancelled() {
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
    void testEvolveStepRetrying() {
        String stepName = "testStep";
        WorkflowError error = WorkflowError.from(new RuntimeException("retry error"));
        StepRetryInfo retryInfo = mock(StepRetryInfo.class);
        when(retryInfo.error()).thenReturn(error);
        Metadata metadata = MetadataUtils.create("workflowId", stepName, StepStatus.RETRYING);
        EventMessage eventMessage = mock(EventMessage.class);
        when(eventMessage.metadata()).thenReturn(metadata);
        when(eventMessage.timestamp()).thenReturn(Instant.now());
        when(eventMessage.payloadAs(eq(StepRetryInfo.class), any())).thenReturn(retryInfo);

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
    void testEvolveStepCompletedAndEvolvePayload() {
        String stepName = "testStep";
        Map<String, Object> initialPayload = Map.of("key1", "value1");
        state = new EventSourcedWorkflowState(initialPayload);

        Map<String, Object> stepResult = Map.of("key2", "value2");
        Metadata metadata = MetadataUtils.create("workflowId", stepName, StepStatus.COMPLETED)
                                         .and(MetadataUtils.METADATA_KEY_MODIFY_PAYLOAD, PayloadReducer.NAME_COMBINE_LOCAL_AND_GLOBAL);

        EventMessage eventMessage = mock(EventMessage.class);
        when(eventMessage.metadata()).thenReturn(metadata);
        when(eventMessage.timestamp()).thenReturn(Instant.now());
        when(eventMessage.payloadAs(any(TypeReference.class), any())).thenReturn(stepResult);
        when(eventMessage.payloadAs(eq(Object.class), any())).thenReturn(stepResult);

        state.evolve(eventMessage, processingContext);

        assertThat(state.containsStep(stepName)).isTrue();
        assertThat(state.getStep(stepName).status()).isEqualTo(StepStatus.COMPLETED);
        assertThat(state.payload()).containsEntry("key1", "value1")
                                   .containsEntry("key2", "value2");
    }

    @Test
    void testEvolveStepFailed() {
        String stepName = "testStep";
        WorkflowError error = WorkflowError.from(new RuntimeException("Test error"));
        Metadata metadata = MetadataUtils.create("workflowId", stepName, StepStatus.FAILED);
        EventMessage eventMessage = mock(EventMessage.class);
        when(eventMessage.metadata()).thenReturn(metadata);
        when(eventMessage.timestamp()).thenReturn(Instant.now());
        // Verify that the payload is retrieved as WorkflowError before accessing it.
        // Even if payloadAs(Object.class) returns something else, payloadAs(WorkflowError.class) must be used.
        when(eventMessage.payloadAs(eq(Object.class), any())).thenReturn("not-an-error");
        when(eventMessage.payloadAs(eq(WorkflowError.class), any())).thenReturn(error);

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
}
