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

import io.axoniq.workflow.runtime.api.execution.state.WorkflowError;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowExecutionException;
import io.axoniq.workflow.runtime.api.payload.PayloadReducer;
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.workflow.runtime.util.MetadataUtils;
import org.axonframework.conversion.Converter;
import org.axonframework.conversion.jackson2.Jackson2Converter;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.junit.jupiter.api.*;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class EventSourcedWorkflowStateWorkflowStatusTest {

    private final Converter converter = new Jackson2Converter();
    private EventSourcedWorkflowState state;
    private ProcessingContext processingContext;

    @BeforeEach
    void setUp() {
        state = new EventSourcedWorkflowState(Map.of("initialKey", "initialValue"));
        processingContext = mock(ProcessingContext.class);
        when(processingContext.component(Converter.class)).thenReturn(converter);
    }

    @Test
    void testEvolvePayloadSuccess() {
        Metadata metadata = MetadataUtils.create("wfId", WorkflowStatus.STARTED)
                                         .and(MetadataUtils.METADATA_KEY_MODIFY_PAYLOAD, PayloadReducer.NAME_COMBINE_LOCAL_AND_GLOBAL);
        EventMessage eventMessage = new GenericEventMessage(new MessageType("evolve"),
                                                            Map.of("newKey", "newValue"),
                                                            metadata);

        state.evolve(eventMessage, processingContext);

        assertThat(state.payload()).containsEntry("initialKey", "initialValue")
                                   .containsEntry("newKey", "newValue");
    }

    @Test
    void testEvolvePayloadWithNonMapPayloadDoesNotThrow() {
        Metadata metadata = MetadataUtils.create("wfId", WorkflowStatus.STARTED)
                                         .and(MetadataUtils.METADATA_KEY_MODIFY_PAYLOAD, PayloadReducer.NAME_COMBINE_LOCAL_AND_GLOBAL);
        // String payload cannot be converted to Map<String, Object> via Jackson as a root object if it is not a JSON object
        EventMessage eventMessage = new GenericEventMessage(new MessageType("evolve"),
                                                            "Not a Map",
                                                            metadata);

        state.evolve(eventMessage, processingContext);

        // Payload should remain unchanged
        assertThat(state.payload()).hasSize(1)
                                   .containsEntry("initialKey", "initialValue");
    }

    @Test
    void testEvolveFailedWithThrowable() {
        RuntimeException exception = new RuntimeException("Workflow failed");
        EventMessage eventMessage = new GenericEventMessage(new MessageType("failed"),
                                                            WorkflowError.from(exception),
                                                            MetadataUtils.create("wfId", WorkflowStatus.FAILED));

        state.evolve(eventMessage, processingContext);

        assertThat(state.workflowStatus()).isEqualTo(WorkflowStatus.FAILED);

        try {
            state.throwTerminalCause();
        } catch (Throwable t) {
            assertThat(t.getCause()).isInstanceOfSatisfying(WorkflowExecutionException.class, e -> {
                assertThat(e.type()).isEqualTo(RuntimeException.class.getName());
                assertThat(e.getMessage()).isEqualTo("Workflow failed");
            });
            return;
        }
        throw new AssertionError("Expected termination cause to be thrown");
    }

    @Test
    void testEvolveCancelledWithThrowable() {
        RuntimeException exception = new RuntimeException("Workflow cancelled");
        EventMessage eventMessage = new GenericEventMessage(new MessageType("cancelled"), WorkflowError.from(exception),
                                                            MetadataUtils.create("wfId", WorkflowStatus.CANCELLED));

        state.evolve(eventMessage, processingContext);

        assertThat(state.workflowStatus()).isEqualTo(WorkflowStatus.CANCELLED);

        try {
            state.throwTerminalCause();
        } catch (Throwable t) {
            assertThat(t.getMessage()).isEqualTo("Workflow cancelled");
            return;
        }
        throw new AssertionError("Expected termination cause to be thrown");
    }

    @Test
    void testEvolveFailedWithNonThrowablePayload() {
        String errorInfo = "Internal Server Error";
        EventMessage eventMessage = new GenericEventMessage(new MessageType("failed"),
                                                            errorInfo,
                                                            MetadataUtils.create("wfId", WorkflowStatus.FAILED));

        state.evolve(eventMessage, processingContext);

        assertThat(state.workflowStatus()).isEqualTo(WorkflowStatus.FAILED);

        try {
            state.throwTerminalCause();
        } catch (Throwable t) {
            assertThat(t).isInstanceOf(io.axoniq.workflow.runtime.api.execution.context.WorkflowFailedException.class);
            assertThat(t.getCause()).isNotNull();
            assertThat(t.getCause().getMessage()).isEqualTo("Workflow already failed");
            return;
        }
        throw new AssertionError("Expected termination cause to be thrown");
    }

    @Test
    void testEvolveCancelledWithNonThrowablePayload() {
        String cancellationReason = "User requested cancellation";
        EventMessage eventMessage = new GenericEventMessage(new MessageType("cancelled"),
                                                            cancellationReason,
                                                            MetadataUtils.create("wfId", WorkflowStatus.CANCELLED));

        state.evolve(eventMessage, processingContext);

        assertThat(state.workflowStatus()).isEqualTo(WorkflowStatus.CANCELLED);

        try {
            state.throwTerminalCause();
        } catch (Throwable t) {
            assertThat(t).isInstanceOf(io.axoniq.workflow.runtime.api.execution.context.WorkflowCancelledException.class);
            assertThat(t.getMessage()).isEqualTo("Workflow already cancelled");
            return;
        }
        throw new AssertionError("Expected termination cause to be thrown");
    }
}
