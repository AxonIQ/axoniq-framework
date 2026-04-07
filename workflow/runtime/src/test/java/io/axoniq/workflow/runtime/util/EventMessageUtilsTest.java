/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */
package io.axoniq.workflow.runtime.util;

import io.axoniq.workflow.runtime.api.execution.context.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.api.execution.state.StepRetryInfo;
import io.axoniq.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.workflow.runtime.api.payload.PayloadReducer;
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.workflow.runtime.util.EventMessageUtils;
import io.axoniq.workflow.runtime.util.MetadataUtils;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.junit.jupiter.api.*;

import java.lang.reflect.Constructor;
import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit test class for the {@code EventMessageUtils} utility class.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
class EventMessageUtilsTest {

    private WorkflowContext context;
    private EventNameCustomizer customizer;
    private final String workflowId = "wf123";
    private final Map<String, Object> payload = Map.of("key", "value");

    @BeforeEach
    void setUp() {
        context = mock(WorkflowContext.class);
        customizer = mock(EventNameCustomizer.class);

        when(context.workflowId()).thenReturn(workflowId);
        when(context.workflowPayload()).thenReturn(payload);

        when(customizer.getEventName(anyString(), anyMap(), any(WorkflowStatus.class)))
                .thenAnswer(inv -> new QualifiedName(
                        inv.getArgument(0).toString() + "." + inv.getArgument(2).toString()));
        when(customizer.getEventName(anyString(), anyMap(), any(StepStatus.class)))
                .thenAnswer(inv -> new QualifiedName(
                        inv.getArgument(0).toString() + "." + inv.getArgument(2).toString()));
    }

    @Test
    void testConstructorIsPrivate() throws NoSuchMethodException {
        Constructor<EventMessageUtils> constructor = EventMessageUtils.class.getDeclaredConstructor();
        assertTrue(java.lang.reflect.Modifier.isPrivate(constructor.getModifiers()));
        constructor.setAccessible(true);
        assertDoesNotThrow(() -> {
            constructor.newInstance();
        });
    }

    @Test
    void testWorkflowIdFilter() {
        EventMessage messageMatch = mock(EventMessage.class);
        when(messageMatch.metadata()).thenReturn(MetadataUtils.create(workflowId));

        EventMessage messageMismatch = mock(EventMessage.class);
        when(messageMismatch.metadata()).thenReturn(MetadataUtils.create("other"));

        var filter = EventMessageUtils.workflowIdFilter(workflowId);
        assertTrue(filter.test(messageMatch));
        assertFalse(filter.test(messageMismatch));
    }

    @Test
    void testStartedWorkflow() {
        EventMessage message = EventMessageUtils.startedWorkflow(context, "myWorkflow", customizer);
        assertTrue(message.type().toString().startsWith("myWorkflow.STARTED"));
        assertEquals(payload, message.payload());
        assertEquals(workflowId, MetadataUtils.getWorkflowId(message.metadata()));
        assertEquals(WorkflowStatus.STARTED, MetadataUtils.getWorkflowStatus(message.metadata()).orElse(null));
        assertEquals(PayloadReducer.NAME_COMBINE_LOCAL_AND_GLOBAL,
                     MetadataUtils.payloadReducer(message.metadata()).orElse(null));
    }

    @Test
    void testCompletedWorkflow() {
        EventMessage message = EventMessageUtils.completedWorkflow(context, "myWorkflow", customizer);
        assertTrue(message.type().toString().startsWith("myWorkflow.COMPLETED"));
        assertEquals(Map.of(), message.payload());
        assertEquals(workflowId, MetadataUtils.getWorkflowId(message.metadata()));
        assertEquals(WorkflowStatus.COMPLETED, MetadataUtils.getWorkflowStatus(message.metadata()).orElse(null));
    }

    @Test
    void testFailedWorkflow() {
        Exception ex = new RuntimeException("fail");
        EventMessage message = EventMessageUtils.failedWorkflow(context, "myWorkflow", ex, customizer);
        assertTrue(message.type().toString().startsWith("myWorkflow.FAILED"));
        assertEquals(ex, message.payload());
        assertEquals(workflowId, MetadataUtils.getWorkflowId(message.metadata()));
        assertEquals(WorkflowStatus.FAILED, MetadataUtils.getWorkflowStatus(message.metadata()).orElse(null));
    }

    @Test
    void testTimeoutWorkflow() {
        Instant now = Instant.now();
        EventMessage message = EventMessageUtils.timeoutWorkflow(context, "myWorkflow", now, customizer);
        assertTrue(message.type().toString().startsWith("myWorkflow.TIMED_OUT"));
        assertEquals(now, message.payload());
        assertEquals(workflowId, MetadataUtils.getWorkflowId(message.metadata()));
        assertEquals(WorkflowStatus.TIMED_OUT, MetadataUtils.getWorkflowStatus(message.metadata()).orElse(null));
    }

    @Test
    void testCancelledWorkflow() {
        EventMessage message = EventMessageUtils.cancelledWorkflow(context, "myWorkflow", customizer);
        assertTrue(message.type().toString().startsWith("myWorkflow.CANCELLED"));
        assertEquals(Map.of(), message.payload());
        assertEquals(workflowId, MetadataUtils.getWorkflowId(message.metadata()));
        assertEquals(WorkflowStatus.CANCELLED, MetadataUtils.getWorkflowStatus(message.metadata()).orElse(null));
    }

    @Test
    void testCancelledWorkflowWithCause() {
        Throwable cause = new RuntimeException("cancelled");
        EventMessage message = EventMessageUtils.cancelledWorkflow(context, "myWorkflow", cause, customizer);
        assertTrue(message.type().toString().startsWith("myWorkflow.CANCELLED"));
        assertEquals(cause, message.payload());
        assertEquals(workflowId, MetadataUtils.getWorkflowId(message.metadata()));
    }

    @Test
    void testStartedStep() {
        Map<String, Object> local = Map.of("localKey", "localVal");
        EventMessage message = EventMessageUtils.startedStep(context, "step1", local, customizer);
        assertTrue(message.type().toString().startsWith("step1.STARTED"));
        assertEquals(local, message.payload());
        assertEquals(workflowId, MetadataUtils.getWorkflowId(message.metadata()));
        assertEquals("step1", MetadataUtils.getStepName(message.metadata()));
        assertEquals(StepStatus.STARTED, MetadataUtils.getStepStatus(message.metadata()).orElse(null));
    }

    @Test
    void testCompletedStep() {
        Map<String, Object> result = Map.of("res", "val");
        EventMessage message = EventMessageUtils.completedStep(context, "step1", result, "myReducer", customizer);
        assertTrue(message.type().toString().startsWith("step1.COMPLETED"));
        assertEquals(result, message.payload());
        assertEquals(workflowId, MetadataUtils.getWorkflowId(message.metadata()));
        assertEquals("step1", MetadataUtils.getStepName(message.metadata()));
        assertEquals(StepStatus.COMPLETED, MetadataUtils.getStepStatus(message.metadata()).orElse(null));
        assertEquals("myReducer", MetadataUtils.payloadReducer(message.metadata()).orElse(null));
    }

    @Test
    void testCompletedStepNoReducer() {
        Map<String, Object> result = Map.of("res", "val");
        EventMessage message = EventMessageUtils.completedStep(context, "step1", result, null, customizer);
        assertFalse(MetadataUtils.payloadReducer(message.metadata()).isPresent());
    }

    @Test
    void testFailStep() {
        Throwable ex = new RuntimeException("step fail");
        EventMessage message = EventMessageUtils.failStep(context, "step1", ex, customizer);
        assertTrue(message.type().toString().startsWith("step1.FAILED"));
        assertEquals(ex, message.payload());
        assertEquals(workflowId, MetadataUtils.getWorkflowId(message.metadata()));
        assertEquals("step1", MetadataUtils.getStepName(message.metadata()));
        assertEquals(StepStatus.FAILED, MetadataUtils.getStepStatus(message.metadata()).orElse(null));
    }

    @Test
    void testCancelledStep() {
        EventMessage message = EventMessageUtils.cancelledStep(context, "step1", customizer);
        assertTrue(message.type().toString().startsWith("step1.CANCELLED"));
        assertEquals(Map.of(), message.payload());
        assertEquals(workflowId, MetadataUtils.getWorkflowId(message.metadata()));
        assertEquals("step1", MetadataUtils.getStepName(message.metadata()));
        assertEquals(StepStatus.CANCELLED, MetadataUtils.getStepStatus(message.metadata()).orElse(null));
    }

    @Test
    void testCancelledStepWithCause() {
        Throwable cause = new RuntimeException("step cancelled");
        EventMessage message = EventMessageUtils.cancelledStep(context, "step1", cause, customizer);
        assertTrue(message.type().toString().startsWith("step1.CANCELLED"));
        assertEquals(cause, message.payload());
        assertEquals(workflowId, MetadataUtils.getWorkflowId(message.metadata()));
    }

    @Test
    void testRetryingStep() {
        StepRetryInfo retryInfo = mock(StepRetryInfo.class);
        EventMessage message = EventMessageUtils.retryingStep(context, "step1", retryInfo, customizer);
        assertTrue(message.type().toString().startsWith("step1.RETRYING"));
        assertEquals(retryInfo, message.payload());
        assertEquals(workflowId, MetadataUtils.getWorkflowId(message.metadata()));
        assertEquals("step1", MetadataUtils.getStepName(message.metadata()));
        assertEquals(StepStatus.RETRYING, MetadataUtils.getStepStatus(message.metadata()).orElse(null));
    }

    @Test
    void testTimeoutStep() {
        Instant now = Instant.now();
        EventMessage message = EventMessageUtils.timeoutStep(context, "step1", now, customizer);
        assertTrue(message.type().toString().startsWith("step1.TIMED_OUT"));
        assertEquals(now, message.payload());
        assertEquals(workflowId, MetadataUtils.getWorkflowId(message.metadata()));
        assertEquals("step1", MetadataUtils.getStepName(message.metadata()));
        assertEquals(StepStatus.TIMED_OUT, MetadataUtils.getStepStatus(message.metadata()).orElse(null));
    }
}
