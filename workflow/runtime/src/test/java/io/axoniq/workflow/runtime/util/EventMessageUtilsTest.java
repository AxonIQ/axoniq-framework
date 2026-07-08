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
package io.axoniq.workflow.runtime.util;

import io.axoniq.workflow.runtime.api.execution.context.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowDefinitionId;
import io.axoniq.workflow.runtime.api.execution.state.StepRetryInfo;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowError;
import io.axoniq.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.junit.jupiter.api.*;

import java.lang.reflect.Constructor;
import java.time.Instant;
import java.util.Map;

import static io.axoniq.workflow.runtime.execution.payload.CombineGlobalAndLocalPayloadReducer.NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
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
    private WorkflowDefinitionId workflowDefinitionId;
    private final String workflowId = "wf123";
    private final Map<String, Object> payload = Map.of("key", "value");

    @BeforeEach
    void setUp() {
        context = mock(WorkflowContext.class);
        customizer = mock(EventNameCustomizer.class);
        workflowDefinitionId = new WorkflowDefinitionId(new QualifiedName("myWorkflow"), "0.0.1");

        when(context.workflowId()).thenReturn(workflowId);
        when(context.workflowPayload()).thenReturn(payload);
        when(context.workflowVersion()).thenReturn(org.axonframework.messaging.core.MessageType.DEFAULT_VERSION);

        when(customizer.getEventName(anyString(), anyMap(), any(WorkflowStatus.class)))
                .thenAnswer(inv -> new QualifiedName(
                        inv.getArgument(0).toString() + "." + inv.getArgument(2).toString()));
        when(customizer.getEventName(anyString(), anyMap(), any(StepStatus.class)))
                .thenAnswer(inv -> new QualifiedName(
                        inv.getArgument(0).toString() + "." + inv.getArgument(2).toString()));
        when(customizer.versionMigrationEventName(anyString(), anyMap()))
                .thenAnswer(inv -> new QualifiedName(
                        inv.getArgument(0).toString() + ".Versioned"));
    }

    @Test
    void testConstructorIsPrivate() throws NoSuchMethodException {
        Constructor<EventMessageUtils> constructor = EventMessageUtils.class.getDeclaredConstructor();
        assertThat(java.lang.reflect.Modifier.isPrivate(constructor.getModifiers())).isTrue();
        constructor.setAccessible(true);
        assertThatCode(constructor::newInstance).doesNotThrowAnyException();
    }

    @Test
    void testWorkflowIdFilter() {
        EventMessage messageMatch = mock(EventMessage.class);
        when(messageMatch.metadata()).thenReturn(MetadataUtils.create(workflowId));

        EventMessage messageMismatch = mock(EventMessage.class);
        when(messageMismatch.metadata()).thenReturn(MetadataUtils.create("other"));

        var filter = EventMessageUtils.workflowIdFilter(workflowId);
        assertThat(filter.test(messageMatch)).isTrue();
        assertThat(filter.test(messageMismatch)).isFalse();
    }

    @Test
    void testStartedWorkflow() {
        EventMessage message = EventMessageUtils.startedWorkflow(context, "myWorkflow", workflowDefinitionId, customizer);
        assertThat(message.type().toString()).startsWith("myWorkflow.STARTED");
        assertThat(message.payload()).isEqualTo(payload);
        assertThat(MetadataUtils.getWorkflowId(message.metadata())).isEqualTo(workflowId);
        assertThat(MetadataUtils.getWorkflowStatus(message.metadata())).contains(WorkflowStatus.STARTED);
        assertThat(MetadataUtils.getWorkflowDefinitionId(message.metadata())).contains(workflowDefinitionId);
        assertThat(MetadataUtils.payloadReducer(message.metadata())).contains(NAME);
    }

    @Test
    void testCompletedWorkflow() {
        EventMessage message = EventMessageUtils.completedWorkflow(context, "myWorkflow", workflowDefinitionId, customizer);
        assertThat(message.type().toString()).startsWith("myWorkflow.COMPLETED");
        assertThat(message.payload()).isEqualTo(Map.of());
        assertThat(MetadataUtils.getWorkflowId(message.metadata())).isEqualTo(workflowId);
        assertThat(MetadataUtils.getWorkflowStatus(message.metadata())).contains(WorkflowStatus.COMPLETED);
        assertThat(MetadataUtils.getWorkflowDefinitionId(message.metadata())).contains(workflowDefinitionId);
    }

    @Test
    void testFailedWorkflow() {
        Exception ex = new RuntimeException("fail");
        EventMessage message = EventMessageUtils.failedWorkflow(context, "myWorkflow", ex, workflowDefinitionId, customizer);
        assertThat(message.type().toString()).startsWith("myWorkflow.FAILED");
        assertThat(message.payload()).isInstanceOfSatisfying(WorkflowError.class, err -> {
            assertThat(err.type()).isEqualTo(RuntimeException.class.getName());
            assertThat(err.message()).isEqualTo("fail");
            assertThat(err.cause()).isNull();
        });
        assertThat(MetadataUtils.getWorkflowId(message.metadata())).isEqualTo(workflowId);
        assertThat(MetadataUtils.getWorkflowStatus(message.metadata())).contains(WorkflowStatus.FAILED);
        assertThat(MetadataUtils.getWorkflowDefinitionId(message.metadata())).contains(workflowDefinitionId);
    }

    @Test
    void testFailedWorkflowCompactsCauseChain() {
        Throwable root = new IllegalStateException("root");
        Throwable wrapped = new RuntimeException("wrap", root);
        EventMessage message = EventMessageUtils.failedWorkflow(context, "myWorkflow", (Exception) wrapped, workflowDefinitionId, customizer);
        WorkflowError err = (WorkflowError) message.payload();
        assertThat(err.type()).isEqualTo(RuntimeException.class.getName());
        assertThat(err.cause()).isNotNull();
        assertThat(err.cause().type()).isEqualTo(IllegalStateException.class.getName());
        assertThat(err.cause().message()).isEqualTo("root");
        assertThat(err.cause().cause()).isNull();
    }

    @Test
    void testTimeoutWorkflow() {
        Instant now = Instant.now();
        EventMessage message = EventMessageUtils.timeoutWorkflow(context, "myWorkflow", now, workflowDefinitionId, customizer);
        assertThat(message.type().toString()).startsWith("myWorkflow.TIMED_OUT");
        assertThat(message.payload()).isEqualTo(now);
        assertThat(MetadataUtils.getWorkflowId(message.metadata())).isEqualTo(workflowId);
        assertThat(MetadataUtils.getWorkflowStatus(message.metadata())).contains(WorkflowStatus.TIMED_OUT);
        assertThat(MetadataUtils.getWorkflowDefinitionId(message.metadata())).contains(workflowDefinitionId);
    }

    @Test
    void testCancelledWorkflow() {
        EventMessage message = EventMessageUtils.cancelledWorkflow(context, "myWorkflow", workflowDefinitionId, customizer);
        assertThat(message.type().toString()).startsWith("myWorkflow.CANCELLED");
        assertThat(message.payload()).isEqualTo(Map.of());
        assertThat(MetadataUtils.getWorkflowId(message.metadata())).isEqualTo(workflowId);
        assertThat(MetadataUtils.getWorkflowStatus(message.metadata())).contains(WorkflowStatus.CANCELLED);
        assertThat(MetadataUtils.getWorkflowDefinitionId(message.metadata())).contains(workflowDefinitionId);
    }

    @Test
    void testCancelledWorkflowWithCause() {
        Throwable cause = new RuntimeException("cancelled");
        EventMessage message = EventMessageUtils.cancelledWorkflow(context, "myWorkflow", cause, workflowDefinitionId, customizer);
        assertThat(message.type().toString()).startsWith("myWorkflow.CANCELLED");
        assertThat(message.payload()).isInstanceOfSatisfying(WorkflowError.class, err -> {
            assertThat(err.type()).isEqualTo(RuntimeException.class.getName());
            assertThat(err.message()).isEqualTo("cancelled");
        });
        assertThat(MetadataUtils.getWorkflowId(message.metadata())).isEqualTo(workflowId);
        assertThat(MetadataUtils.getWorkflowDefinitionId(message.metadata())).contains(workflowDefinitionId);
    }

    @Test
    void testStartedStep() {
        Map<String, Object> local = Map.of("localKey", "localVal");
        EventMessage message = EventMessageUtils.startedStep(context, "step1", local, customizer);
        assertThat(message.type().toString()).startsWith("step1.STARTED");
        assertThat(message.payload()).isEqualTo(local);
        assertThat(MetadataUtils.getWorkflowId(message.metadata())).isEqualTo(workflowId);
        assertThat(MetadataUtils.getStepName(message.metadata())).isEqualTo("step1");
        assertThat(MetadataUtils.getStepStatus(message.metadata())).contains(StepStatus.STARTED);
    }

    @Test
    void testCompletedStep() {
        Map<String, Object> result = Map.of("res", "val");
        EventMessage message = EventMessageUtils.completedStep(context, "step1", result, "myReducer", customizer);
        assertThat(message.type().toString()).startsWith("step1.COMPLETED");
        assertThat(message.payload()).isEqualTo(result);
        assertThat(MetadataUtils.getWorkflowId(message.metadata())).isEqualTo(workflowId);
        assertThat(MetadataUtils.getStepName(message.metadata())).isEqualTo("step1");
        assertThat(MetadataUtils.getStepStatus(message.metadata())).contains(StepStatus.COMPLETED);
        assertThat(MetadataUtils.payloadReducer(message.metadata())).contains("myReducer");
    }

    @Test
    void testCompletedStepNoReducer() {
        Map<String, Object> result = Map.of("res", "val");
        EventMessage message = EventMessageUtils.completedStep(context, "step1", result, null, customizer);
        assertThat(MetadataUtils.payloadReducer(message.metadata())).isEmpty();
    }

    @Test
    void testFailStep() {
        Throwable ex = new RuntimeException("step fail");
        EventMessage message = EventMessageUtils.failStep(context, "step1", ex, customizer);
        assertThat(message.type().toString()).startsWith("step1.FAILED");
        assertThat(message.payload()).isInstanceOfSatisfying(WorkflowError.class, err -> {
            assertThat(err.type()).isEqualTo(RuntimeException.class.getName());
            assertThat(err.message()).isEqualTo("step fail");
        });
        assertThat(MetadataUtils.getWorkflowId(message.metadata())).isEqualTo(workflowId);
        assertThat(MetadataUtils.getStepName(message.metadata())).isEqualTo("step1");
        assertThat(MetadataUtils.getStepStatus(message.metadata())).contains(StepStatus.FAILED);
    }

    @Test
    void testCancelledStep() {
        EventMessage message = EventMessageUtils.cancelledStep(context, "step1", customizer);
        assertThat(message.type().toString()).startsWith("step1.CANCELLED");
        assertThat(message.payload()).isEqualTo(Map.of());
        assertThat(MetadataUtils.getWorkflowId(message.metadata())).isEqualTo(workflowId);
        assertThat(MetadataUtils.getStepName(message.metadata())).isEqualTo("step1");
        assertThat(MetadataUtils.getStepStatus(message.metadata())).contains(StepStatus.CANCELLED);
    }

    @Test
    void testCancelledStepWithCause() {
        Throwable cause = new RuntimeException("step cancelled");
        EventMessage message = EventMessageUtils.cancelledStep(context, "step1", cause, customizer);
        assertThat(message.type().toString()).startsWith("step1.CANCELLED");
        assertThat(message.payload()).isInstanceOfSatisfying(WorkflowError.class, err -> {
            assertThat(err.type()).isEqualTo(RuntimeException.class.getName());
            assertThat(err.message()).isEqualTo("step cancelled");
        });
        assertThat(MetadataUtils.getWorkflowId(message.metadata())).isEqualTo(workflowId);
    }

    @Test
    void testRetryingStep() {
        StepRetryInfo retryInfo = mock(StepRetryInfo.class);
        EventMessage message = EventMessageUtils.retryingStep(context, "step1", retryInfo, customizer);
        assertThat(message.type().toString()).startsWith("step1.RETRYING");
        assertThat(message.payload()).isEqualTo(retryInfo);
        assertThat(MetadataUtils.getWorkflowId(message.metadata())).isEqualTo(workflowId);
        assertThat(MetadataUtils.getStepName(message.metadata())).isEqualTo("step1");
        assertThat(MetadataUtils.getStepStatus(message.metadata())).contains(StepStatus.RETRYING);
    }

    @Test
    void testTimeoutStep() {
        Instant now = Instant.now();
        EventMessage message = EventMessageUtils.timeoutStep(context, "step1", now, customizer);
        assertThat(message.type().toString()).startsWith("step1.TIMED_OUT");
        assertThat(message.payload()).isEqualTo(now);
        assertThat(MetadataUtils.getWorkflowId(message.metadata())).isEqualTo(workflowId);
        assertThat(MetadataUtils.getStepName(message.metadata())).isEqualTo("step1");
        assertThat(MetadataUtils.getStepStatus(message.metadata())).contains(StepStatus.TIMED_OUT);
    }

    @Test
    void testMigrationStep_eventNameDerivedFromChangeId() {
        EventMessage message = EventMessageUtils.versionMigrationStep(context, "payment-redesign", "0.0.2", customizer);

        // The wire-level event name is the changeId + .Versioned suffix — business-meaningful "what changed".
        assertThat(message.type().toString()).startsWith("payment-redesign.Versioned");
        // The marker's MessageType.version() carries the new workflow version directly.
        assertThat(message.type().version()).isEqualTo("0.0.2");
        assertThat(message.payload()).isEqualTo(Map.of("changeId", "payment-redesign", "version", "0.0.2"));
        assertThat(MetadataUtils.getWorkflowId(message.metadata())).isEqualTo(workflowId);
        // The discriminator follows the modifyPayload pattern: presence of versionChangeId metadata.
        assertThat(MetadataUtils.isVersionMigrationStep(message.metadata())).isTrue();
        assertThat(MetadataUtils.getVersionChangeId(message.metadata())).contains("payment-redesign");
        assertThat(MetadataUtils.getVersion(message.metadata())).contains("0.0.2");
    }
}
