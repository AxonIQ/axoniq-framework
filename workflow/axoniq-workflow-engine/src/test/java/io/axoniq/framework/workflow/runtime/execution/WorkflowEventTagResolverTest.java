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

import io.axoniq.framework.workflow.runtime.api.execution.context.EventNameCustomizer;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.framework.workflow.runtime.api.execution.state.StepRetryInfo;
import io.axoniq.framework.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.framework.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.util.EventMessageUtils;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.axonframework.messaging.eventstreaming.Tag;
import org.junit.jupiter.api.*;

import java.time.Instant;
import java.util.Map;
import java.util.Set;

import static io.axoniq.framework.workflow.runtime.execution.WorkflowEventTags.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class WorkflowEventTagResolverTest {

    private final WorkflowEventTagResolver resolver = new WorkflowEventTagResolver();
    private final EventNameCustomizer customizer = new TestEventNameCustomizer();
    private final MessageType workflowDefinitionId = new MessageType(new QualifiedName("OrderWorkflow"), "0.0.1");
    private WorkflowContext context;

    @BeforeEach
    void setUp() {
        context = mock(WorkflowContext.class);
        when(context.workflowId()).thenReturn("wf-123");
        when(context.workflowPayload()).thenReturn(Map.of("orderId", "123"));
        when(context.workflowVersion()).thenReturn("0.0.1");
        ProcessingContext processingContext = mock(ProcessingContext.class);
        when(context.processingContext()).thenReturn(processingContext);
        when(processingContext.component(EventConverter.class)).thenReturn(mock(EventConverter.class));
    }

    @Test
    void workflowLifecycleEventsGetWorkflowIdAndLifecycleTags() {
        var started = EventMessageUtils.startedWorkflow(context, "OrderWorkflow", workflowDefinitionId, customizer);
        var timedOut = EventMessageUtils.timeoutWorkflow(context,
                                                         "OrderWorkflow",
                                                         Instant.parse("2026-07-08T10:15:00Z"),
                                                         workflowDefinitionId,
                                                         customizer);

        assertThat(resolver.resolve(started)).isEqualTo(Set.of(
                Tag.of(TAG_WORKFLOW_ID, "wf-123"),
                Tag.of(TAG_WORKFLOW_EVENT_TYPE, TAG_VALUE_EVENT_TYPE_LIFECYCLE)
        ));
        assertThat(resolver.resolve(timedOut)).isEqualTo(Set.of(
                Tag.of(TAG_WORKFLOW_ID, "wf-123"),
                Tag.of(TAG_WORKFLOW_EVENT_TYPE, TAG_VALUE_EVENT_TYPE_LIFECYCLE)
        ));
    }

    @Test
    void waitForEventStepEventsGetWorkflowWaitTag() {
        var started = EventMessageUtils.startedWaitForEventStep(
                context,
                "awaitPayment",
                Map.of(
                        "startTime", Instant.parse("2026-07-08T10:00:00Z"),
                        "eventQualifiedName", "io.acme.PaymentConfirmed",
                        "criteria", Set.of("payload:orderId=123"),
                        "timeoutTime", Instant.parse("2026-07-08T10:15:00Z")
                ),
                customizer
        );
        var completed = EventMessageUtils.completedWaitForEventStep(
                context,
                "awaitPayment",
                Map.of("orderId", "123"),
                "GLOBAL_ONLY",
                customizer
        );

        assertThat(resolver.resolve(started)).isEqualTo(Set.of(
                Tag.of(TAG_WORKFLOW_ID, "wf-123"),
                Tag.of(TAG_WORKFLOW_EVENT_TYPE, TAG_VALUE_EVENT_TYPE_WAIT_STEP)
        ));
        assertThat(resolver.resolve(completed)).isEqualTo(Set.of(
                Tag.of(TAG_WORKFLOW_ID, "wf-123"),
                Tag.of(TAG_WORKFLOW_EVENT_TYPE, TAG_VALUE_EVENT_TYPE_WAIT_STEP)
        ));
    }

    @Test
    void regularStepEventsKeepOnlyWorkflowIdTag() {
        var started = EventMessageUtils.startedStep(context, "shipOrder", Map.of("x", "y"), customizer);

        assertThat(resolver.resolve(started)).isEqualTo(Set.of(Tag.of("workflowId", "wf-123")));
    }

    @Test
    void retryingWaitForEventStepDoesNotGetWorkflowWaitTag() {
        var retrying = retryingWaitForEventStep(customizer);

        assertThat(resolver.resolve(retrying)).isEqualTo(Set.of(Tag.of("workflowId", "wf-123")));
    }

    private EventMessage retryingWaitForEventStep(EventNameCustomizer customizer) {
        var retryInfo = mock(StepRetryInfo.class);
        var retrying = EventMessageUtils.retryingStep(context, "awaitPayment", retryInfo, customizer);
        return mockEventMessage(MetadataUtils.markWaitForEventStep(retrying.metadata()));
    }

    private EventMessage mockEventMessage(org.axonframework.messaging.core.Metadata metadata) {
        EventMessage eventMessage = mock(EventMessage.class);
        when(eventMessage.metadata()).thenReturn(metadata);
        return eventMessage;
    }

    private static final class TestEventNameCustomizer implements EventNameCustomizer {

        @Override
        public QualifiedName getEventName(String stepName,
                                                   Map<String, @Nullable Object> parameters,
                                                   StepStatus stepStatus) {
            return new QualifiedName("test", stepName + stepStatus.name());
        }

        @Override
        public QualifiedName getEventName(String stepName,
                                                   Map<String, @Nullable Object> parameters,
                                                   WorkflowStatus stepStatus) {
            return new QualifiedName("test", stepName + stepStatus.name());
        }

        @Override
        public EventNameCustomizer forStepInheritance() {
            return this;
        }
    }
}
