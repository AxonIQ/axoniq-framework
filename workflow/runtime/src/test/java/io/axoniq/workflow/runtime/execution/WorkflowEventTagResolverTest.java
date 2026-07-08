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

import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.util.EventMessageUtils;
import io.axoniq.workflow.runtime.util.WorkflowEventTagResolver;
import org.axonframework.messaging.eventstreaming.Tag;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class WorkflowEventTagResolverTest {

    private final WorkflowEventTagResolver resolver = new WorkflowEventTagResolver();
    private WorkflowContext context;

    @BeforeEach
    void setUp() {
        context = mock(WorkflowContext.class);
        when(context.workflowId()).thenReturn("wf-123");
        when(context.workflowPayload()).thenReturn(Map.of("orderId", "123"));
        when(context.workflowVersion()).thenReturn("0.0.1");
    }

    @Test
    void workflowLifecycleEventsGetWorkflowIdAndLifecycleTags() {
        var customizer = DefaultEventNameCustomizer.Builder.defaults();
        var started = EventMessageUtils.startedWorkflow(context, "OrderWorkflow", customizer);
        var timedOut = EventMessageUtils.timeoutWorkflow(context, "OrderWorkflow", Instant.parse("2026-07-08T10:15:00Z"),
                                                        customizer);

        assertThat(resolver.resolve(started)).isEqualTo(Set.of(
                Tag.of("workflowId", "wf-123"),
                Tag.of("workflowLifecycle", "started")
        ));
        assertThat(resolver.resolve(timedOut)).isEqualTo(Set.of(
                Tag.of("workflowId", "wf-123"),
                Tag.of("workflowLifecycle", "timeout")
        ));
    }

    @Test
    void waitForEventStepEventsGetWorkflowWaitTag() {
        var customizer = DefaultEventNameCustomizer.Builder.defaults();
        var started = EventMessageUtils.startedWaitForEventStep(
                context,
                "awaitPayment",
                Map.of(
                        "startTime", Instant.parse("2026-07-08T10:00:00Z"),
                        "eventQualifiedName", "io.acme.PaymentConfirmed",
                        "serializedAssociations", Set.of("payload:orderId=123"),
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
                Tag.of("workflowId", "wf-123"),
                Tag.of("workflowWait", "started")
        ));
        assertThat(resolver.resolve(completed)).isEqualTo(Set.of(
                Tag.of("workflowId", "wf-123"),
                Tag.of("workflowWait", "completed")
        ));
    }

    @Test
    void regularStepEventsKeepOnlyWorkflowIdTag() {
        var customizer = DefaultEventNameCustomizer.Builder.defaults();
        var started = EventMessageUtils.startedStep(context, "shipOrder", Map.of("x", "y"), customizer);

        assertThat(resolver.resolve(started)).isEqualTo(Set.of(Tag.of("workflowId", "wf-123")));
    }
}
