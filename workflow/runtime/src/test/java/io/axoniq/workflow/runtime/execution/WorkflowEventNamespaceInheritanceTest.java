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

import io.axoniq.workflow.runtime.api.execution.context.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.util.EventMessageUtils;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.junit.jupiter.api.*;

import java.util.Map;

import static io.axoniq.workflow.runtime.execution.DefaultEventNameCustomizer.Builder.merge;
import static io.axoniq.workflow.runtime.execution.payload.GlobalOnlyPayloadReducer.NAME_GLOBAL_ONLY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

/**
 * Verifies that step events inherit the workflow namespace by default and that a step override wins when provided.
 */
class WorkflowEventNamespaceInheritanceTest {

    @Test
    void stepEventsInheritWorkflowNamespaceByDefault() {
        // Given a workflow-level namespace
        EventNameCustomizer workflowCustomizer = DefaultEventNameCustomizer.Builder.namespace("wf.ns");
        // And a step-level customizer which is not overriding anything (defaults)
        EventNameCustomizer stepCustomizer = DefaultEventNameCustomizer.Builder.defaults();
        EventNameCustomizer merged = merge(workflowCustomizer, stepCustomizer);

        WorkflowContext ctx = mock(WorkflowContext.class);
        when(ctx.workflowId()).thenReturn("wf-1");
        when(ctx.workflowPayload()).thenReturn(Map.of());

        // When producing a completed step event using the merged customizer
        EventMessage evt = EventMessageUtils.completedStep(ctx,
                                                           "myStep",
                                                           Map.of(),
                                                           NAME_GLOBAL_ONLY,
                                                           merged);

        // Then the namespace should be inherited from the workflow customizer
        assertThat(evt.type().qualifiedName().namespace()).isEqualTo("wf.ns");
        assertThat(evt.type().qualifiedName().localName()).isEqualTo("MyStepCompleted");
    }

    @Test
    void stepNamespaceOverrideWinsOverWorkflowNamespace() {
        // Given a workflow-level namespace
        EventNameCustomizer workflowCustomizer = DefaultEventNameCustomizer.Builder.namespace("wf.ns");
        // And a step-level override for namespace
        EventNameCustomizer stepCustomizer = DefaultEventNameCustomizer.Builder.namespace("step.ns");
        EventNameCustomizer merged = merge(workflowCustomizer, stepCustomizer);

        WorkflowContext ctx = mock(WorkflowContext.class);
        when(ctx.workflowId()).thenReturn("wf-1");
        when(ctx.workflowPayload()).thenReturn(Map.of());

        // When producing a completed step event using the merged customizer
        EventMessage evt = EventMessageUtils.completedStep(ctx,
                                                           "myStep",
                                                           Map.of(),
                                                           NAME_GLOBAL_ONLY,
                                                           merged);

        // Then the step-level namespace should win
        assertThat(evt.type().qualifiedName().namespace()).isEqualTo("step.ns");
    }
}
