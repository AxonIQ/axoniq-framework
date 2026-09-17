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

import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecutionOperations;

import org.jspecify.annotations.Nullable;

import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowConfiguration;
import io.axoniq.framework.workflow.dsl.api.WorkflowContext;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.junit.jupiter.api.*;

import java.util.Map;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Test for {@link WorkflowContextAdoptingExecutionFactory}.
 */
class WorkflowContextAdoptingExecutionFactoryTest {

    @Test
    void testConstructorWithValidType() {
        assertThatCode(() -> new WorkflowContextAdoptingExecutionFactory<>(MyWorkflowContext.class)).doesNotThrowAnyException();
    }

    @Test
    void testConstructorWithInvalidType() {
        assertThatThrownBy(() -> new WorkflowContextAdoptingExecutionFactory<>(WorkflowContext.class))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testCreateWithSupportedContext() {
        var factory = new WorkflowContextAdoptingExecutionFactory<>(MyWorkflowContext.class);
        var execution = mock(WorkflowExecution.class);
        var context = mock(MyWorkflowContext.class);
        when(context.execution()).thenReturn(execution);

        var result = factory.create(context);

        assertThat(result).isSameAs(execution);
    }

    @Test
    void testCreateWithUnsupportedContext() {
        var factory = new WorkflowContextAdoptingExecutionFactory<>(MyWorkflowContext.class);
        var context = mock(WorkflowContext.class);

        assertThatThrownBy(() -> factory.create(context))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void workflowContextDoesNotExposeTheRuntimeOperationsInterface() {
        assertThat(WorkflowContext.class.isAssignableFrom(MyWorkflowContext.class)).isTrue();
        assertThat(WorkflowExecutionOperations.class.isAssignableFrom(MyWorkflowContext.class)).isFalse();
    }

    /**
     * Helper class for testing.
     */
    private abstract static class MyWorkflowContext extends AbstractWorkflowContext {
        public MyWorkflowContext(String workflowId, Map<String, @Nullable Object> payload, ProcessingContext processingContext, WorkflowConfiguration<?> workflowConfiguration) {
            super(workflowId, payload, processingContext, workflowConfiguration);
        }
    }
}
