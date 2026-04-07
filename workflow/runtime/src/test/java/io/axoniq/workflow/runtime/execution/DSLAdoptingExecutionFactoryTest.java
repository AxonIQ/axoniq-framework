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
package io.axoniq.workflow.runtime.execution;

import io.axoniq.workflow.runtime.api.execution.context.WorkflowConfiguration;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test for {@link DSLAdoptingExecutionFactory}.
 */
class DSLAdoptingExecutionFactoryTest {

    @Test
    void testConstructorWithValidType() {
        assertDoesNotThrow(() -> new DSLAdoptingExecutionFactory<>(MyDSLContext.class));
    }

    @Test
    void testConstructorWithInvalidType() {
        assertThrows(IllegalArgumentException.class, () -> new DSLAdoptingExecutionFactory<>(WorkflowContext.class));
    }

    @Test
    void testCreateWithSupportedContext() {
        var factory = new DSLAdoptingExecutionFactory<>(MyDSLContext.class);
        var execution = mock(WorkflowExecution.class);
        var context = mock(MyDSLContext.class);
        when(context.execution()).thenReturn(execution);

        var result = factory.create(context);

        assertSame(execution, result);
    }

    @Test
    void testCreateWithUnsupportedContext() {
        var factory = new DSLAdoptingExecutionFactory<>(MyDSLContext.class);
        var context = mock(WorkflowContext.class);

        assertThrows(IllegalStateException.class, () -> factory.create(context));
    }

    /**
     * Helper class for testing.
     */
    private abstract static class MyDSLContext extends AbstractDSLWorkflowContext {
        public MyDSLContext(String workflowId, Map<String, Object> payload, ProcessingContext processingContext, WorkflowConfiguration<?> workflowConfiguration) {
            super(workflowId, payload, processingContext, workflowConfiguration);
        }
    }
}
