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

import org.jspecify.annotations.Nullable;

import io.axoniq.workflow.runtime.api.execution.context.WorkflowConfiguration;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.junit.jupiter.api.*;

import java.util.Map;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Test for {@link DSLAdoptingExecutionFactory}.
 */
class DSLAdoptingExecutionFactoryTest {

    @Test
    void testConstructorWithValidType() {
        assertThatCode(() -> new DSLAdoptingExecutionFactory<>(MyDSLContext.class)).doesNotThrowAnyException();
    }

    @Test
    void testConstructorWithInvalidType() {
        assertThatThrownBy(() -> new DSLAdoptingExecutionFactory<>(WorkflowContext.class))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testCreateWithSupportedContext() {
        var factory = new DSLAdoptingExecutionFactory<>(MyDSLContext.class);
        var execution = mock(WorkflowExecution.class);
        var context = mock(MyDSLContext.class);
        when(context.execution()).thenReturn(execution);

        var result = factory.create(context);

        assertThat(result).isSameAs(execution);
    }

    @Test
    void testCreateWithUnsupportedContext() {
        var factory = new DSLAdoptingExecutionFactory<>(MyDSLContext.class);
        var context = mock(WorkflowContext.class);

        assertThatThrownBy(() -> factory.create(context))
                .isInstanceOf(IllegalStateException.class);
    }

    /**
     * Helper class for testing.
     */
    private abstract static class MyDSLContext extends AbstractDSLWorkflowContext {
        public MyDSLContext(String workflowId, Map<String, @Nullable Object> payload, ProcessingContext processingContext, WorkflowConfiguration<?> workflowConfiguration) {
            super(workflowId, payload, processingContext, workflowConfiguration);
        }
    }
}
