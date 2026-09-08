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
package io.axoniq.framework.workflow.runtime.test.utils;

import io.axoniq.framework.workflow.runtime.api.execution.context.ExecutePrimitive;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.framework.workflow.runtime.api.payload.PayloadProcessor;
import org.junit.jupiter.api.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link ManualExecuteStepActionResolver}.
 *
 * @author Simon Zambrovski
 */
class ManualExecuteStepActionResolverTest {

    @Test
    void applyActionReturnsSuppliedActionForStep() {
        ManualExecuteStepActionResolver resolver = new ManualExecuteStepActionResolver();
        PayloadProcessor action = mock(PayloadProcessor.class);
        ExecutePrimitive.ExecuteCommand command = command("charge");

        resolver.applyAction("charge", action);

        PayloadProcessor resolved = resolver.resolve(mock(WorkflowContext.class),
                                                     mock(WorkflowExecution.class),
                                                     command);

        assertThat(resolved).isSameAs(action);
    }

    @Test
    void applyOriginalActionReturnsCommandAction() {
        ManualExecuteStepActionResolver resolver = new ManualExecuteStepActionResolver();
        PayloadProcessor action = mock(PayloadProcessor.class);
        ExecutePrimitive.ExecuteCommand command = command("charge");
        when(command.action()).thenReturn(action);

        resolver.applyOriginalAction("charge");

        PayloadProcessor resolved = resolver.resolve(mock(WorkflowContext.class),
                                                     mock(WorkflowExecution.class),
                                                     command);

        assertThat(resolved).isSameAs(action);
    }

    @Test
    void resolveWrapsActionFactoryFailure() {
        ManualExecuteStepActionResolver resolver = new ManualExecuteStepActionResolver();
        RuntimeException failure = new RuntimeException("missing action");
        ExecutePrimitive.ExecuteCommand command = command("charge");
        when(command.action()).thenThrow(failure);

        resolver.applyOriginalAction("charge");

        assertThatThrownBy(() -> resolver.resolve(mock(WorkflowContext.class),
                                                  mock(WorkflowExecution.class),
                                                  command))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Failed to resolve test action for step charge")
                .hasCause(failure);
    }

    private static ExecutePrimitive.ExecuteCommand command(String stepName) {
        ExecutePrimitive.ExecuteCommand command = mock(ExecutePrimitive.ExecuteCommand.class);
        when(command.stepName()).thenReturn(stepName);
        return command;
    }
}
