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

package io.axoniq.framework.workflow.configuration;

import io.axoniq.framework.workflow.runtime.api.execution.context.RecoverableWorkflowExceptionPolicy;
import io.axoniq.framework.workflow.dsl.api.EventNameCustomizer;
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowIdProvider;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowStatusChangeListener;
import io.axoniq.framework.workflow.runtime.execution.DefaultEventNameCustomizer;
import io.axoniq.framework.workflow.runtime.execution.MessageWorkflowIdProvider;
import org.axonframework.common.configuration.Configuration;
import org.junit.jupiter.api.*;

import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link WorkflowCustomization}.
 *
 * @author Simon Zambrovski
 */
class WorkflowCustomizationTest {

    private static final String WORKFLOW_NAME = "test-workflow";

    @Test
    void testConstructorWithNullConfiguration() {
        WorkflowCustomization config = new WorkflowCustomization(WORKFLOW_NAME, null);

        assertThat(config.eventNameCustomizer).isInstanceOf(DefaultEventNameCustomizer.class);
        assertThat(config.workflowIdProvider).isInstanceOf(MessageWorkflowIdProvider.class);
    }

    @Test
    void testConstructorWithConfiguration() {
        Configuration configuration = mock(Configuration.class);
        EventNameCustomizer customizer = mock(EventNameCustomizer.class);

        when(configuration.getComponent(eq(EventNameCustomizer.class), any(Supplier.class))).thenReturn(customizer);
        when(configuration.getComponent(eq(WorkflowIdProvider.class),
                                        any(Supplier.class))).thenReturn(new MessageWorkflowIdProvider());

        WorkflowCustomization config = new WorkflowCustomization(WORKFLOW_NAME, configuration);

        assertThat(config.eventNameCustomizer).isSameAs(customizer);
        assertThat(config.workflowIdProvider).isInstanceOf(MessageWorkflowIdProvider.class);
    }

    @Test
    void testCopyConstructor() {
        WorkflowCustomization base = new WorkflowCustomization(WORKFLOW_NAME, null);
        EventNameCustomizer customizer = mock(EventNameCustomizer.class);
        WorkflowIdProvider provider = mock(WorkflowIdProvider.class);
        base.eventNameCustomizer(customizer).workflowIdProvider(provider);

        WorkflowCustomization copy = new WorkflowCustomization(base);

        assertThat(copy.eventNameCustomizer).isSameAs(customizer);
        assertThat(copy.workflowIdProvider).isSameAs(provider);
    }

    @Test
    void testSetters() {
        WorkflowCustomization config = new WorkflowCustomization(WORKFLOW_NAME, null);
        EventNameCustomizer customizer = mock(EventNameCustomizer.class);
        WorkflowIdProvider provider = mock(WorkflowIdProvider.class);

        config.eventNameCustomizer(customizer);
        config.workflowIdProvider(provider);

        assertThat(config.eventNameCustomizer).isSameAs(customizer);
        assertThat(config.workflowIdProvider).isSameAs(provider);
    }

    @Test
    void recoverableExceptionPolicyDefaultsAndCanBeOverriddenPerWorkflow() {
        // given
        WorkflowCustomization config = new WorkflowCustomization(WORKFLOW_NAME, null);
        RecoverableWorkflowExceptionPolicy custom = e -> e instanceof IllegalStateException;

        // when
        var defaultPolicy = config.recoverableExceptionPolicy();
        var copiedPolicy = new WorkflowCustomization(config.recoverableExceptionPolicy(custom))
                .recoverableExceptionPolicy();

        // then
        assertThat(defaultPolicy).isSameAs(RecoverableWorkflowExceptionPolicy.DEFAULT);
        assertThat(copiedPolicy).isSameAs(custom);
    }

    @Test
    void testConstructorThrowsOnNullWorkflowName() {
        assertThatThrownBy(() -> new WorkflowCustomization(null, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("Workflow name must be provided");
    }

    @Test
    void testSettersThrowOnNull() {
        WorkflowCustomization config = new WorkflowCustomization(WORKFLOW_NAME, null);

        assertThatThrownBy(() -> config.eventNameCustomizer(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("Event name customizer must not be null.");

        assertThatThrownBy(() -> config.workflowIdProvider(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("Workflow id provider must not be null.");
    }

    @Test
    void testRegisterWorkflowStatusChangeListener() {
        WorkflowCustomization config = new WorkflowCustomization(WORKFLOW_NAME, null);
        WorkflowStatusChangeListener listener = mock(WorkflowStatusChangeListener.class);

        config.registerWorkflowStatusChangeListener(WorkflowStatus.STARTED, listener);

        assertThat(config.workflowStatusChangeListeners()).containsKey(WorkflowStatus.STARTED);
    }

    @Test
    void testUnregisterWorkflowStatusChangeListener() {
        WorkflowCustomization config = new WorkflowCustomization(WORKFLOW_NAME, null);
        WorkflowStatusChangeListener listener = mock(WorkflowStatusChangeListener.class);

        config.registerWorkflowStatusChangeListener(WorkflowStatus.STARTED, listener);
        assertThat(config.workflowStatusChangeListeners()).containsKey(WorkflowStatus.STARTED);

        config.unregisterWorkflowStatusChangeListener(WorkflowStatus.STARTED, listener);
        assertThat(config.workflowStatusChangeListeners()).doesNotContainKey(WorkflowStatus.STARTED);
    }

    @Test
    void testRegisterWorkflowStatusChangeListenerThrowsOnNull() {
        WorkflowCustomization config = new WorkflowCustomization(WORKFLOW_NAME, null);

        assertThatThrownBy(() -> config.registerWorkflowStatusChangeListener(null,
                                                                             mock(WorkflowStatusChangeListener.class)))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("Workflow status must not be null");

        assertThatThrownBy(() -> config.registerWorkflowStatusChangeListener(WorkflowStatus.STARTED, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("Workflow status change listener must not be null");
    }
}
