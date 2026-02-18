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
 *    https://lp.axoniq.io/axoniq-software-subscription-agreement-terms
 *
 *
 */

package io.axoniq.workflow.runtime.engine.configuration;

import io.axoniq.workflow.runtime.api.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.WorkflowIdProvider;
import io.axoniq.workflow.runtime.engine.impl.DefaultEventNameCustomizer;
import io.axoniq.workflow.runtime.engine.impl.MessageWorkflowIdProvider;
import org.axonframework.common.configuration.Configuration;
import org.junit.jupiter.api.*;

import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link WorkflowModuleConfiguration}.
 */
class WorkflowModuleConfigurationTest {

    private static final String WORKFLOW_NAME = "test-workflow";

    @Test
    void testConstructorWithNullConfiguration() {
        WorkflowModuleConfiguration config = new WorkflowModuleConfiguration(WORKFLOW_NAME, null);

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

        WorkflowModuleConfiguration config = new WorkflowModuleConfiguration(WORKFLOW_NAME, configuration);

        assertThat(config.eventNameCustomizer).isSameAs(customizer);
        assertThat(config.workflowIdProvider).isInstanceOf(MessageWorkflowIdProvider.class);
    }

    @Test
    void testCopyConstructor() {
        WorkflowModuleConfiguration base = new WorkflowModuleConfiguration(WORKFLOW_NAME, null);
        EventNameCustomizer customizer = mock(EventNameCustomizer.class);
        WorkflowIdProvider provider = mock(WorkflowIdProvider.class);
        base.eventNameCustomizer(customizer).workflowIdProvider(provider);

        WorkflowModuleConfiguration copy = new WorkflowModuleConfiguration(base);

        assertThat(copy.eventNameCustomizer).isSameAs(customizer);
        assertThat(copy.workflowIdProvider).isSameAs(provider);
    }

    @Test
    void testSetters() {
        WorkflowModuleConfiguration config = new WorkflowModuleConfiguration(WORKFLOW_NAME, null);
        EventNameCustomizer customizer = mock(EventNameCustomizer.class);
        WorkflowIdProvider provider = mock(WorkflowIdProvider.class);

        config.eventNameCustomizer(customizer);
        config.workflowIdProvider(provider);

        assertThat(config.eventNameCustomizer).isSameAs(customizer);
        assertThat(config.workflowIdProvider).isSameAs(provider);
    }

    @Test
    void testConstructorThrowsOnNullWorkflowName() {
        assertThatThrownBy(() -> new WorkflowModuleConfiguration(null, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("Workflow name must be provided");
    }

    @Test
    void testSettersThrowOnNull() {
        WorkflowModuleConfiguration config = new WorkflowModuleConfiguration(WORKFLOW_NAME, null);

        assertThatThrownBy(() -> config.eventNameCustomizer(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("Event name customizer must not be null.");

        assertThatThrownBy(() -> config.workflowIdProvider(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("Workflow id provider must not be null.");
    }
}
