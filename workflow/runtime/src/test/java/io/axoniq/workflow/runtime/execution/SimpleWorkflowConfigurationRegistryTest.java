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

import io.axoniq.workflow.runtime.api.execution.context.EventCondition;
import io.axoniq.workflow.runtime.api.execution.context.EventConditions;
import io.axoniq.workflow.runtime.api.execution.context.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowConfiguration;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContextFactory;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowDefinition;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecutionFactory;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowIdProvider;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowStatusChangeListener;
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.workflow.runtime.execution.WorkflowConfigurationRegistry.PredicatedWorkflowConfiguration;
import jakarta.annotation.Nonnull;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.*;
import org.mockito.*;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiPredicate;

import static io.axoniq.workflow.runtime.execution.DefaultEventNameCustomizer.Builder.defaults;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link SimpleWorkflowConfigurationRegistry}.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
class SimpleWorkflowConfigurationRegistryTest {

    private SimpleWorkflowConfigurationRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new SimpleWorkflowConfigurationRegistry();
    }

    @Test
    void testRegisterAndGetConfigurations() {
        QualifiedName eventName = new QualifiedName("com.example.MyEvent");
        BiPredicate<EventMessage, ProcessingContext> predicate = (eventMessage, pc) -> true;
        EventCondition eventCondition = EventConditions.fromQualifiedName(eventName, predicate);

        WorkflowConfiguration<?> workflowConfiguration = new StubWorkflowConfiguration();

        registry.register(eventCondition, workflowConfiguration);

        Set<QualifiedName> supportedEvents = registry.supportedEvents();
        assertThat(supportedEvents).contains(eventName);
        assertThat(supportedEvents).hasSize(1);

        List<PredicatedWorkflowConfiguration> configs = registry.getWorkflowsConfigurations(eventName);
        assertThat(configs).hasSize(1);
        assertThat(configs.getFirst().predicate()).isEqualTo(predicate);
        assertThat(configs.getFirst().configuration()).isEqualTo(workflowConfiguration);
    }

    @Test
    void testRegisterMultipleConfigurationsForSameEvent() {
        QualifiedName eventName = new QualifiedName("com.example.MyEvent");

        BiPredicate<EventMessage, ProcessingContext> pred1 = (msg, pc) -> true;
        EventCondition cond1 = EventConditions.fromQualifiedName(eventName, pred1);
        WorkflowConfiguration<?> conf1 = new StubWorkflowConfiguration();

        BiPredicate<EventMessage, ProcessingContext> pred2 = (msg, pc) -> false;
        EventCondition cond2 = EventConditions.fromQualifiedName(eventName, pred2);
        WorkflowConfiguration<?> conf2 = new StubWorkflowConfiguration();

        registry.register(cond1, conf1);
        registry.register(cond2, conf2);

        List<PredicatedWorkflowConfiguration> configs = registry.getWorkflowsConfigurations(eventName);
        assertThat(configs).hasSize(2);

        assertThat(configs).anyMatch(c -> c.predicate().equals(pred1) && c.configuration().equals(conf1));
        assertThat(configs).anyMatch(c -> c.predicate().equals(pred2) && c.configuration().equals(conf2));
    }

    @Test
    void testSupportedEvents() {
        QualifiedName event1 = new QualifiedName("Event1");
        QualifiedName event2 = new QualifiedName("Event2");

        registry.register(event1, new StubWorkflowConfiguration());
        registry.register(event2, new StubWorkflowConfiguration());

        Set<QualifiedName> supportedEvents = registry.supportedEvents();
        assertThat(supportedEvents).hasSize(2);
        assertThat(supportedEvents).contains(event1);
        assertThat(supportedEvents).contains(event2);
    }

    @Test
    void testGetWorkflowsConfigurationsReturnsEmptyListForUnknownEvent() {
        List<PredicatedWorkflowConfiguration> configs = registry.getWorkflowsConfigurations(new QualifiedName("Unknown"));
        assertThat(configs).isNotNull();
        assertThat(configs).isEmpty();
    }

    @Test
    void testDescribeTo() {
        QualifiedName eventName = new QualifiedName("com.example.MyEvent");
        WorkflowDefinition<WorkflowContext> definition = mock(WorkflowDefinition.class);
        WorkflowConfiguration<?> config = new StubWorkflowConfiguration(definition);

        registry.register(eventName, config);

        ComponentDescriptor descriptor = mock(ComponentDescriptor.class);
        registry.describeTo(descriptor);

        ArgumentCaptor<List> listCaptor = ArgumentCaptor.forClass(List.class);
        verify(descriptor).describeProperty(eq("workflowDefinitions"), listCaptor.capture());

        List<?> descriptors = listCaptor.getValue();
        assertThat(descriptors).hasSize(1);
        Object descObj = descriptors.get(0);

        // The descriptor is an internal record WorkflowDefinitionDescriptor, which is DescribableComponent
        assertThat(descObj).isInstanceOf(org.axonframework.common.infra.DescribableComponent.class);

        ComponentDescriptor subDescriptor = mock(ComponentDescriptor.class);
        ((org.axonframework.common.infra.DescribableComponent) descObj).describeTo(subDescriptor);

        verify(subDescriptor).describeProperty(eq(eventName.toString()), any(List.class));
    }

    @Test
    void testRegisterWithNullInputs() {
        WorkflowConfiguration<?> workflowConfiguration = new StubWorkflowConfiguration();
        assertThatThrownBy(() -> registry.register((EventCondition) null, workflowConfiguration))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> registry.register(EventConditions.never(), null))
                .isInstanceOf(NullPointerException.class);
    }

    private static class StubWorkflowConfiguration implements WorkflowConfiguration<WorkflowContext> {

        private final WorkflowDefinition<WorkflowContext> definition;

        public StubWorkflowConfiguration() {
            this(null);
        }

        public StubWorkflowConfiguration(WorkflowDefinition<WorkflowContext> definition) {
            this.definition = definition;
        }

        @Override
        public Class<WorkflowContext> getWorkflowContextType() {
            return WorkflowContext.class;
        }

        @Nonnull
        @Override
        public WorkflowDefinition<WorkflowContext> workflowDefinition() {
            return definition;
        }

        @Nonnull
        @Override
        public WorkflowContextFactory<WorkflowContext> workflowContextFactory() {
            return new WorkflowContextFactory<>() {
                @Override
                public @NonNull WorkflowContext createContext(@NonNull Map<String, Object> initialPayload,
                                                              @NonNull String workflowId,
                                                              @NonNull ProcessingContext processingContext,
                                                              @NonNull WorkflowConfiguration<?> workflowConfiguration) {
                    throw new UnsupportedOperationException("Stub factory can't create contexts");
                }
            };
        }

        @Nonnull
        @Override
        public WorkflowExecutionFactory workflowExecutionFactory() {
            return new DSLAdoptingExecutionFactory<>(getWorkflowContextType());
        }

        @Override
        public @NonNull WorkflowIdProvider workflowIdProvider() {
            return new MessageWorkflowIdProvider();
        }

        @Override
        public @NonNull String workflowName() {
            return WorkflowConfiguration.super.workflowName();
        }

        @Override
        public @NonNull EventNameCustomizer eventNameCustomizer() {
            return defaults();
        }

        @Override
        public @NonNull Map<WorkflowStatus, WorkflowStatusChangeListener> workflowStatusChangeListeners() {
            return WorkflowConfiguration.super.workflowStatusChangeListeners();
        }
    }
}
