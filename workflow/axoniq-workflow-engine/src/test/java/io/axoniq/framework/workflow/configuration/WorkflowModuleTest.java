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

import io.axoniq.framework.workflow.dsl.api.EventCondition;
import io.axoniq.framework.workflow.dsl.api.EventConditions;
import io.axoniq.framework.workflow.dsl.api.EventNameCustomizer;
import io.axoniq.framework.workflow.dsl.api.WorkflowContext;
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.api.annotation.Workflow;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowConfiguration;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowContextFactory;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowDefinition;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecutionFactory;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowIdProvider;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowStatusChangeListener;
import io.axoniq.framework.workflow.runtime.association.ValueComparisonOperatorRegistry;
import io.axoniq.framework.workflow.runtime.execution.DefaultEventNameCustomizer;
import io.axoniq.framework.workflow.runtime.execution.PayloadPropertyWorkflowIdProvider;
import io.axoniq.framework.workflow.runtime.execution.WorkflowConfigurationRegistry;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.QualifiedName;
import org.junit.jupiter.api.*;
import org.mockito.*;

import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Test for {@link WorkflowModule} and {@link SimpleWorkflowModule}.
 *
 * @author Simon Zambrovski
 */
class WorkflowModuleTest {

    private SimpleWorkflowModule<TestWorkflowContext> module;
    private Configuration configuration;
    private WorkflowConfigurationRegistry<?> registry;
    private MessageTypeResolver messageTypeResolver;

    @BeforeEach
    void setUp() {
        module = new SimpleWorkflowModule<>(UUID.randomUUID().toString(), TestWorkflowContext.class);
        configuration = mock(Configuration.class);
        registry = mock(WorkflowConfigurationRegistry.class);
        messageTypeResolver = mock(MessageTypeResolver.class);
        when(configuration.getComponent(WorkflowConfigurationRegistry.class)).thenReturn(registry);
        when(configuration.getComponent(MessageTypeResolver.class)).thenReturn(messageTypeResolver);
        when(configuration.getComponent(eq(ValueComparisonOperatorRegistry.class), any(Supplier.class)))
                .thenAnswer(invocation -> {
                    Supplier<ValueComparisonOperatorRegistry> defaultSupplier = invocation.getArgument(1);
                    return defaultSupplier.get();
                });
    }

    @SuppressWarnings("unchecked")
    @Test
    void returnSimpleModule() {
        WorkflowContextFactory<TestWorkflowContext> contextFactory = mock(WorkflowContextFactory.class);
        WorkflowExecutionFactory stateFactory = mock(WorkflowExecutionFactory.class);

        EventCondition startCondition = EventConditions.fromQualifiedName(new QualifiedName("startEvent"));
        WorkflowDefinition<TestWorkflowContext> definition = mock(WorkflowDefinition.class);

        var myModule = WorkflowModule.defaults(UUID.randomUUID().toString(), TestWorkflowContext.class)
                                     .workflowContextFactory(c -> contextFactory)
                                     .definition(d -> d.declarative(c -> definition).workflowName("name")
                                                       .on(c -> startCondition).notCustomized());
        assertThat(myModule).isNotNull();
        assertThat(myModule).isInstanceOf(SimpleWorkflowModule.class);
        assertThat(myModule.getWorkflowContextType()).isEqualTo(TestWorkflowContext.class);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testDeclarativeWorkflowDefinition() {
        EventCondition startCondition = EventConditions.fromQualifiedName(new QualifiedName("startEvent"));
        WorkflowDefinition<TestWorkflowContext> definition = mock(WorkflowDefinition.class);

        WorkflowContextFactory<TestWorkflowContext> contextFactory = mock(WorkflowContextFactory.class);
        WorkflowExecutionFactory stateFactory = mock(WorkflowExecutionFactory.class);

        module.workflowContextFactory(c -> contextFactory).definition(dsl -> dsl.declarative(c -> definition)
                                                                                .workflowName("testWorkflow")
                                                                                .on(c -> startCondition)
                                                                                .notCustomized());

        module.registerWorkflowDefinitions(configuration);

        ArgumentCaptor<WorkflowConfiguration<TestWorkflowContext>> configCaptor = ArgumentCaptor.forClass(
                WorkflowConfiguration.class);
        verify(registry).register(any(EventCondition.class), configCaptor.capture());

        WorkflowConfiguration<TestWorkflowContext> config = configCaptor.getValue();
        assertThat(config.workflowName()).isEqualTo("testWorkflow");
        assertThat(config.workflowDefinition()).isSameAs(definition);
        assertThat(config.workflowContextFactory()).isSameAs(contextFactory);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testCustomizedWorkflowDefinition() {
        EventCondition startCondition = EventConditions.fromQualifiedName(new QualifiedName("startEvent"));
        WorkflowDefinition<TestWorkflowContext> definition = mock(WorkflowDefinition.class);
        WorkflowContextFactory<TestWorkflowContext> contextFactory = mock(WorkflowContextFactory.class);
        EventNameCustomizer customizer = mock(EventNameCustomizer.class);
        WorkflowIdProvider idProvider = mock(WorkflowIdProvider.class);

        module.workflowContextFactory(c -> contextFactory)
              .definition(dsl -> dsl
                      .declarative(c -> definition)
                      .workflowName("testWorkflow")
                      .on(c -> startCondition)
                      .customized((c, config) -> config.eventNameCustomizer(
                              customizer).workflowIdProvider(
                              idProvider)));

        module.registerWorkflowDefinitions(configuration);

        @SuppressWarnings("unchecked") ArgumentCaptor<WorkflowConfiguration<TestWorkflowContext>> configCaptor = ArgumentCaptor.forClass(
                WorkflowConfiguration.class);
        verify(registry).register(any(EventCondition.class), configCaptor.capture());

        WorkflowConfiguration<TestWorkflowContext> config = configCaptor.getValue();
        assertThat(config.eventNameCustomizer()).isSameAs(customizer);
        assertThat(config.workflowIdProvider()).isSameAs(idProvider);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testAutodetectedWorkflowDefinition() {
        QualifiedName startEventName = new QualifiedName("startEvent");
        when(messageTypeResolver.resolve(String.class)).thenReturn(Optional.of(new MessageType(startEventName)));
        WorkflowContextFactory<TestWorkflowContext> ctxFactory = mock(WorkflowContextFactory.class);
        module.workflowContextFactory(c -> ctxFactory)
              .definition(dsl -> dsl
                      .autodetected(c -> new TestAutodetectedWorkflow())
              );

        module.registerWorkflowDefinitions(configuration);

        ArgumentCaptor<WorkflowConfiguration<TestWorkflowContext>> configCaptor = ArgumentCaptor.forClass(
                WorkflowConfiguration.class);
        verify(registry).register(any(EventCondition.class), configCaptor.capture());

        WorkflowConfiguration<TestWorkflowContext> config = configCaptor.getValue();
        assertThat(config.workflowName()).isEqualTo("autodetectedWorkflow");
        assertThat(config.eventNameCustomizer()).isInstanceOf(DefaultEventNameCustomizer.class);
        assertThat(config.workflowIdProvider()).isInstanceOf(PayloadPropertyWorkflowIdProvider.class);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testWorkflowStatusChangeListenerRegistration() {
        EventCondition startCondition = EventConditions.fromQualifiedName(new QualifiedName("startEvent"));
        WorkflowDefinition<TestWorkflowContext> definition = mock(WorkflowDefinition.class);
        WorkflowStatusChangeListener listener = mock(WorkflowStatusChangeListener.class);
        WorkflowContextFactory<TestWorkflowContext> contextFactory = mock(WorkflowContextFactory.class);
        module.workflowContextFactory(c -> contextFactory)
              .definition(dsl -> dsl
                      .declarative(c -> definition)
                      .workflowName("testWorkflow")
                      .on(c -> startCondition)
                      .customized(
                              (c, config) -> config.registerWorkflowStatusChangeListener(
                                      WorkflowStatus.STARTED,
                                      listener)));

        module.registerWorkflowDefinitions(configuration);

        ArgumentCaptor<WorkflowConfiguration<TestWorkflowContext>> configCaptor = ArgumentCaptor.forClass(
                WorkflowConfiguration.class);
        verify(registry).register(any(EventCondition.class), configCaptor.capture());

        WorkflowConfiguration<TestWorkflowContext> config = configCaptor.getValue();
        assertThat(config.workflowStatusChangeListeners()).containsKey(WorkflowStatus.STARTED);
        assertThat(config.workflowStatusChangeListeners().get(WorkflowStatus.STARTED)).isInstanceOf(
                CompositeWorkflowStatusChangeListener.class);
    }

    interface TestWorkflowContext extends WorkflowContext {

    }

    public static class TestAutodetectedWorkflow {

        @Workflow(workflowName = "autodetectedWorkflow", startOnEventName = "java.lang.String", idProperty = "id")
        void myWorkflow(TestWorkflowContext context) {
            // some workflow logic
        }
    }
}
