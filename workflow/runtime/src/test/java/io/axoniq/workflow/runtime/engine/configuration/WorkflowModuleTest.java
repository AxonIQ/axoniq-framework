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

import io.axoniq.workflow.runtime.api.EventCondition;
import io.axoniq.workflow.runtime.api.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.Workflow;
import io.axoniq.workflow.runtime.api.WorkflowConfiguration;
import io.axoniq.workflow.runtime.api.WorkflowContext;
import io.axoniq.workflow.runtime.api.WorkflowContextFactory;
import io.axoniq.workflow.runtime.api.WorkflowDefinition;
import io.axoniq.workflow.runtime.api.WorkflowConfigurationRegistry;
import io.axoniq.workflow.runtime.api.WorkflowIdProvider;
import io.axoniq.workflow.runtime.api.WorkflowStatusChangeListener;
import io.axoniq.workflow.runtime.engine.execution.EventConditions;
import io.axoniq.workflow.runtime.engine.execution.WorkflowStateFactory;
import io.axoniq.workflow.runtime.engine.execution.WorkflowStatus;
import io.axoniq.workflow.runtime.engine.impl.DefaultEventNameCustomizer;
import io.axoniq.workflow.runtime.engine.impl.PayloadPropertyWorkflowIdProvider;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.QualifiedName;
import org.junit.jupiter.api.*;
import org.mockito.*;

import java.util.Optional;

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
    private Converter converter;

    @BeforeEach
    void setUp() {
        module = new SimpleWorkflowModule<>(TestWorkflowContext.class);
        configuration = mock(Configuration.class);
        registry = mock(WorkflowConfigurationRegistry.class);
        messageTypeResolver = mock(MessageTypeResolver.class);
        converter = mock(Converter.class);
        when(configuration.getComponent(WorkflowConfigurationRegistry.class)).thenReturn(registry);
        when(configuration.getComponent(MessageTypeResolver.class)).thenReturn(messageTypeResolver);
        when(configuration.getComponent(Converter.class)).thenReturn(converter);
    }

    @SuppressWarnings("unchecked")
    @Test
    void returnSimpleModule() {
        WorkflowContextFactory<TestWorkflowContext> contextFactory = mock(WorkflowContextFactory.class);
        WorkflowStateFactory stateFactory = mock(WorkflowStateFactory.class);

        EventCondition startCondition = EventConditions.fromQualifiedName(new QualifiedName("startEvent"));
        WorkflowDefinition<TestWorkflowContext> definition = mock(WorkflowDefinition.class);

        var myModule = WorkflowModule.usingContext(TestWorkflowContext.class)
                                     .workflowContextFactory(c -> contextFactory)
                                     .workflowStateFactory(c -> stateFactory)
                                     .definitions(d -> d.declarative(
                                                                c -> definition
                                                        ).workflowName("name")
                                                        .on(c -> startCondition)
                                                        .notCustomized()
                                     );
        assertThat(myModule).isNotNull();
        assertThat(myModule).isInstanceOf(SimpleWorkflowModule.class);
        assertThat(myModule.getContextType()).isEqualTo(TestWorkflowContext.class);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testDeclarativeWorkflowDefinition() {
        EventCondition startCondition = EventConditions.fromQualifiedName(new QualifiedName("startEvent"));
        WorkflowDefinition<TestWorkflowContext> definition = mock(WorkflowDefinition.class);

        WorkflowContextFactory<TestWorkflowContext> contextFactory = mock(WorkflowContextFactory.class);
        WorkflowStateFactory stateFactory = mock(WorkflowStateFactory.class);

        module.workflowContextFactory(c -> contextFactory)
              .workflowStateFactory(c -> stateFactory)
              .definitions(dsl -> dsl.declarative(c -> definition)
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
        assertThat(config.workflowStateFactory()).isSameAs(stateFactory);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testCustomizedWorkflowDefinition() {
        EventCondition startCondition = EventConditions.fromQualifiedName(new QualifiedName("startEvent"));
        WorkflowDefinition<TestWorkflowContext> definition = mock(WorkflowDefinition.class);
        WorkflowContextFactory<TestWorkflowContext> contextFactory = mock(WorkflowContextFactory.class);
        WorkflowStateFactory stateFactory = mock(WorkflowStateFactory.class);
        EventNameCustomizer customizer = mock(EventNameCustomizer.class);
        WorkflowIdProvider idProvider = mock(WorkflowIdProvider.class);

        module.workflowContextFactory(c -> contextFactory)
              .workflowStateFactory(c -> stateFactory)
              .definitions(dsl -> dsl.declarative(c -> definition)
                                     .workflowName("testWorkflow")
                                     .on(c -> startCondition)
                                     .customized((c, config) -> config.eventNameCustomizer(customizer)
                                                                      .workflowIdProvider(idProvider)));

        module.registerWorkflowDefinitions(configuration);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<WorkflowConfiguration<TestWorkflowContext>> configCaptor = ArgumentCaptor.forClass(
                WorkflowConfiguration.class);
        verify(registry).register(any(EventCondition.class), configCaptor.capture());

        WorkflowConfiguration<TestWorkflowContext> config = configCaptor.getValue();
        assertThat(config.eventNameCustomizer()).isSameAs(customizer);
        assertThat(config.workflowIdProvider()).isSameAs(idProvider);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testMultipleWorkflows() {
        EventCondition startCondition1 = EventConditions.fromQualifiedName(new QualifiedName("startEvent1"));
        EventCondition startCondition2 = EventConditions.fromQualifiedName(new QualifiedName("startEvent2"));
        WorkflowDefinition<TestWorkflowContext> definition = mock(WorkflowDefinition.class);

        module.workflowContextFactory(c -> mock(WorkflowContextFactory.class))
              .workflowStateFactory(c -> mock(WorkflowStateFactory.class))
              .definitions(dsl ->
                                   ((WorkflowModule.WorkflowDefinitionPhase.DetectionPhase<TestWorkflowContext>) dsl)
                                           .declarative(c -> definition)
                                           .workflowName("workflow 1")
                                           .on(c -> startCondition1)
                                           .notCustomized()
                                           .declarative(c -> definition)
                                           .workflowName("workflow 2")
                                           .on(c -> startCondition2)
                                           .notCustomized()
              );

        module.registerWorkflowDefinitions(configuration);

        verify(registry).register(eq(startCondition1), any());
        verify(registry).register(eq(startCondition2), any());
    }

    @Test
    @SuppressWarnings("unchecked")
    void testAutodetectedWorkflowDefinition() {
        QualifiedName startEventName = new QualifiedName("startEvent");
        when(messageTypeResolver.resolve(String.class)).thenReturn(Optional.of(new MessageType(startEventName)));

        module.workflowContextFactory(c -> mock(WorkflowContextFactory.class))
              .workflowStateFactory(c -> mock(WorkflowStateFactory.class))
              .definitions(dsl ->
                                   ((WorkflowModule.WorkflowDefinitionPhase.DetectionPhase<TestWorkflowContext>) dsl)
                                           .autodetected(
                                                   c -> new TestAutodetectedWorkflow(),
                                                   TestWorkflowContext.class)
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

        module.workflowContextFactory(c -> mock(WorkflowContextFactory.class))
              .workflowStateFactory(c -> mock(WorkflowStateFactory.class))
              .definitions(dsl -> ((WorkflowModule.WorkflowDefinitionPhase.DetectionPhase<TestWorkflowContext>) dsl)
                      .declarative(c -> definition)
                      .workflowName("testWorkflow")
                      .on(c -> startCondition)
                      .customized((c, config) -> config.registerWorkflowStatusChangeListener(
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

        @Workflow(workflowName = "autodetectedWorkflow", startOn = "java.lang.String", idProperty = "id")
        public void myWorkflow(TestWorkflowContext context) {
            // some workflow logic
        }
    }
}
