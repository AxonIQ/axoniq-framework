package io.axoniq.workflow.runtime.engine.configuration;

import io.axoniq.workflow.runtime.api.EventCondition;
import io.axoniq.workflow.runtime.api.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.WorkflowConfiguration;
import io.axoniq.workflow.runtime.api.WorkflowContext;
import io.axoniq.workflow.runtime.api.WorkflowContextFactory;
import io.axoniq.workflow.runtime.api.WorkflowDefinition;
import io.axoniq.workflow.runtime.api.WorkflowDefinitionRegistry;
import io.axoniq.workflow.runtime.api.WorkflowIdProvider;
import io.axoniq.workflow.runtime.engine.execution.WorkflowStateFactory;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.messaging.core.QualifiedName;
import org.junit.jupiter.api.*;
import org.mockito.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class SimpleWorkflowModuleTest {

    private SimpleWorkflowModule<TestWorkflowContext> module;
    private Configuration configuration;
    private WorkflowDefinitionRegistry<?> registry;

    @BeforeEach
    void setUp() {
        module = new SimpleWorkflowModule<>(TestWorkflowContext.class);
        configuration = mock(Configuration.class);
        registry = mock(WorkflowDefinitionRegistry.class);
        when(configuration.getComponent(WorkflowDefinitionRegistry.class)).thenReturn(registry);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testDeclarativeWorkflowDefinition() {
        EventCondition startCondition = new EventCondition(new QualifiedName("startEvent"), e -> true);
        WorkflowDefinition<TestWorkflowContext> definition = mock(WorkflowDefinition.class);

        WorkflowContextFactory<TestWorkflowContext> contextFactory = mock(WorkflowContextFactory.class);
        WorkflowStateFactory stateFactory = mock(WorkflowStateFactory.class);

        module.workflowContextFactory(c -> contextFactory)
              .workflowStateFactory(c -> stateFactory)
              .definitions(dsl -> dsl.declarative("testWorkflow")
                                     .on(c -> startCondition)
                                     .workflowDefinition(c -> definition)
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
        EventCondition startCondition = new EventCondition(new QualifiedName("startEvent"), e -> true);
        WorkflowDefinition<TestWorkflowContext> definition = mock(WorkflowDefinition.class);
        WorkflowContextFactory<TestWorkflowContext> contextFactory = mock(WorkflowContextFactory.class);
        WorkflowStateFactory stateFactory = mock(WorkflowStateFactory.class);
        EventNameCustomizer customizer = mock(EventNameCustomizer.class);
        WorkflowIdProvider idProvider = mock(WorkflowIdProvider.class);

        module.workflowContextFactory(c -> contextFactory)
              .workflowStateFactory(c -> stateFactory)
              .definitions(dsl -> dsl.declarative("testWorkflow")
                                     .on(c -> startCondition)
                                     .workflowDefinition(c -> definition)
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
        EventCondition startCondition1 = new EventCondition(new QualifiedName("startEvent1"), e -> true);
        EventCondition startCondition2 = new EventCondition(new QualifiedName("startEvent2"), e -> true);
        WorkflowDefinition<TestWorkflowContext> definition = mock(WorkflowDefinition.class);

        module.workflowContextFactory(c -> mock(WorkflowContextFactory.class))
              .workflowStateFactory(c -> mock(WorkflowStateFactory.class))
              .definitions(dsl -> {
                  ((WorkflowModule.WorkflowDefinitionPhase.DetectionPhase<TestWorkflowContext>) dsl)
                          .declarative("workflow 1")
                          .on(c -> startCondition1)
                          .workflowDefinition(c -> definition)
                          .notCustomized();
                  ((WorkflowModule.WorkflowDefinitionPhase.DetectionPhase<TestWorkflowContext>) dsl)
                          .declarative("workflow 2")
                          .on(c -> startCondition2)
                          .workflowDefinition(c -> definition)
                          .notCustomized();
              });

        module.registerWorkflowDefinitions(configuration);

        verify(registry).register(eq(startCondition1), any());
        verify(registry).register(eq(startCondition2), any());
    }

    private interface TestWorkflowContext extends WorkflowContext {

    }
}
