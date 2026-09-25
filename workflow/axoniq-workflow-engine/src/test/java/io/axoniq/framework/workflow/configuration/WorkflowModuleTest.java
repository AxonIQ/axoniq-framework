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

import io.axoniq.framework.workflow.annotation.Workflow;
import io.axoniq.framework.workflow.annotation.WorkflowCompletedHandler;
import io.axoniq.framework.workflow.dsl.api.EventCondition;
import io.axoniq.framework.workflow.dsl.api.EventConditions;
import io.axoniq.framework.workflow.dsl.api.EventNameCustomizer;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowConfiguration;
import io.axoniq.framework.workflow.dsl.api.WorkflowContext;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowContextFactory;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowDefinition;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecutionFactory;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowIdProvider;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowStatusChangeListener;
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.association.ValueComparisonOperatorRegistry;
import io.axoniq.framework.workflow.runtime.execution.DefaultEventNameCustomizer;
import io.axoniq.framework.workflow.runtime.execution.AbstractWorkflowContext;
import io.axoniq.framework.workflow.runtime.execution.PayloadPropertyWorkflowIdProvider;
import io.axoniq.framework.workflow.runtime.execution.WorkflowConfigurationRegistry;
import io.axoniq.framework.workflow.runtime.util.DefaultTimeoutFutureResolver;
import io.axoniq.framework.workflow.runtime.util.FutureResolver;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.annotation.AnnotatedMessageHandlingMemberDefinition;
import org.axonframework.messaging.core.annotation.HandlerDefinition;
import org.axonframework.messaging.core.annotation.HandlerEnhancerDefinition;
import org.axonframework.messaging.core.annotation.MessageHandlingMember;
import org.axonframework.messaging.core.annotation.MultiHandlerDefinition;
import org.axonframework.messaging.core.annotation.MultiParameterResolverFactory;
import org.axonframework.messaging.core.annotation.ParameterResolverFactory;
import org.axonframework.messaging.core.annotation.UnsupportedHandlerException;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.core.unitofwork.annotation.ProcessingContextParameterResolverFactory;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.junit.jupiter.api.*;
import org.mockito.*;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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
        when(configuration.getComponent(HandlerEnhancerDefinition.class)).thenReturn(new HandlerEnhancerDefinition() {
            @Override
            public <T> MessageHandlingMember<T> wrapHandler(MessageHandlingMember<T> original) {
                return original;
            }
        });
        when(configuration.getComponent(ParameterResolverFactory.class))
                .thenReturn(new WorkflowMethodParameterResolverFactory());
        when(configuration.getComponent(HandlerDefinition.class)).thenReturn(MultiHandlerDefinition.ordered(
                new AnnotatedWorkflowHandlerDefinition(), new AnnotatedMessageHandlingMemberDefinition()
        ));
    }

    @SuppressWarnings("unchecked")
    @Test
    void returnSimpleModule() {
        WorkflowContextFactory<TestWorkflowContext> contextFactory = mock(WorkflowContextFactory.class);
        WorkflowExecutionFactory stateFactory = mock(WorkflowExecutionFactory.class);

        EventCondition startCondition = EventConditions.fromQualifiedName(new QualifiedName("startEvent"));
        WorkflowDefinition<TestWorkflowContext> definition = mock(WorkflowDefinition.class);

        WorkflowModule<TestWorkflowContext> myModule =
                WorkflowModule.defaults(UUID.randomUUID().toString(), TestWorkflowContext.class)
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

    @Test
    @SuppressWarnings("unchecked")
    void autodetectedWorkflowBodyIsInvokedThroughTheRegisteredHandlerEnhancerDefinition() {
        RecordingHandlerEnhancerDefinition enhancer = new RecordingHandlerEnhancerDefinition();
        when(configuration.getComponent(HandlerEnhancerDefinition.class)).thenReturn(enhancer);

        RecordedWorkflow workflow = new RecordedWorkflow();
        WorkflowContextFactory<TestWorkflowContext> ctxFactory = mock(WorkflowContextFactory.class);
        module.workflowContextFactory(c -> ctxFactory)
              .definition(dsl -> dsl.autodetected(c -> workflow));

        module.registerWorkflowDefinitions(configuration);

        ArgumentCaptor<WorkflowConfiguration<TestWorkflowContext>> configCaptor = ArgumentCaptor.forClass(
                WorkflowConfiguration.class);
        verify(registry).register(any(EventCondition.class), configCaptor.capture());
        WorkflowConfiguration<TestWorkflowContext> config = configCaptor.getValue();

        TestWorkflowContext context = workflowContextWith(processingContextWithFutureResolver(), config);

        config.workflowDefinition().accept(context);

        assertThat(workflow.invocations).containsExactly("body");
        assertThat(enhancer.invocations).isNotEmpty();
    }

    @Test
    @SuppressWarnings("unchecked")
    void autodetectedLifecycleHandlerIsInvokedThroughTheRegisteredHandlerEnhancerDefinition() {
        RecordingHandlerEnhancerDefinition enhancer = new RecordingHandlerEnhancerDefinition();
        when(configuration.getComponent(HandlerEnhancerDefinition.class)).thenReturn(enhancer);

        RecordedWorkflow workflow = new RecordedWorkflow();
        WorkflowContextFactory<TestWorkflowContext> ctxFactory = mock(WorkflowContextFactory.class);
        module.workflowContextFactory(c -> ctxFactory)
              .definition(dsl -> dsl.autodetected(c -> workflow));

        module.registerWorkflowDefinitions(configuration);

        ArgumentCaptor<WorkflowConfiguration<TestWorkflowContext>> configCaptor = ArgumentCaptor.forClass(
                WorkflowConfiguration.class);
        verify(registry).register(any(EventCondition.class), configCaptor.capture());
        WorkflowConfiguration<TestWorkflowContext> config = configCaptor.getValue();

        TestWorkflowContext context = mock(TestWorkflowContext.class);
        EventMessage eventMessage = mock(EventMessage.class);
        ProcessingContext processingContext = processingContextWithFutureResolver();

        config.workflowStatusChangeListeners()
              .get(WorkflowStatus.COMPLETED)
              .onWorkflowStatus(WorkflowStatus.COMPLETED, context, eventMessage, processingContext);

        assertThat(workflow.invocations).containsExactly("onCompleted");
        assertThat(enhancer.invocations).isNotEmpty();
    }

    private static ProcessingContext processingContextWithFutureResolver() {
        return StubProcessingContext.withComponents(
                cr -> cr.registerComponent(FutureResolver.class, cfg -> new DefaultTimeoutFutureResolver())
        );
    }

    private static TestWorkflowContext workflowContextWith(
            ProcessingContext processingContext,
            WorkflowConfiguration<?> workflowConfiguration
    ) {
        return new TestWorkflowContext("workflow-id", Map.of(), processingContext, workflowConfiguration);
    }

    @Test
    @SuppressWarnings("unchecked")
    void autodetectedWorkflowBodyWithoutAWorkflowContextParameterIsStillDiscoveredAndInvoked() {
        ContextlessWorkflow workflow = new ContextlessWorkflow();
        WorkflowContextFactory<TestWorkflowContext> ctxFactory = mock(WorkflowContextFactory.class);
        module.workflowContextFactory(c -> ctxFactory)
              .definition(dsl -> dsl.autodetected(c -> workflow));

        module.registerWorkflowDefinitions(configuration);

        ArgumentCaptor<WorkflowConfiguration<TestWorkflowContext>> configCaptor = ArgumentCaptor.forClass(
                WorkflowConfiguration.class);
        verify(registry).register(any(EventCondition.class), configCaptor.capture());
        WorkflowConfiguration<TestWorkflowContext> config = configCaptor.getValue();

        TestWorkflowContext context = workflowContextWith(processingContextWithFutureResolver(), config);

        config.workflowDefinition().accept(context);

        assertThat(workflow.invoked).isTrue();
    }

    @Test
    @SuppressWarnings("unchecked")
    void autodetectedWorkflowBodyResolvesFrameworkNativeProcessingContextParameter() {
        // The workflow-specific factory composes with the framework's own resolvers rather than replacing
        // them, exactly as AutoDetectingWorkflowBuilder's real ParameterResolverFactory component does.
        when(configuration.getComponent(ParameterResolverFactory.class)).thenReturn(
                MultiParameterResolverFactory.ordered(
                        new WorkflowMethodParameterResolverFactory(),
                        new ProcessingContextParameterResolverFactory()
                ));

        ProcessingContextInjectingWorkflow workflow = new ProcessingContextInjectingWorkflow();
        WorkflowContextFactory<TestWorkflowContext> ctxFactory = mock(WorkflowContextFactory.class);
        module.workflowContextFactory(c -> ctxFactory)
              .definition(dsl -> dsl.autodetected(c -> workflow));

        module.registerWorkflowDefinitions(configuration);

        ArgumentCaptor<WorkflowConfiguration<TestWorkflowContext>> configCaptor = ArgumentCaptor.forClass(
                WorkflowConfiguration.class);
        verify(registry).register(any(EventCondition.class), configCaptor.capture());
        WorkflowConfiguration<TestWorkflowContext> config = configCaptor.getValue();

        TestWorkflowContext context = workflowContextWith(processingContextWithFutureResolver(), config);

        config.workflowDefinition().accept(context);

        assertThat(workflow.capturedProcessingContext).isNotNull();
        assertThat(workflow.capturedProcessingContext.containsResource(
                WorkflowMethodParameterResolverFactory.WORKFLOW_CONTEXT_RESOURCE_KEY)).isTrue();
    }

    @Test
    @SuppressWarnings("unchecked")
    void autodetectedWorkflowBodyResolvesDeclaringInstanceAndWrapperTypeParameters() {
        ParameterInjectionWorkflow workflow = new ParameterInjectionWorkflow();
        WorkflowContextFactory<TestWorkflowContext> ctxFactory = mock(WorkflowContextFactory.class);
        module.workflowContextFactory(c -> ctxFactory)
              .definition(dsl -> dsl.autodetected(c -> workflow));

        module.registerWorkflowDefinitions(configuration);

        ArgumentCaptor<WorkflowConfiguration<TestWorkflowContext>> configCaptor = ArgumentCaptor.forClass(
                WorkflowConfiguration.class);
        verify(registry).register(any(EventCondition.class), configCaptor.capture());
        WorkflowConfiguration<TestWorkflowContext> config = configCaptor.getValue();

        TestWorkflowContext context = workflowContextWith(processingContextWithFutureResolver(), config);

        config.workflowDefinition().accept(context);

        assertThat(workflow.capturedInstance).isSameAs(workflow);
        assertThat(workflow.capturedWrapper).isNotNull();
        assertThat(workflow.capturedWrapper.context()).isSameAs(context);
    }

    @Test
    @SuppressWarnings("unchecked")
    void autodetectedWorkflowWithAnUnresolvableParameterFailsAtConfigurationTime() {
        UnresolvableParameterWorkflow workflow = new UnresolvableParameterWorkflow();
        WorkflowContextFactory<TestWorkflowContext> ctxFactory = mock(WorkflowContextFactory.class);
        module.workflowContextFactory(c -> ctxFactory)
              .definition(dsl -> dsl.autodetected(c -> workflow));

        assertThatThrownBy(() -> module.registerWorkflowDefinitions(configuration))
                .isInstanceOf(UnsupportedHandlerException.class)
                .hasMessageContaining("run");
    }

    static class TestWorkflowContext extends AbstractWorkflowContext {

        TestWorkflowContext(
                String workflowId,
                Map<String, Object> payload,
                ProcessingContext processingContext,
                WorkflowConfiguration<?> workflowConfiguration
        ) {
            super(workflowId, payload, processingContext, workflowConfiguration);
        }

    }

    public static class TestAutodetectedWorkflow {

        @Workflow(workflowName = "autodetectedWorkflow", startOnEventName = "java.lang.String", idProperty = "id")
        void myWorkflow(TestWorkflowContext context) {
            // some workflow logic
        }
    }

    public static class ContextlessWorkflow {

        boolean invoked = false;

        @Workflow(workflowName = "contextlessWorkflow", startOnEventName = "java.lang.String", idProperty = "id")
        public void run() {
            invoked = true;
        }
    }

    public static class UnresolvableParameterWorkflow {

        @Workflow(workflowName = "unresolvableParameterWorkflow", startOnEventName = "java.lang.String", idProperty = "id")
        public void run(SomeUnrelatedType notResolvable) {
        }
    }

    public static class SomeUnrelatedType {

    }

    public static class ProcessingContextInjectingWorkflow {

        volatile ProcessingContext capturedProcessingContext;

        @Workflow(workflowName = "processingContextWorkflow", startOnEventName = "java.lang.String", idProperty = "id")
        public void run(TestWorkflowContext context, ProcessingContext processingContext) {
            this.capturedProcessingContext = processingContext;
        }
    }

    public static class ParameterInjectionWorkflow {

        volatile ParameterInjectionWorkflow capturedInstance;
        volatile ContextWrapper capturedWrapper;

        @Workflow(workflowName = "parameterInjectionWorkflow", startOnEventName = "java.lang.String", idProperty = "id")
        public void run(TestWorkflowContext context, ParameterInjectionWorkflow self, ContextWrapper wrapper) {
            this.capturedInstance = self;
            this.capturedWrapper = wrapper;
        }
    }

    public record ContextWrapper(WorkflowContext context) {

    }

    public static class RecordedWorkflow {

        final List<String> invocations = new CopyOnWriteArrayList<>();

        @Workflow(workflowName = "recordedWorkflow", startOnEventName = "java.lang.String", idProperty = "id")
        public void run(TestWorkflowContext context) {
            invocations.add("body");
        }

        @WorkflowCompletedHandler
        public void onCompleted(WorkflowStatus status, TestWorkflowContext context) {
            invocations.add("onCompleted");
        }
    }

    /**
     * A {@link HandlerEnhancerDefinition} recording every invocation of the member it wraps, proving the wrapped
     * member is what actually gets invoked rather than the raw, unenhanced one.
     */
    static class RecordingHandlerEnhancerDefinition implements HandlerEnhancerDefinition {

        final List<String> invocations = new CopyOnWriteArrayList<>();

        @Override
        public <T> MessageHandlingMember<T> wrapHandler(MessageHandlingMember<T> original) {
            return new RecordingMember<>(original, invocations);
        }
    }

    record RecordingMember<T>(
            MessageHandlingMember<T> delegate,
            List<String> invocations
    ) implements MessageHandlingMember<T> {

        @Override
        public Class<?> payloadType() {
            return delegate.payloadType();
        }

        @Override
        public boolean canHandle(Message message, ProcessingContext context) {
            return delegate.canHandle(message, context);
        }

        @Override
        public boolean canHandleMessageType(Class<? extends Message> messageType) {
            return delegate.canHandleMessageType(messageType);
        }

        @Override
        public MessageStream<?> handle(Message message, ProcessingContext context, T target) {
            invocations.add(delegate.signature());
            return delegate.handle(message, context, target);
        }

        @Override
        public <HT> Optional<HT> unwrap(Class<HT> handlerType) {
            return delegate.unwrap(handlerType);
        }
    }
}
