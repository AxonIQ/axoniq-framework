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

import io.axoniq.framework.workflow.runtime.api.execution.context.EventCondition;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowContextFactory;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowDefinition;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowIdProvider;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowStatusChangeListener;
import io.axoniq.framework.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.execution.DefaultEventNameCustomizer;
import io.axoniq.framework.workflow.runtime.util.FutureResolver;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.configuration.ComponentBuilder;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.VersionedType;
import org.axonframework.messaging.core.annotation.AnnotatedHandlerInspector;
import org.axonframework.messaging.core.annotation.HandlerDefinition;
import org.axonframework.messaging.core.annotation.HandlerEnhancerDefinition;
import org.axonframework.messaging.core.annotation.MessageHandlingMember;
import org.axonframework.messaging.core.annotation.MultiHandlerDefinition;
import org.axonframework.messaging.core.annotation.ParameterResolverFactory;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.jspecify.annotations.Nullable;

import java.lang.reflect.Executable;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import static io.axoniq.framework.workflow.configuration.AutoDetectionUtils.*;
import static io.axoniq.framework.workflow.configuration.WorkflowMethodParameterResolverFactory.WORKFLOW_CONTEXT_RESOURCE_KEY;
import static io.axoniq.framework.workflow.configuration.WorkflowMethodParameterResolverFactory.WORKFLOW_INSTANCE_RESOURCE_KEY;

/**
 * Builder that auto-detects workflow definitions from annotations on a given component.
 * <p>
 * A single builder operates on one component class and can detect multiple workflow methods, producing a
 * {@link SimpleWorkflowModule.ConditionedWorkflowConfiguration} for each. The resulting configurations are registered
 * on the parent {@link SimpleWorkflowModule} during construction.
 * <p>
 * Every {@link io.axoniq.framework.workflow.runtime.api.annotation.Workflow} method on the detected component is built
 * once, at construction time, into an {@link AnnotatedHandlerInspector}-managed, enhancer-wrapped
 * {@link MessageHandlingMember}.
 *
 * @param <C> the type of {@link WorkflowContext} used by the workflows being built
 * @author Simon Zambrovski
 * @author Steven van Beelen
 * @since 5.4.0
 * TODO #494 - This class deserves further clean-up.
 */
@Internal
class AutoDetectingWorkflowBuilder<C extends WorkflowContext>
        implements WorkflowModule.WorkflowDefinitionPhase.FinalizedPhase<C> {

    /**
     * Constructs an {@code AutoDetectingWorkflowBuilder} for the given {@code parent} module.
     * <p>
     * The given {@code instanceBuilder} is used to create the component instance at build time, which is then inspected
     * for annotated workflow methods via {@link AutoDetectionUtils}.
     *
     * @param workflowContextType           the {@link WorkflowContext} type of the workflows being built
     * @param workflowContextFactoryBuilder a {@link ComponentBuilder} constructing the {@link WorkflowContextFactory}
     * @param parent                        the parent {@link SimpleWorkflowModule} to register the results on
     * @param instanceBuilder               a {@link ComponentBuilder} constructing the annotated workflow component
     */
    AutoDetectingWorkflowBuilder(
            Class<C> workflowContextType,
            ComponentBuilder<WorkflowContextFactory<C>> workflowContextFactoryBuilder,
            SimpleWorkflowModule<C> parent,
            ComponentBuilder<Object> instanceBuilder
    ) {
        Objects.requireNonNull(workflowContextType, "The workflow context type must not be null.");
        Objects.requireNonNull(workflowContextFactoryBuilder, "The workflow context factory builder must not be null.");
        Objects.requireNonNull(parent, "The parent workflow module must not be null.");
        Objects.requireNonNull(instanceBuilder, "The workflow instance builder must not be null.");

        ComponentBuilder<List<SimpleWorkflowModule.ConditionedWorkflowConfiguration<C>>> workflowConfigurationBuilder =
                configuration -> {
                    Object instance = instanceBuilder.build(configuration);
                    AnnotatedHandlerInspector<Object> inspector = buildInspector(instance, configuration);
                    return AutoDetectionUtils.workflowMethods(instance.getClass())
                                             .map(workflowMethod -> mapToWorkflowConfiguration(
                                                     workflowMethod, configuration, instance, inspector,
                                                     workflowContextType, workflowContextFactoryBuilder
                                             ))
                                             .toList();
                };
        parent.workflowConfigurationBuilder(workflowConfigurationBuilder);
    }

    /**
     * Builds the {@link AnnotatedHandlerInspector} shared by every workflow-annotated method detected on
     * {@code instance}'s class.
     * <p>
     * Composes the {@link Configuration}'s {@link HandlerEnhancerDefinition} chain with the {@link Configuration}'s
     * {@link HandlerDefinition} component — which, since {@link AnnotatedWorkflowDefinition} is classpath-discovered
     * (see {@code META-INF/services}), already recognizes {@code @Workflow} methods, on top of the framework's default
     * recognizers (including {@code AnnotatedMessageHandlingMemberDefinition}, needed so that
     * {@code @MessageHandlerInterceptor} methods declared on the same class are recognized and chained) — with
     * {@link AnnotatedWorkflowStatusChangedHandlerDefinition} (for
     * {@link io.axoniq.framework.workflow.runtime.api.annotation.WorkflowStatusChangedHandler} methods), which is
     * <b>not</b> classpath-discovered, as it recognizes methods by the generic {@link EventMessage} type and must
     * remain scoped to workflow autodetection only. Mirrors how {@code AnnotatedCommandHandlingComponent} itself is
     * built.
     */
    private static AnnotatedHandlerInspector<Object> buildInspector(Object instance, Configuration configuration) {
        MultiHandlerDefinition combinedHandlerDefinition = MultiHandlerDefinition.ordered(
                configuration.getComponent(HandlerEnhancerDefinition.class),
                configuration.getComponent(HandlerDefinition.class),
                new AnnotatedWorkflowStatusChangedHandlerDefinition()
        );
        @SuppressWarnings("unchecked")
        Class<Object> instanceType = (Class<Object>) instance.getClass();
        return AnnotatedHandlerInspector.inspectType(
                instanceType,
                configuration.getComponent(MessageTypeResolver.class),
                configuration.getComponent(ParameterResolverFactory.class),
                combinedHandlerDefinition
        );
    }

    /**
     * Finds the enhanced {@link MessageHandlingMember} the given {@code inspector} built for {@code method}.
     */
    static MessageHandlingMember<Object> findMember(
            AnnotatedHandlerInspector<Object> inspector,
            Object instance,
            Class<? extends Message> messageType,
            Method method
    ) {
        return inspector.getUniqueHandlers(instance.getClass(), messageType)
                        .stream()
                        .filter(member -> member.unwrap(Executable.class).map(method::equals).orElse(false))
                        .findFirst()
                        .map(AutoDetectingWorkflowBuilder::cast)
                        .orElseThrow(() -> new IllegalStateException(
                                "No enhanced handler member could be built for " + method
                        ));
    }

    @SuppressWarnings("unchecked")
    private static <T> MessageHandlingMember<T> cast(MessageHandlingMember<?> member) {
        return (MessageHandlingMember<T>) member;
    }

    private static <C extends WorkflowContext> SimpleWorkflowModule.ConditionedWorkflowConfiguration<C> mapToWorkflowConfiguration(
            MethodWithWorkflowAttributes workflowMethod,
            Configuration config,
            Object instance,
            AnnotatedHandlerInspector<Object> inspector,
            Class<C> workflowContextType,
            ComponentBuilder<WorkflowContextFactory<C>> workflowContextFactoryBuilder
    ) {
        Class<?> instanceType = instance.getClass();
        Map<String, @Nullable Object> attributes = workflowMethod.attributes();
        Method method = workflowMethod.method();
        AutoDetectionUtils.validateAttributes(attributes, instanceType, method);

        ComponentBuilder<EventCondition> eventConditionBuilder = eventConditionComponentBuilder(attributes);
        String workflowName = workflowName(instanceType, attributes, method);
        String workflowVersion = workflowVersion(attributes);

        validateWorkflowDefinition(workflowName, workflowVersion);

        MessageType messageType = new MessageType(new QualifiedName(workflowName), workflowVersion);
        MessageHandlingMember<Object> member = findMember(inspector, instance, WorkflowCommandMessage.class, method);

        WorkflowDefinition<C> workflowDefinition = workflowContext -> {
            WorkflowCommandMessage command = new WorkflowCommandMessage(messageType, workflowContext);
            ProcessingContext processingContext = workflowContext.processingContext()
                                                                 .withResource(WORKFLOW_CONTEXT_RESOURCE_KEY,
                                                                               workflowContext)
                                                                 .withResource(WORKFLOW_INSTANCE_RESOURCE_KEY,
                                                                               instance);
            CompletableFuture<?> future = inspector.chainedInterceptor(instanceType)
                                                   .handle(command, processingContext, instance, member)
                                                   .first()
                                                   .asCompletableFuture();
            FutureResolver.resolve(processingContext, future);
        };
        ComponentBuilder<WorkflowIdProvider> workflowIdProviderComponentBuilder =
                workflowIdProviderComponentBuilder(attributes);
        DefaultEventNameCustomizer namespaceCustomizer = namespace(instanceType, attributes);
        Map<WorkflowStatus, WorkflowStatusChangeListener> statusChangeListeners =
                statusChangeListeners(instance, workflowName, inspector);

        return new SimpleWorkflowModule.ConditionedWorkflowConfiguration<>(
                eventConditionBuilder.build(config),
                new SimpleWorkflowConfiguration<>(
                        workflowContextType,
                        workflowName,
                        workflowVersion,
                        workflowDefinition,
                        workflowContextFactoryBuilder.build(config),
                        workflowIdProviderComponentBuilder.build(config),
                        namespaceCustomizer,
                        statusChangeListeners
                )
        );
    }

    private static void validateWorkflowDefinition(String workflowName, String workflowVersion) {
        // validating by construction
        VersionedType.of(new QualifiedName(workflowName), workflowVersion);
    }
}
