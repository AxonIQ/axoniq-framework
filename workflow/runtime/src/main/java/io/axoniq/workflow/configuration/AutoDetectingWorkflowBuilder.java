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
package io.axoniq.workflow.configuration;

import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContextFactory;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowDefinition;
import io.axoniq.workflow.runtime.util.WorkflowReflectionUtils;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.configuration.ComponentBuilder;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;

import java.util.List;
import java.util.Objects;

import static io.axoniq.workflow.configuration.AutoDetectionUtils.*;

/**
 * Builder that auto-detects workflow definitions from annotations on a given component.
 * <p>
 * A single builder operates on one component class and can detect multiple workflow methods, producing a
 * {@link SimpleWorkflowModule.ConditionedWorkflowConfiguration} for each. The resulting configurations are registered
 * on the parent {@link SimpleWorkflowModule} during construction.
 *
 * @param <C> the type of {@link WorkflowContext} used by the workflows being built
 * @author Simon Zambrovski
 * @since 0.1.0
 */
@Internal
class AutoDetectingWorkflowBuilder<C extends WorkflowContext>
        implements WorkflowModule.WorkflowDefinitionPhase.FinalizedPhase<C> {

    /**
     * Constructs an {@link AutoDetectingWorkflowBuilder} for the given {@code parent} module.
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
                    var instance = instanceBuilder.build(configuration);
                    return AutoDetectionUtils.workflowMethods(instance.getClass(), workflowContextType)
                                             .map(workflowMethod -> mapToWorkflowConfiguration(
                                                     workflowMethod, configuration, instance,
                                                     workflowContextType, workflowContextFactoryBuilder
                                             ))
                                             .toList();
                };
        parent.workflowConfigurationBuilder(workflowConfigurationBuilder);
    }

    private static <C extends WorkflowContext> SimpleWorkflowModule.ConditionedWorkflowConfiguration<C> mapToWorkflowConfiguration(
            MethodWithWorkflowAttributes workflowMethod,
            Configuration config,
            Object instance,
            Class<C> workflowContextType,
            ComponentBuilder<WorkflowContextFactory<C>> workflowContextFactoryBuilder
    ) {
        Class<?> instanceType = instance.getClass();
        var attributes = workflowMethod.attributes();
        var method = workflowMethod.method();
        AutoDetectionUtils.validateAttributes(attributes, instanceType, method);

        var eventConditionBuilder = eventConditionComponentBuilder(attributes);
        var workflowName = workflowName(instanceType, attributes, method);
        var workflowVersion = workflowVersion(attributes);

        validateWorkflowDefinition(workflowName, workflowVersion);

        WorkflowDefinition<C> workflowDefinition = workflowContext -> {
            Class<?>[] parameterTypes = method.getParameterTypes();
            Object[] args = new Object[parameterTypes.length];
            for (int i = 0; i < parameterTypes.length; i++) {
                Class<?> paramType = parameterTypes[i];
                if (paramType.isInstance(workflowContext)) {
                    args[i] = workflowContext;
                } else if (paramType.isInstance(instance)) {
                    args[i] = instance;
                } else {
                    Object wrap = AutoDetectionUtils.wrapIfPossible(paramType, workflowContext);
                    if (wrap != null) {
                        args[i] = wrap;
                    }
                }
            }
            WorkflowReflectionUtils.invoke(instance, method, args);
        };
        var workflowIdProviderComponentBuilder = workflowIdProviderComponentBuilder(attributes);
        var namespaceCustomizer = namespace(instanceType, attributes);
        var statusChangeListeners = statusChangeListeners(instance, workflowContextType, workflowName);

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
        new MessageType(new QualifiedName(workflowName), workflowVersion);
    }
}
