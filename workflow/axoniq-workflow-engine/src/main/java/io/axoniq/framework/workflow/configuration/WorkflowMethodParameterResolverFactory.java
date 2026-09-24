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
import io.axoniq.framework.workflow.annotation.WorkflowStatusChangedHandler;
import io.axoniq.framework.workflow.dsl.api.WorkflowContext;
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import org.axonframework.common.annotation.AnnotationUtils;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.annotation.ParameterResolver;
import org.axonframework.messaging.core.annotation.ParameterResolverFactory;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jspecify.annotations.Nullable;

import java.lang.reflect.Executable;
import java.lang.reflect.Parameter;
import java.util.concurrent.CompletableFuture;

/**
 * Parameter resolver factory serving both {@link io.axoniq.framework.workflow.annotation.Workflow} body
 * methods and {@link io.axoniq.framework.workflow.annotation.WorkflowStatusChangedHandler} lifecycle
 * methods.
 *
 * @author Steven van Beelen
 * @since 5.4.0
 */
@Internal
public class WorkflowMethodParameterResolverFactory implements ParameterResolverFactory {

    /**
     * Resource key under which the {@link WorkflowContext} of the invocation in progress is stored.
     */
    public static final Context.ResourceKey<WorkflowContext> WORKFLOW_CONTEXT_RESOURCE_KEY =
            Context.ResourceKey.withLabel("workflowContext");
    /**
     * Resource key under which the {@link WorkflowStatus} of the invocation in progress is stored. Only populated for
     * lifecycle-handler invocations.
     */
    public static final Context.ResourceKey<WorkflowStatus> WORKFLOW_STATUS_RESOURCE_KEY =
            Context.ResourceKey.withLabel("workflowStatus");
    /**
     * Resource key under which the workflow-annotated component instance of the invocation in progress is stored.
     */
    public static final Context.ResourceKey<Object> WORKFLOW_INSTANCE_RESOURCE_KEY =
            Context.ResourceKey.withLabel("workflowInstance");

    @Nullable
    @Override
    public ParameterResolver<?> createInstance(Executable executable, Parameter[] parameters, int parameterIndex) {
        if (!isWorkflowAnnotated(executable)) {
            return null;
        }
        Class<?> parameterType = parameters[parameterIndex].getType();
        if (WorkflowStatus.class.isAssignableFrom(parameterType)) {
            return new ResourceParameterResolver<>(WORKFLOW_STATUS_RESOURCE_KEY);
        }
        if (WorkflowContext.class.isAssignableFrom(parameterType)) {
            return new ResourceParameterResolver<>(WORKFLOW_CONTEXT_RESOURCE_KEY);
        }
        if (parameterType.isAssignableFrom(executable.getDeclaringClass())) {
            return new ResourceParameterResolver<>(WORKFLOW_INSTANCE_RESOURCE_KEY);
        }
        if (AutoDetectionUtils.isWorkflowContextWrapper(parameterType)) {
            return new WorkflowContextWrapperParameterResolver(parameterType);
        }
        return null;
    }

    /**
     * Checks whether {@code executable} is a {@link Workflow} body method or a
     * {@link WorkflowStatusChangedHandler}-meta-annotated lifecycle method.
     * <p>
     * Without this check, {@link #createInstance(Executable, Parameter[], int)} would resolve parameters for
     * <em>any</em> executable in the application, since, for example, an {@code Object}-typed parameter is trivially
     * assignable from any declaring class.
     *
     * @param executable the executable to check
     * @return {@code true} if {@code executable} is workflow-related, {@code false} otherwise
     */
    private static boolean isWorkflowAnnotated(Executable executable) {
        return AnnotationUtils.findAnnotationAttributes(executable, Workflow.class).isPresent()
                || AnnotationUtils.findAnnotationAttributes(executable, WorkflowStatusChangedHandler.class).isPresent();
    }

    private record ResourceParameterResolver<T>(Context.ResourceKey<T> key) implements ParameterResolver<T> {

        @Override
        public CompletableFuture<T> resolveParameterValue(ProcessingContext context) {
            return CompletableFuture.completedFuture(context.getResource(key));
        }

        @Override
        public boolean matches(ProcessingContext context) {
            return context.containsResource(key);
        }
    }

    private record WorkflowContextWrapperParameterResolver(Class<?> wrapperType) implements ParameterResolver<Object> {

        @Override
        public CompletableFuture<Object> resolveParameterValue(ProcessingContext context) {
            WorkflowContext workflowContext = context.getResource(WORKFLOW_CONTEXT_RESOURCE_KEY);
            return CompletableFuture.completedFuture(AutoDetectionUtils.wrapIfPossible(wrapperType, workflowContext));
        }

        @Override
        public boolean matches(ProcessingContext context) {
            return context.containsResource(WORKFLOW_CONTEXT_RESOURCE_KEY);
        }
    }
}
