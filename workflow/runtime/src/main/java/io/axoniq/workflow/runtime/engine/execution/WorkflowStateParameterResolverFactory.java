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
package io.axoniq.workflow.runtime.engine.execution;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.messaging.core.annotation.ParameterResolver;
import org.axonframework.messaging.core.annotation.ParameterResolverFactory;

import java.lang.reflect.Executable;
import java.lang.reflect.Parameter;

import static java.util.Objects.requireNonNull;

/**
 * Parameter resolver factory for workflow state.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
public class WorkflowStateParameterResolverFactory implements ParameterResolverFactory {

    private final Configuration configuration;

    /**
     * Constructs parameter resolver factory.
     */
    public WorkflowStateParameterResolverFactory(@Nonnull Configuration configuration) {
        this.configuration = requireNonNull(configuration, "The Configuration is required");
    }

    @Nullable
    @Override
    public ParameterResolver<?> createInstance(
            @Nonnull Executable executable,
            @Nonnull Parameter[] parameters,
            int parameterIndex) {
        Class<?> parameterType = parameters[parameterIndex].getType();
        if (WorkflowState.class.isAssignableFrom(parameterType)) {
            return new WorkflowStateParameterResolver(configuration);
        }
        return null;
    }
}
