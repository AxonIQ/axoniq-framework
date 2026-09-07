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
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.runtime.api.execution.state.WorkflowState;
import org.jspecify.annotations.Nullable;
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
 * @since 5.4.0
 */
public class WorkflowStateParameterResolverFactory implements ParameterResolverFactory {

    private final Configuration configuration;

    /**
     * Constructs parameter resolver factory.
     */
    public WorkflowStateParameterResolverFactory(Configuration configuration) {
        this.configuration = requireNonNull(configuration, "The Configuration is required");
    }

    @Nullable
    @Override
    public ParameterResolver<?> createInstance(
            Executable executable,
            Parameter[] parameters,
            int parameterIndex) {
        Class<?> parameterType = parameters[parameterIndex].getType();
        if (WorkflowState.class.isAssignableFrom(parameterType)) {
            return new WorkflowStateParameterResolver(configuration);
        }
        return null;
    }
}
