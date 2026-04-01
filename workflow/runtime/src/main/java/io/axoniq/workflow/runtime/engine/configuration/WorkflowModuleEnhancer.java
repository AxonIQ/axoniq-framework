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
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */
package io.axoniq.workflow.runtime.engine.configuration;

import jakarta.annotation.Nonnull;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.annotation.RegistrationScope;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.common.configuration.Module;

/**
 * A workflow module enhancer is responsible for registration of a single workflow module. A workflow module is a
 * logical unit used for configuration of a set of workflow definitions and written in a particular workflow DSL.
 *
 * @author Mateusz Nowak
 * @author Simon Zambrovski
 * @since 1.0.0
 */
@RegistrationScope(scope = RegistrationScope.Scope.CURRENT)
@Internal
public class WorkflowModuleEnhancer implements ConfigurationEnhancer {

    private final Module workflowModule;

    /**
     * Configures the enhancer to register the given workflow module.
     *
     * @param workflowModule module to register.
     */
    public WorkflowModuleEnhancer(Module workflowModule) {
        this.workflowModule = workflowModule;
    }

    @Override
    public void enhance(@Nonnull ComponentRegistry componentRegistry) {
        componentRegistry.registerModule(workflowModule);
    }
}
