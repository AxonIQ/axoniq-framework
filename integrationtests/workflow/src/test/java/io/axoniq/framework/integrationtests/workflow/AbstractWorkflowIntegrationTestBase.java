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
package io.axoniq.framework.integrationtests.workflow;

import io.axoniq.framework.axonserver.connector.configuration.AxonServerConfigurationEnhancer;
import io.axoniq.framework.workflow.configuration.WorkflowConfigurer;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowContextFactory;
import io.axoniq.framework.workflow.runtime.test.AbstractWorkflowTestBase;
import org.axonframework.common.configuration.ComponentBuilder;

import java.util.function.UnaryOperator;

/**
 * Abstarct test base for Workflow integration tests, disabling Axon Server and Multi-Tenancy.
 *
 * @param <T> workflow context type
 */
abstract class AbstractWorkflowIntegrationTestBase<T extends WorkflowContext> extends AbstractWorkflowTestBase<T> {

    protected AbstractWorkflowIntegrationTestBase(Class<T> dslType,
                                                  ComponentBuilder<WorkflowContextFactory<T>> contextFactoryBuilder) {
        super(dslType, contextFactoryBuilder);
    }

    @Override
    protected UnaryOperator<WorkflowConfigurer> configure() {
        return c -> c.componentRegistry(r -> r.disableEnhancer(AxonServerConfigurationEnhancer.class));
    }
}
