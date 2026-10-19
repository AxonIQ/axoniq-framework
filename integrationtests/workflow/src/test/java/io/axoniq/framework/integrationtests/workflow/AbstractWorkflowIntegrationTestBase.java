/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.integrationtests.workflow;

import io.axoniq.framework.axonserver.connector.configuration.AxonServerConfigurationEnhancer;
import io.axoniq.framework.workflow.configuration.WorkflowConfigurer;
import io.axoniq.framework.workflow.dsl.api.WorkflowContext;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowContextFactory;
import io.axoniq.framework.workflow.runtime.execution.WorkflowExecutionRepository;
import io.axoniq.framework.workflow.runtime.test.AbstractWorkflowTestBase;
import org.axonframework.common.configuration.ComponentBuilder;

import java.util.function.UnaryOperator;

/**
 * Abstract test base for Workflow integration tests, disabling Axon Server and Multi-Tenancy.
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

    /**
     * Returns the {@link WorkflowExecutionRepository} tied to this test.
     *
     * @return the {@link WorkflowExecutionRepository} tied to this test
     */
    protected WorkflowExecutionRepository executionRepository() {
        return configuration.getComponents(WorkflowExecutionRepository.class)
                            .get("WorkflowExecutionRepository[" + getClass().getSimpleName() + "]");
    }
}
