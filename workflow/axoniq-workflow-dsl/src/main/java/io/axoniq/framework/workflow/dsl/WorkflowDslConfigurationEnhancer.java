/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.dsl;

import io.axoniq.framework.workflow.dsl.base.BaseWorkflowContext;
import io.axoniq.framework.workflow.dsl.base.BaseWorkflowContextFactory;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContextFactory;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowContextFactory;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;

/**
 * ServiceLoader-discovered {@link ConfigurationEnhancer} that registers the standard Java DSL workflow context
 * factories.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
public class WorkflowDslConfigurationEnhancer implements ConfigurationEnhancer {

    @Override
    public void enhance(ComponentRegistry registry) {
        registry.registerIfNotPresent(WorkflowContextFactory.class,
                                      BaseWorkflowContext.class.getName(),
                                      configuration -> new BaseWorkflowContextFactory());
        registry.registerIfNotPresent(WorkflowContextFactory.class,
                                      SimpleWorkflowContext.class.getName(),
                                      configuration -> new SimpleWorkflowContextFactory());
    }
}
