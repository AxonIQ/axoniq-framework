/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.configuration;

import io.axoniq.framework.workflow.index.inmemory.InMemoryWorkflowIndex;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndex;

/**
 * Runs the tests of {@link WorkflowConfigurerWaitAssociationRoutingTest} with a {@link WorkflowIndex}, so a claimed
 * segment reads its open waits from the rows of the index instead of sourcing them.
 *
 * @author Stefan Dragisic
 */
class WorkflowConfigurerWaitAssociationRoutingWithWorkflowIndexTest extends WorkflowConfigurerWaitAssociationRoutingTest {

    // Shared by the configurations of one test, so a restarted node reads the rows of the stopped one.
    private final WorkflowIndex index = new InMemoryWorkflowIndex();

    @Override
    void configure(WorkflowConfigurer configurer) {
        configurer.registerWorkflowIndex(configuration -> index);
    }
}
