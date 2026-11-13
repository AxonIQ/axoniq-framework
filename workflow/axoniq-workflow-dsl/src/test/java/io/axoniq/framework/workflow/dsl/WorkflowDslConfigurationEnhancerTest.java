/*
 * Copyright (c) 2010-2026. Axoniq B.V.
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
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class WorkflowDslConfigurationEnhancerTest {

    @Nested
    class FactoryRegistration {

        @Test
        void registersTheBaseWorkflowContextFactory() {
            // when
            var configuration = EventSourcingConfigurer.create().build();

            // then
            assertThat(configuration.getComponent(WorkflowContextFactory.class, BaseWorkflowContext.class.getName()))
                    .isInstanceOf(BaseWorkflowContextFactory.class);
        }

        @Test
        void registersTheSimpleWorkflowContextFactory() {
            // when
            var configuration = EventSourcingConfigurer.create().build();

            // then
            assertThat(configuration.getComponent(WorkflowContextFactory.class, SimpleWorkflowContext.class.getName()))
                    .isInstanceOf(SimpleWorkflowContextFactory.class);
        }
    }
}
