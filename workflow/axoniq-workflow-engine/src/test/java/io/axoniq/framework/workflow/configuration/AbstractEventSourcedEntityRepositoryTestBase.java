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

import io.axoniq.framework.workflow.dsl.api.EventConditions;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowConfiguration;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecutionOperations;
import io.axoniq.framework.workflow.runtime.execution.AbstractWorkflowContext;
import io.axoniq.framework.workflow.runtime.execution.WorkflowEngine;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.axonframework.modelling.repository.Repository;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.util.Map;

import static org.mockito.Mockito.*;

/**
 * Abstract base class for testing event-sourced entity repositories.
 *
 * @author Simon Zambrovski
 */
abstract class AbstractEventSourcedEntityRepositoryTestBase {

    protected AxonConfiguration configuration;

    @AfterEach
    void tearDownConfiguration() {
        if (configuration != null) {
            configuration.shutdown();
        }
    }

    protected AxonConfiguration configuration(String moduleName) {
        var module = WorkflowModule.defaults(moduleName, TestContext.class)
                                   .definition(d -> d
                                           .declarative(c -> ctx -> {
                                           })
                                           .workflowName(moduleName)
                                           .on(c -> EventConditions.fromQualifiedName(new QualifiedName("start")))
                                           .notCustomized()
                                   )
                                   .contextFactory(c -> TestContext::new);

        var configurer = WorkflowConfigurer.create();
        configurer.componentRegistry(cr -> cr.registerModule(module));
        return configurer.build();
    }

    protected void publish(EventMessage eventMessage) {
        var eventStore = configuration.getComponent(EventStore.class);
        var unitOfWorkFactory = configuration.getComponent(UnitOfWorkFactory.class);
        unitOfWorkFactory.create("publish-event-sourced-entity-test")
                         .executeWithResult(context -> eventStore.publish(context, eventMessage)
                                                                 .thenApply(ignored -> eventMessage))
                         .join();
    }

    @SuppressWarnings("unchecked")
    protected <T> Repository<String, T> repository(Class<T> entityType) {
        return configuration.getComponents(Repository.class)
                            .values()
                            .stream()
                            .filter(repository -> repository.entityType().equals(entityType))
                            .map(repository -> (Repository<String, T>) repository)
                            .findFirst()
                            .orElseThrow();
    }

    protected WorkflowExecutionOperations workflowExecutionOperations(String workflowId) {
        return workflowExecutionOperations(workflowId, "0.0.1");
    }

    protected WorkflowExecutionOperations workflowExecutionOperations(String workflowId, String version) {
        var context = mock(WorkflowExecutionOperations.class);
        when(context.workflowId()).thenReturn(workflowId);
        when(context.workflowPayload()).thenReturn(Map.of("orderId", workflowId));
        when(context.workflowVersion()).thenReturn(version);
        ProcessingContext processingContext = mock(ProcessingContext.class);
        when(context.processingContext()).thenReturn(processingContext);
        when(processingContext.component(EventConverter.class)).thenReturn(mock(EventConverter.class));
        return context;
    }

    protected WorkflowEngine getWorkflowEngine(String moduleName) {
        return configuration.getComponents(WorkflowEngine.class)
                            .get("WorkflowEngine[" + moduleName + "]");
    }

    static class TestContext extends AbstractWorkflowContext {

        TestContext(Map<String, @Nullable Object> payload,
                    String workflowId,
                    ProcessingContext processingContext,
                    WorkflowConfiguration<?> workflowConfiguration) {
            super(workflowId, payload, processingContext, workflowConfiguration);
        }
    }
}
