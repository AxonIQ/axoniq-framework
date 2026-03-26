package io.axoniq.workflow.runtime.engine.configuration;

import io.axoniq.workflow.runtime.engine.history.WorkflowHistoryProjector;
import io.axoniq.workflow.runtime.engine.impl.WorkflowEngine;
import jakarta.annotation.Nonnull;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.annotation.RegistrationScope;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.messaging.eventhandling.configuration.EventProcessorModule;

import static io.axoniq.workflow.runtime.engine.configuration.AllEventEventHandlingComponent.ANY_EVENT_IN_ONE_SEGMENT;

@RegistrationScope(scope = RegistrationScope.Scope.CURRENT)
@Internal
public class WorkflowEventProcessingRegistrationEnhancer implements ConfigurationEnhancer {

    public static final String WORKFLOW_ENGINE_EVENT_MODULE = "WorkflowEngine";

    @Override
    public void enhance(@Nonnull ComponentRegistry componentRegistry) {
        componentRegistry.registerModule(
                EventProcessorModule
                        .pooledStreaming(WORKFLOW_ENGINE_EVENT_MODULE)
                        .eventHandlingComponents(req -> req
                                .declarative("workflowEngineComponent", cfg -> new AllEventEventHandlingComponent(
                                                     cfg.getComponent(WorkflowEngine.class)
                                             )
                                ).declarative("workflowHistoryProjector", cfg -> new AllEventEventHandlingComponent(
                                        cfg.getComponent(WorkflowHistoryProjector.class)
                                ))
                        )
                        .customized(ANY_EVENT_IN_ONE_SEGMENT)
                        .build()
        );
    }

    @Override
    public int order() {
        return ConfigurationEnhancer.super.order();
    }
}
