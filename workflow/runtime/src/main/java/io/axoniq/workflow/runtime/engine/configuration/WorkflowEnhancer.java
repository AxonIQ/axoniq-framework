package io.axoniq.workflow.runtime.engine.configuration;

import io.axoniq.workflow.runtime.api.WorkflowDefinitionRegistry;
import io.axoniq.workflow.runtime.engine.impl.WorkflowEngine;
import io.axoniq.workflow.runtime.engine.registry.SimpleWorkflowDefinitionRegistry;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventSink;
import org.axonframework.messaging.eventhandling.configuration.EventProcessorModule;
import org.jetbrains.annotations.NotNull;

import static io.axoniq.workflow.runtime.engine.configuration.AllEventEventHandlingComponent.ANY_EVENT_IN_ONE_SEGMENT;

/**
 * Enhancer for registration of the workflow engine, the registry and sets up the eventing.
 */
public class WorkflowEnhancer implements ConfigurationEnhancer {

  @Override
  public void enhance(@NotNull ComponentRegistry componentRegistry) {

    componentRegistry
      .registerComponent(WorkflowDefinitionRegistry.class, cfg -> new SimpleWorkflowDefinitionRegistry());

    componentRegistry
      .registerComponent(WorkflowEngine.class, cfg ->
        new WorkflowEngine(
          cfg.getComponent(UnitOfWorkFactory.class),
          cfg.getComponent(EventSink.class),
          cfg.getComponent(WorkflowDefinitionRegistry.class),
          cfg.getComponent(Converter.class)
        )
      );
    componentRegistry.registerModule(
      EventProcessorModule
        .pooledStreaming("WorkflowEngine")
        .eventHandlingComponents(req -> req.declarative(cfg -> new AllEventEventHandlingComponent(
          cfg.getComponent(WorkflowEngine.class)
        )))
        .customized(ANY_EVENT_IN_ONE_SEGMENT)
        .build()
    );
  }
}
