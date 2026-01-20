package io.axoniq.workflow.runtime.engine;

import io.axoniq.workflow.runtime.api.workflow.WorkflowContext;
import io.axoniq.workflow.runtime.api.workflow.WorkflowDefinition;
import io.axoniq.workflow.runtime.context.ConversionDelegate;
import io.axoniq.workflow.runtime.util.EventMessageUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;

public class Coordinator {

  private static final Logger logger = LoggerFactory.getLogger(Coordinator.class);

  private final WorkflowEngine workflowEngine;
  private final StateManager stateManager;
  private final Map<Class<?>, Class<? extends WorkflowDefinition<?>>> definitions = new HashMap<>();
  private final List<WorkflowContext> history = new ArrayList<>();
  private final ConversionDelegate conversionDelegate = new ConversionDelegate();

  public Coordinator(StateManager stateManager) {
    this.stateManager = stateManager;
    this.workflowEngine = new WorkflowEngine(stateManager);
  }

  public void register(
    Class<? extends WorkflowDefinition<?>> workflowDefinitionType,
    Class<?> eventType
  ) {
    definitions.put(eventType, workflowDefinitionType);
  }

  public CompletableFuture<Void> start() {
    var futures = definitions.entrySet().stream().map(entry -> {
      var type = entry.getKey();
      var workflowDefinition = entry.getValue();
      return EventMessageUtils
        .eventRetriever(this.stateManager, type, e -> true)
        .thenApply(e -> conversionDelegate.typeToPayloadConverter().apply(e))
        .thenApply(payload -> {
          logger.info("Starting workflow definition {} with payload {}", workflowDefinition.getSimpleName(), payload);
          var definition = createInstance(workflowDefinition);
          var historic = workflowEngine.execute(definition, payload);
          history.add(historic);
          return historic;
        })
        .exceptionally(ex -> {
          logger.error("Error executing workflow definition {}: {}", workflowDefinition.getSimpleName(), ex.getMessage(), ex);
          return null;
        }); // TODO -> timeout, interception, etc...
    }).toList();

    // Execute all workflows concurrently and wait for all to complete
    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
  }

  public List<WorkflowContext> getHistory() {
    return history;
  }

  static <T> T createInstance(Class<T> clazz) {
    try {
      var constructor = clazz.getDeclaredConstructor();
      constructor.setAccessible(true);
      return constructor.newInstance();
    } catch (Exception e) {
      logger.error("Error instantiating workflow definition {}: {}", clazz.getName(), e.getMessage(), e);
      throw new RuntimeException("Unable to instantiate workflow definition: " + clazz.getName(), e);
    }
  }


}
