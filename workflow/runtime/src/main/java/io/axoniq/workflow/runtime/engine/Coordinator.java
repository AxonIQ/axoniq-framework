package io.axoniq.workflow.runtime.engine;

import io.axoniq.workflow.runtime.api.workflow.WorkflowContext;
import io.axoniq.workflow.runtime.api.workflow.WorkflowDefinition;
import io.axoniq.workflow.runtime.context.ConversionDelegate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;

import static io.axoniq.workflow.runtime.util.Utils.createInstance;

public class Coordinator {

  private static final Logger logger = LoggerFactory.getLogger(Coordinator.class);

  private final WorkflowEngine workflowEngine;
  private final StateManager stateManager;
  private final ConversionDelegate conversionDelegate = new ConversionDelegate();

  private final Map<Class<?>, Class<? extends WorkflowDefinition<?>>> definitions = new HashMap<>();
  private final List<WorkflowContext> history = Collections.synchronizedList(new ArrayList<>());
  private final List<String> running = Collections.synchronizedList(new ArrayList<>());
  private final List<String> consumedMessages = Collections.synchronizedList(new ArrayList<>());
  private final List<Subscription> activeSubscriptions = Collections.synchronizedList(new ArrayList<>());

  private volatile boolean isRunning = false;

  public Coordinator(StateManager stateManager) {
    this.stateManager = stateManager;
    this.workflowEngine = new WorkflowEngine(stateManager);
  }

  public void register(Class<? extends WorkflowDefinition<?>> workflowDefinitionType, Class<?> eventType) {
    definitions.put(eventType, workflowDefinitionType);
  }

  public void start() {
    if (isRunning) {
      logger.warn("Coordinator is already running");
      return;
    }
    isRunning = true;
    definitions.forEach(this::subscribeForTriggerEvents
    );
  }

  public void stop() {
    isRunning = false;
    // Cancel all active subscriptions
    activeSubscriptions.forEach(Subscription::cancel);
    activeSubscriptions.clear();
  }

  @SuppressWarnings("unchecked")
  private <T> void subscribeForTriggerEvents(Class<T> eventType,
                                              Class<? extends WorkflowDefinition<?>> workflowDefinitionType) {
    var definition = createInstance(workflowDefinitionType);

    var subscription = stateManager.subscribe(
        eventType,
        m -> !consumedMessages.contains(m.identifier()),
        event -> {
          if (!isRunning) {
            return true; // Unsubscribe when coordinator is stopped
          }

          try {
            consumedMessages.add(event.identifier());

            var payload = conversionDelegate.typeToPayloadConverter().apply(event.payloadAs(eventType));
            var workflowId = definition.workflowId(payload);

            boolean workflowIdExists = running.contains(workflowId);

            if (!workflowIdExists) {
              logger.info("Starting workflow {}:{} with payload {}.", workflowDefinitionType.getSimpleName(), workflowId, payload);

              running.add(workflowId);
              applyAndExecute(definition, payload)
                  .whenComplete((completedContext, ex) -> {
                    if (ex != null) {
                      logger.error("Workflow {} finished with error.", workflowId, ex);
                    } else {
                      logger.info("Workflow {} finished with payload {}.", workflowId, completedContext.getPayload());
                    }
                    history.add(completedContext);
                    running.remove(workflowId);
                  });
            } else {
              logger.info("Skipping event {}, since it would start workflow with id {}, which is already running.",
                  event.payloadAs(eventType), workflowId);
            }
          } catch (Exception e) {
            logger.error("Error processing event type {}: {}", eventType.getName(), e.getMessage(), e);
          }

          return false; // Keep subscription active
        });

    activeSubscriptions.add(subscription);
  }

  public List<WorkflowContext> getHistory() {
    return Collections.unmodifiableList(history);
  }

  public List<String> getRunning() {
    return Collections.unmodifiableList(running);
  }

  /**
   * Event Sourcing: Hydrate context state from history, then execute workflow.
   */
  private <T extends WorkflowContext> CompletableFuture<T> applyAndExecute(
      WorkflowDefinition<T> definition,
      Map<String, Object> payload) {
    //Event Source context
    T context = workflowEngine.apply(definition, payload);
    //Execute on context
    return workflowEngine.execute(definition, payload, context);
  }
}
