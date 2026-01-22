package io.axoniq.workflow.runtime.engine;

import io.axoniq.workflow.runtime.api.workflow.WorkflowConfiguration;
import io.axoniq.workflow.runtime.api.workflow.WorkflowContext;
import io.axoniq.workflow.runtime.context.ConversionDelegate;
import org.axonframework.messaging.core.QualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Coordinator {

  private static final Logger logger = LoggerFactory.getLogger(Coordinator.class);

  private final SimpleWorkflowConfigurationRegistry workflowConfigurationRegistry;
  private final WorkflowEngine workflowEngine;
  private final StateManager stateManager;
  private final ConversionDelegate conversionDelegate = new ConversionDelegate();

  private final List<WorkflowContext> history = Collections.synchronizedList(new ArrayList<>());
  private final List<String> running = Collections.synchronizedList(new ArrayList<>());
  private final List<String> consumedMessages = Collections.synchronizedList(new ArrayList<>());
  private final List<Subscription> activeSubscriptions = Collections.synchronizedList(new ArrayList<>());

  private volatile boolean isRunning = false;


  public Coordinator(StateManager stateManager) {
    this.stateManager = stateManager;
    this.workflowEngine = new WorkflowEngine(stateManager);
    this.workflowConfigurationRegistry = new SimpleWorkflowConfigurationRegistry();
  }

  // FIXME -> move to module configurer...
  public SimpleWorkflowConfigurationRegistry declarative() {
    return this.workflowConfigurationRegistry;
  }

  public void start() {
    if (isRunning) {
      logger.warn("Coordinator is already running");
      return;
    }
    isRunning = true;
    workflowConfigurationRegistry.getWorkflowsConfigurations()
      .forEach((key, value) -> value.forEach(c -> subscribeForTriggerEvents(key, c)));
  }

  public void stop() {
    isRunning = false;
    // Cancel all active subscriptions
    activeSubscriptions.forEach(Subscription::cancel);
    activeSubscriptions.clear();
  }

  private <T> void subscribeForTriggerEvents(QualifiedName qualifiedName, WorkflowConfiguration<?> workflowConfiguration) {
    var subscription = stateManager.subscribe(
      qualifiedName,
      m -> !consumedMessages.contains(m.identifier()),
      event -> {
        if (!isRunning) {
          return true; // Unsubscribe when coordinator is stopped
        }

        try {
          consumedMessages.add(event.identifier());

          var payload = conversionDelegate.typeToPayloadConverter().apply(event.payload());
          var workflowId = workflowConfiguration.associationProvider().associationKey(payload)
            .orElseThrow(() -> new IllegalStateException("Correlation key is a mandatory requirement"));

          boolean workflowIdExists = running.contains(workflowId);


          if (!workflowIdExists) {
            logger.info("Starting workflow {}: {} with payload {}.", workflowConfiguration.workflowDefinition()
              .getClass().getSimpleName(), workflowId, payload);

            running.add(workflowId);
            workflowEngine.restoreAndExecute(workflowConfiguration, payload)
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
              event.payload(), workflowId);
          }
        } catch (Exception e) {
          logger.error("Error processing event type {}: {}", qualifiedName, e.getMessage(), e);
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

}
