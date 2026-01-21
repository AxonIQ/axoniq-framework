package io.axoniq.workflow.runtime.engine;

import io.axoniq.workflow.runtime.api.workflow.WorkflowContext;
import io.axoniq.workflow.runtime.api.workflow.WorkflowDefinition;
import io.axoniq.workflow.runtime.context.ConversionDelegate;
import org.axonframework.common.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.axoniq.workflow.runtime.util.EventMessageUtils.eventMessageRetriever;
import static io.axoniq.workflow.runtime.util.Utils.createInstance;
import static io.axoniq.workflow.runtime.util.Utils.sleep;

public class Coordinator {

  private static final Logger logger = LoggerFactory.getLogger(Coordinator.class);

  private final WorkflowEngine workflowEngine;
  private final StateManager stateManager;
  private final ConversionDelegate conversionDelegate = new ConversionDelegate();

  private final Map<Class<?>, Class<? extends WorkflowDefinition<?>>> definitions = new HashMap<>();
  private final List<WorkflowContext> history = Collections.synchronizedList(new ArrayList<>());
  private final List<String> running = Collections.synchronizedList(new ArrayList<>());
  private final List<String> consumedMessages = Collections.synchronizedList(new ArrayList<>());

  private final ExecutorService executorService = Executors.newCachedThreadPool();

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
    definitions.forEach((eventType, workflowDefinitionType)
      -> CompletableFuture.runAsync(() -> pollForEvents(eventType, workflowDefinitionType), executorService)
    );
  }

  public void stop() {
    isRunning = false;
    executorService.shutdownNow();
    try {
      if (!executorService.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)) {
        logger.warn("Executor did not terminate within timeout");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.warn("Interrupted while waiting for executor termination");
    }
  }

  @SuppressWarnings("unchecked")
  private <T> void pollForEvents(Class<T> eventType, Class<? extends WorkflowDefinition<?>> workflowDefinitionType) {

    var definition = createInstance(workflowDefinitionType);

    while (isRunning) {
      try {
        var message = eventMessageRetriever(
          stateManager,
          eventType,
          m -> !consumedMessages.contains(m.identifier())
        ).orTimeout(500, java.util.concurrent.TimeUnit.MILLISECONDS).join();

        if (!isRunning) break;  // Check again after blocking call

        consumedMessages.add(message.identifier());

        var event = message.payloadAs(eventType);
        var payload = conversionDelegate.typeToPayloadConverter().apply(event);
        var workflowId = definition.workflowId(payload);

        boolean workflowIdExists = running.stream().anyMatch(id -> id.equals(workflowId));

        if (!workflowIdExists) {
          logger.info("Starting workflow {}:{} with payload {}.", workflowDefinitionType.getSimpleName(), workflowId, payload);

          ((CompletableFuture<WorkflowContext>) workflowEngine.execute(definition, payload))
            .whenComplete((completedContext, ex) -> {
              if (ex != null) {
                logger.error("Workflow {} finished with error.", workflowId, ex);
              } else {
                logger.info("Workflow {} finished with payload {}.", workflowId, completedContext.getPayload());
              }
              history.add(completedContext);
              running.remove(workflowId);
            });
          running.add(workflowId);

        } else {
          logger.info("Skipping event {}, since it would start workflow with id {}, which is already running.", event, workflowId);
        }
      } catch (Exception e) {
        if (!isRunning) break;  // Exit if shutting down
        // If there's an error (including timeout), log it and sleep a bit to avoid tight loops
        if (!(e.getCause() instanceof java.util.concurrent.TimeoutException)) {
          logger.error("Error processing event type {}: {}", eventType.getName(), e.getMessage(), e);
        }
        sleep(101);
      }
    }
    logger.info("Polling stopped for event type {}", eventType.getName());
  }

  public List<WorkflowContext> getHistory() {
    return Collections.unmodifiableList(history);
  }

  public List<String> getRunning() {
    return Collections.unmodifiableList(running);
  }
}
