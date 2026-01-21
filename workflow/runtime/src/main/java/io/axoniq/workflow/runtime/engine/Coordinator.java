package io.axoniq.workflow.runtime.engine;

import io.axoniq.workflow.runtime.api.workflow.WorkflowContext;
import io.axoniq.workflow.runtime.api.workflow.WorkflowDefinition;
import io.axoniq.workflow.runtime.context.ConversionDelegate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.axoniq.workflow.runtime.util.EventMessageUtils.eventMessageRetriever;

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
    executorService.shutdown();
  }

  @SuppressWarnings("unchecked")
  private <T> void pollForEvents(Class<T> eventType, Class<? extends WorkflowDefinition<?>> workflowDefinitionType) {
    if (!isRunning) {
      return;
    }

    try {
      eventMessageRetriever(
        stateManager,
        eventType,
        message -> !consumedMessages.contains(message.identifier())
      )
        .thenAccept(message -> {
          consumedMessages.add(message.identifier());

          var definition = createInstance(workflowDefinitionType);

          var event = message.payloadAs(eventType);
          var payload = conversionDelegate.typeToPayloadConverter().apply(event);
          var workflowId = definition.workflowId(payload);

          boolean workflowIdExists = running.stream().anyMatch(id -> id.equals(workflowId));

          if (!workflowIdExists) {
            logger.info("Starting workflow {}:{} with payload {}", workflowDefinitionType.getSimpleName(), workflowId, payload);

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
            logger.info("Skipping event {}, since it would start workflow with id {}, which is already running ", event, workflowId);
          }

          // Continue polling for more events
          pollForEvents(eventType, workflowDefinitionType);
        })
        .exceptionally(ex -> {

          sleep(100);
          if (isRunning) {
            pollForEvents(eventType, workflowDefinitionType);
          }

          return null;
        });
    } catch (Exception e) {
      sleep(100);
      if (isRunning) {
        pollForEvents(eventType, workflowDefinitionType);
      }
    }
  }

  public List<WorkflowContext> getHistory() {
    return Collections.unmodifiableList(history);
  }

  public List<String> getRunning() {
    return Collections.unmodifiableList(running);
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

  static void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    }
  }
}
