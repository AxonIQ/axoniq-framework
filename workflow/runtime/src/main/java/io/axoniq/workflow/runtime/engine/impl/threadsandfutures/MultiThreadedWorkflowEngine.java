package io.axoniq.workflow.runtime.engine.impl.threadsandfutures;

import io.axoniq.workflow.runtime.api.workflow.WorkflowConfiguration;
import io.axoniq.workflow.runtime.api.workflow.WorkflowContext;
import io.axoniq.workflow.runtime.api.workflow.WorkflowServices;
import io.axoniq.workflow.runtime.engine.execution.ExecutionSuspended;
import io.axoniq.workflow.runtime.engine.execution.WorkflowState;
import io.axoniq.workflow.runtime.engine.execution.WorkflowStatus;
import io.axoniq.workflow.runtime.engine.impl.ConversionDelegate;
import io.axoniq.workflow.runtime.engine.registry.WorkflowDefinitionRegistry;
import io.axoniq.workflow.runtime.engine.util.MetadataUtils;
import jakarta.annotation.Nonnull;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventHandler;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventSink;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class MultiThreadedWorkflowEngine implements EventHandler, WorkflowServices {

  private static final Logger logger = LoggerFactory.getLogger(MultiThreadedWorkflowEngine.class);

  // Stateless services
  private final ConversionDelegate conversionDelegate = new ConversionDelegate();
  private final Executor executor;
  private final UnitOfWorkFactory unitOfWorkFactory;
  private final Clock clock;
  private final EventSink eventSink;

  // Stateful registry
  private final WorkflowDefinitionRegistry<?> workflowDefinitionRegistry;

  // State of the workflow engine
  private final Map<String, ExecutionHandle> workflowInstances = new ConcurrentHashMap<>();

  // Stateful event subscription manager
  private final EventSubscriptionManager eventSubscriptionManager;
  // Stateful appender
  private final WorkflowEventAppender workflowEventAppender;
  // stateful task manager
  private final TaskManager taskManager;


  public MultiThreadedWorkflowEngine(
    @Nonnull UnitOfWorkFactory unitOfWorkFactory,
    @Nonnull EventSink eventSink,
    @Nonnull WorkflowDefinitionRegistry<?> workflowDefinitionRegistry
  ) {
    this.workflowDefinitionRegistry = workflowDefinitionRegistry;
    this.unitOfWorkFactory = unitOfWorkFactory;
    this.eventSink = eventSink;
    this.workflowEventAppender = new WorkflowEventAppender(this.eventSink);
    this.clock = Clock.systemDefaultZone();
    this.executor = Executors.newVirtualThreadPerTaskExecutor();
    this.eventSubscriptionManager = new EventSubscriptionManager(this);
    this.taskManager = new TaskManager(this);
    this.taskManager.start();
  }

  @NotNull
  @Override
  public MessageStream.Empty<Message> handle(@NotNull EventMessage event, @NotNull ProcessingContext processingContext) {
    if (MetadataUtils.hasWorkflowId().test(event.metadata())) {
      logger.info("Updating workflow with '{}'", event.type().qualifiedName());
      // state update
      updateState(event, processingContext);
      workflowEventAppender.onWorkflowEvent(event);
    } else {
      // handle starting of new processes
      checkAndCreateNewWorkflow(event, processingContext);
    }
    // notify subscription manager
    eventSubscriptionManager.onEvent(event);
    return MessageStream.empty();
  }

  /**
   * This is a place to be called from Event Processor
   */
  public void runWorkflows() {
    logger.debug("Starting {} workflows.", workflowInstances.size());
    for (var handle : workflowInstances.values()) {
      CompletableFuture.runAsync(() -> {
        try {
          handle.state.execute(handle.workflowConfiguration, handle.context);
        } catch (ExecutionSuspended e) {
          throw new RuntimeException(e);
        }
      }, executor);
    }
  }

  @Override
  public EventSubscriptionManager getEventSubscriptionManager() {
    return eventSubscriptionManager;
  }

  @Override
  public WorkflowEventAppender getWorkflowEventAppender() {
    return workflowEventAppender;
  }

  @Override
  public TaskManager getTaskManager() {
    return taskManager;
  }

  @Override
  public UnitOfWorkFactory getUnitOfWorkFactory() {
    return unitOfWorkFactory;
  }

  @Override
  public Executor getExecutor() {
    return executor;
  }

  @Override
  public EventSink getEventSink() {
    return null;
  }

  @Override
  public Clock getClock() {
    return this.clock;
  }

  public Map<String, ExecutionHandle> workflowInstances() {
    return workflowInstances;
  }

  public void shutdown() {
    taskManager.shutdown();
  }

  public int cancel(String workflowId) {
    int tasksCancelled = taskManager.cancel(workflowId);
    int subscriptionsCancelled = eventSubscriptionManager.cancel(workflowId);
    return tasksCancelled + subscriptionsCancelled;
  }

  public static class ExecutionHandle {
    WorkflowConfiguration<?> workflowConfiguration;
    WorkflowContext context;
    WorkflowState state;

    public ExecutionHandle(WorkflowConfiguration<?> workflowConfiguration, WorkflowContext context, WorkflowState state) {
      this.workflowConfiguration = workflowConfiguration;
      this.state = state;
      this.context = context;
    }

    public WorkflowStatus getStatus() {
      return context.getStatus();
    }

    public WorkflowContext getContext() {
      return context;
    }
  }


  private void checkAndCreateNewWorkflow(EventMessage eventMessage,
                                         ProcessingContext processingContext) {
    var definitions = workflowDefinitionRegistry.getWorkflowsConfigurations(eventMessage.type().qualifiedName());
    definitions.forEach(
      workflowConfiguration -> {
        var payload = conversionDelegate.typeToPayloadConverter().apply(eventMessage.payload());

        var workflowContext = workflowConfiguration.workflowContextFactory()
          .createContext(payload, eventMessage.timestamp(), processingContext, this);
        var workflowId = workflowContext.getWorkflowId();

        // avoid multiple workflows for the same workflow id.
        workflowInstances.computeIfAbsent(workflowId, (id) -> {
          logger.info("Starting new workflow with '{}'", eventMessage.payload());
          var workflowState = workflowConfiguration.workflowStateFactory().create(workflowContext);
          return new ExecutionHandle(workflowConfiguration, workflowContext, workflowState);
        });
      }
    );
  }

  private void updateState(EventMessage event, ProcessingContext processingContext) {
    var workflowId = MetadataUtils.getWorkflowId(event.metadata());
    var workflowInstance = Objects.requireNonNull(workflowInstances.get(workflowId), "No workflow found for id " + workflowId);
    workflowInstance.state.onEvent(event, processingContext);
  }
}
