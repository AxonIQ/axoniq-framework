package io.axoniq.workflow.runtime.engine.impl.multi;

import io.axoniq.workflow.runtime.api.workflow.WorkflowConfiguration;
import io.axoniq.workflow.runtime.api.workflow.WorkflowContext;
import io.axoniq.workflow.runtime.api.workflow.WorkflowFailedException;
import io.axoniq.workflow.runtime.api.workflow.WorkflowServices;
import io.axoniq.workflow.runtime.engine.execution.ExecutionSuspended;
import io.axoniq.workflow.runtime.engine.execution.WorkflowState;
import io.axoniq.workflow.runtime.engine.execution.WorkflowStatus;
import io.axoniq.workflow.runtime.engine.registry.WorkflowRepository;
import io.axoniq.workflow.runtime.engine.util.MetadataUtils;
import jakarta.annotation.Nonnull;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.ProcessingLifecycle;
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

import static io.axoniq.workflow.runtime.engine.util.EventMessageUtils.*;

public class EventBasedWorkflowEngine implements EventHandler, WorkflowServices {

  private static final Logger logger = LoggerFactory.getLogger(EventBasedWorkflowEngine.class);

  // Stateless services
  private final ConversionDelegate conversionDelegate = new ConversionDelegate();
  private final Executor executor;
  private final UnitOfWorkFactory unitOfWorkFactory;
  private final Clock clock;
  private final EventSink eventSink;

  // Stateful registry
  private final WorkflowRepository<?> workflowRepository;

  // State of the workflow engine
  private final Map<String, ExecutionHandle> workflowInstances = new ConcurrentHashMap<>();

  // Stateful event subscription manager
  private final EventSubscriptionManager eventSubscriptionManager;
  // Stateful appender
  private final WorkflowEventAppender workflowEventAppender;
  // stateful task manager
  private final TaskManager taskManager;


  public EventBasedWorkflowEngine(
    @Nonnull UnitOfWorkFactory unitOfWorkFactory,
    @Nonnull EventSink eventSink,
    @Nonnull WorkflowRepository<?> workflowRepository
  ) {
    this.workflowRepository = workflowRepository;
    this.workflowEventAppender = new WorkflowEventAppender(eventSink);
    this.unitOfWorkFactory = unitOfWorkFactory;
    this.eventSink = eventSink;
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
      executeVirtual(handle.workflowConfiguration, handle.context);
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


  private void checkAndCreateNewWorkflow(EventMessage event, ProcessingContext context) {
    var definitions = workflowRepository.getWorkflowsConfigurations(event.type().qualifiedName());
    definitions.forEach(
      workflowConfiguration -> {
        var payload = conversionDelegate.typeToPayloadConverter().apply(event.payload());

        var workflowContext = workflowConfiguration.workflowContextFactory().createContext(payload, this);
        var workflowId = workflowContext.getWorkflowId();

        // avoid multiple workflows for the same workflow id.
        workflowInstances.computeIfAbsent(workflowId, (id) -> {
          logger.info("Starting new workflow with '{}'", event.payload());
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


  private <T extends WorkflowContext> CompletableFuture<T> executeVirtual(WorkflowConfiguration<T> configuration, WorkflowContext workflowContext) {
    return CompletableFuture.supplyAsync(() -> {
      try {
        return execute(configuration, workflowContext);
      } catch (ExecutionSuspended e) {
        throw new RuntimeException(e);
      }
    }, executor);
  }

  private <T extends WorkflowContext> T execute(WorkflowConfiguration<T> configuration, WorkflowContext workflowContext) throws ExecutionSuspended {
    //noinspection unchecked
    T context = (T) workflowContext;
    try {
      // Check if workflow is already in terminal state - do nothing
      if (context.getStatus().isTerminal()) {
        return context;
      }

      // Optional, maybe we don't need a workflow started event at all
      if (context.getStatus() == WorkflowStatus.NONE) {
        started(context, configuration);
      }

      // this execution will run until it is blocked by a wait for event
      configuration.workflowDefinition().execute(context);

      completed(context, configuration);

      return context;
    } catch (WorkflowFailedException e) {
      // User explicitly failed the workflow
      failed(context, configuration, e);
      throw e;
    } catch (RuntimeException e) {
      // Any other runtime exception - log and rethrow, stay ACTIVE
      logger.warn("Workflow {} encountered error, staying active: {}", context.getWorkflowId(), e.getMessage());
      throw e;
    }
  }


  void started(WorkflowContext context, WorkflowConfiguration<?> configuration) throws ExecutionSuspended {
    sendEvent(startedWorkflow(context, configuration.eventNameCustomizer()));
  }

  void completed(WorkflowContext context, WorkflowConfiguration<?> configuration) throws ExecutionSuspended {
    sendEvent(completedWorkflow(context, configuration.eventNameCustomizer()));
  }

  void failed(WorkflowContext context, WorkflowConfiguration<?> configuration, Exception e) throws ExecutionSuspended {
    sendEvent(failedWorkflow(context, e, configuration.eventNameCustomizer()));
  }

  private void sendEvent(EventMessage eventMessage) throws ExecutionSuspended {

    unitOfWorkFactory.create().on(ProcessingLifecycle.DefaultPhases.PRE_INVOCATION, (c) -> {
      return CompletableFuture.completedFuture("");
    });

    // block
    workflowEventAppender.appendEvent(
      eventMessage,
      null
    ).join();
  }


}
