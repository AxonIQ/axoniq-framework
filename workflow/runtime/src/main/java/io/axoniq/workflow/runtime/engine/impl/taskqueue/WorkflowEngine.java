package io.axoniq.workflow.runtime.engine.impl.taskqueue;

import io.axoniq.workflow.runtime.api.workflow.WorkflowConfiguration;
import io.axoniq.workflow.runtime.api.workflow.WorkflowContext;
import io.axoniq.workflow.runtime.api.workflow.WorkflowServices;
import io.axoniq.workflow.runtime.engine.execution.WorkflowState;
import io.axoniq.workflow.runtime.engine.execution.WorkflowStatus;
import io.axoniq.workflow.runtime.engine.impl.ConversionDelegate;
import io.axoniq.workflow.runtime.engine.impl.threadsandfutures.EventSubscriptionManager;
import io.axoniq.workflow.runtime.engine.impl.threadsandfutures.TaskManager;
import io.axoniq.workflow.runtime.engine.impl.threadsandfutures.WorkflowEventAppender;
import io.axoniq.workflow.runtime.engine.registry.WorkflowRepository;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class WorkflowEngine implements EventHandler, WorkflowServices {

  private final Logger logger = LoggerFactory.getLogger(this.getClass());

  private final WorkflowRepository<?> workflowRepository;
  private final EventSink eventSink;
  private final Clock clock;
  private final Executor executor;
  private final UnitOfWorkFactory unitOfWorkFactory;

  private final ConversionDelegate conversionDelegate = new ConversionDelegate();

  private final Map<String, ExecutionHandle> executionHandles = new ConcurrentHashMap<>();


  public WorkflowEngine(
    @Nonnull UnitOfWorkFactory unitOfWorkFactory,
    @Nonnull EventSink eventSink,
    @Nonnull WorkflowRepository<?> workflowRepository
  ) {
    this.eventSink = eventSink;
    this.workflowRepository = workflowRepository;
    this.clock = Clock.systemDefaultZone();
    this.executor = Executors.newVirtualThreadPerTaskExecutor();
    this.unitOfWorkFactory = unitOfWorkFactory;
  }

  @NotNull
  @Override
  public MessageStream.Empty<Message> handle(@NotNull EventMessage event, @NotNull ProcessingContext context) {
    logger.trace("Received event {}", event.type());
    if (MetadataUtils.hasWorkflowId().test(event.metadata())) {
      var workflowId = MetadataUtils.getWorkflowId(event.metadata());
      // TODO: discussion regarding hibernating workflows ->
      // TODO: is it safe to put an event in the queue?
      executionHandles.get(workflowId).workflowState.onEvent(event, context);
    } else {
      // handle starting of new processes
      checkAndCreateNewWorkflow(event, context);
      // route external events to workflows waiting for them
      for (var handle : executionHandles.values()) {
        // TODO: discussion regarding hibernating workflows ->
        // TODO: is it safe to put an event in the queue?
        handle.workflowState.onEvent(event, context);
      }
    }

    return MessageStream.empty();
  }

  /**
   * This is a place to be called from Event Processor
   */
  public void runWorkflows() {
    logger.debug("Executing {} workflows.", executionHandles.size());
    for (var handle : executionHandles.values()) {
      try {
        handle.workflowState.execute(handle.workflowConfiguration, handle.workflowContext);
      } catch (Throwable t) {
        throw new RuntimeException(t);
      }
    }
  }


  private void checkAndCreateNewWorkflow(EventMessage event, ProcessingContext context) {
    var definitions = workflowRepository.getWorkflowsConfigurations(event.type().qualifiedName());
    definitions.forEach(
      workflowConfiguration -> {
        var payload = conversionDelegate.typeToPayloadConverter().apply(event.payload());

        var workflowContext = workflowConfiguration.workflowContextFactory().createContext(payload, event.timestamp(), this);
        var workflowId = workflowContext.getWorkflowId();

        // avoid multiple workflows for the same workflow id.
        executionHandles.computeIfAbsent(workflowId, (id) -> {
          logger.info("Starting new workflow with '{}'", event.payload());
          var workflowState = workflowConfiguration.workflowStateFactory().create(workflowContext);
          return new ExecutionHandle(workflowConfiguration, workflowContext, workflowState);
        });
      }
    );
  }

  public Map<String, ExecutionHandle> workflowInstances() {
    return this.executionHandles;
  }

  public record ExecutionHandle(
    WorkflowConfiguration<?> workflowConfiguration,
    WorkflowContext workflowContext,
    WorkflowState workflowState)
  {

    public WorkflowStatus getStatus() {
      return workflowContext.getStatus();
    }

  }

  @Override
  public Clock getClock() {
    return clock;
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
    return eventSink;
  }

  public void shutdown() {

  }

  /*
   Not used in this implementation.
   */

  @Override
  public EventSubscriptionManager getEventSubscriptionManager() {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public WorkflowEventAppender getWorkflowEventAppender() {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public TaskManager getTaskManager() {
    throw new UnsupportedOperationException("Not implemented yet");
  }


}
