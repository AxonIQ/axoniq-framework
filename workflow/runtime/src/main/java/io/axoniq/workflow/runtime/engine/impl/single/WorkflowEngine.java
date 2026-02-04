package io.axoniq.workflow.runtime.engine.impl.single;

import io.axoniq.workflow.runtime.api.workflow.WorkflowConfiguration;
import io.axoniq.workflow.runtime.api.workflow.WorkflowContext;
import io.axoniq.workflow.runtime.api.workflow.WorkflowServices;
import io.axoniq.workflow.runtime.engine.execution.WorkflowState;
import io.axoniq.workflow.runtime.engine.execution.WorkflowStatus;
import io.axoniq.workflow.runtime.engine.impl.ConversionDelegate;
import io.axoniq.workflow.runtime.engine.impl.multi.EventSubscriptionManager;
import io.axoniq.workflow.runtime.engine.impl.multi.TaskManager;
import io.axoniq.workflow.runtime.engine.impl.multi.WorkflowEventAppender;
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

  private final Map<String, ExecutionHandle> workflowInstances = new ConcurrentHashMap<>();


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
    if (MetadataUtils.hasWorkflowId().test(event.metadata())) {
      logger.info("Updating workflow with '{}'", event.type().qualifiedName());
      // state update
      var workflowId = MetadataUtils.getWorkflowId(event.metadata());
      // append task
      workflowInstances.get(workflowId).workflowState.appendTask(
        (w) -> w.applyStateChange(event)
      );
    } else {
      // handle starting of new processes
      checkAndCreateNewWorkflow(event, context);
    }

    return MessageStream.empty();
  }

  /**
   * This is a place to be called from Event Processor
   */
  public void runWorkflows() {
    logger.debug("Starting {} workflows.", workflowInstances.size());
    for (var handle : workflowInstances.values()) {
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

  public Map<String, ExecutionHandle> workflowInstances() {
    return this.workflowInstances;
  }

  public record ExecutionHandle(WorkflowConfiguration<?> workflowConfiguration, WorkflowContext workflowContext,
                         WorkflowState workflowState) {

    public WorkflowStatus getStatus() {
      return workflowContext.getStatus();
    }

  }

}
