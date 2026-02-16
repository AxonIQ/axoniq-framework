package io.axoniq.workflow.runtime.engine.impl;

import io.axoniq.workflow.runtime.api.WorkflowConfiguration;
import io.axoniq.workflow.runtime.api.WorkflowContext;
import io.axoniq.workflow.runtime.api.WorkflowDefinitionRegistry;
import io.axoniq.workflow.runtime.api.WorkflowServices;
import io.axoniq.workflow.runtime.engine.execution.WorkflowState;
import io.axoniq.workflow.runtime.engine.execution.WorkflowStatus;
import io.axoniq.workflow.runtime.engine.util.MetadataUtils;
import jakarta.annotation.Nonnull;
import org.axonframework.common.TypeReference;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventHandler;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventSink;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class WorkflowEngine implements EventHandler, WorkflowServices {

  private final Logger logger = LoggerFactory.getLogger(this.getClass());

  private final WorkflowDefinitionRegistry<?> workflowDefinitionRegistry;
  private final EventSink eventSink;
  private final Clock clock;
  private final Executor executor;
  private final UnitOfWorkFactory unitOfWorkFactory;
  private final Converter converter;

  // FIXME -> offload it from here to some kind of a "store"
  // key => workflowId
  // value => configuration/context/state
  // currently it holds all instances, running and historic
  private final Map<String, ExecutionHandle> executionHandles = new ConcurrentHashMap<>();


  public WorkflowEngine(
    @Nonnull UnitOfWorkFactory unitOfWorkFactory,
    @Nonnull EventSink eventSink,
    @Nonnull WorkflowDefinitionRegistry<?> workflowDefinitionRegistry,
    @Nonnull Converter converter
  ) {
    this.eventSink = eventSink;
    this.workflowDefinitionRegistry = workflowDefinitionRegistry;
    this.clock = GenericEventMessage.clock;
    this.executor = Executors.newVirtualThreadPerTaskExecutor();
    this.unitOfWorkFactory = unitOfWorkFactory;
    this.converter = converter;
  }

  @NotNull
  @Override
  public MessageStream.Empty<Message> handle(@NotNull EventMessage eventMessage,
                                             @NotNull ProcessingContext processingContext) {
    logger.trace("Received eventMessage {}", eventMessage.type());
    if (MetadataUtils.hasWorkflowId().test(eventMessage.metadata())) {
      var workflowId = MetadataUtils.getWorkflowId(eventMessage.metadata());
      // TODO: discussion regarding hibernating workflows ->
      // TODO: is it safe to put an eventMessage in the queue?
      executionHandles.get(workflowId).workflowState.onEvent(eventMessage, processingContext);
    } else {
      // handle starting of new processes
      checkAndCreateNewWorkflow(eventMessage, processingContext);
      // route external events to workflows waiting for them
      for (var handle : executionHandles.values()) {
        // TODO: discussion regarding hibernating workflows ->
        // TODO: is it safe to put an eventMessage in the queue?
        handle.workflowState.onEvent(eventMessage, processingContext);
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
        throw new RuntimeException("Error during workflow execution", t);
      }
    }
  }

  private void checkAndCreateNewWorkflow(@Nonnull EventMessage eventMessage,
                                         @Nonnull ProcessingContext processingContext) {
    var definitions = workflowDefinitionRegistry.getWorkflowsConfigurations(eventMessage.type().qualifiedName());
    definitions.forEach(predicatedWorkflowConfiguration -> {

        if (predicatedWorkflowConfiguration.predicate().test(eventMessage)) {

          var workflowConfiguration = predicatedWorkflowConfiguration.configuration();

          var payload = Objects.requireNonNull(eventMessage.payloadAs(
            new TypeReference<Map<String, Object>>() {
            },
            processingContext.component(Converter.class)
          ), "Error converting initial payload");
          var workflowId = workflowConfiguration.associationProvider().apply(payload)
            .orElseThrow(() -> new IllegalArgumentException(
              "Could not extract workflow id from payload " + payload + " for workflow definition " + workflowConfiguration.workflowDefinition())
            );

          var workflowContext = workflowConfiguration.workflowContextFactory()
            .createContext(payload, workflowId, processingContext, this);

          // avoid multiple workflows for the same workflow id.
          executionHandles.computeIfAbsent(workflowId, (id) -> {
            logger.info("Starting new workflow with '{}'", eventMessage.payload());
            var workflowState = workflowConfiguration.workflowStateFactory().create(workflowContext);
            return new ExecutionHandle(workflowConfiguration, workflowContext, workflowState);
          });
        }
      }
    );
  }

  public Map<String, ExecutionHandle> workflowInstances() {
    return this.executionHandles;
  }

  public void shutdown() {
    this.workflowInstances().clear();
  }

  public record ExecutionHandle(
    WorkflowConfiguration<?> workflowConfiguration,
    WorkflowContext workflowContext,
    WorkflowState workflowState) {

    public WorkflowStatus getStatus() {
      return workflowContext.getStatus();
    }

  }

  @Nonnull
  @Override
  public Clock getClock() {
    return clock;
  }

  @Nonnull
  @Override
  public UnitOfWorkFactory getUnitOfWorkFactory() {
    return unitOfWorkFactory;
  }

  @Nonnull
  @Override
  public Executor getExecutor() {
    return executor;
  }

  @Nonnull
  @Override
  public EventSink getEventSink() {
    return eventSink;
  }

  @Nonnull
  @Override
  public Converter getConverter() {
    return converter;
  }
}
