package io.axoniq.workflow.runtime.engine.impl.threadsandfutures;

import io.axoniq.workflow.runtime.api.primitives.*;
import io.axoniq.workflow.runtime.api.workflow.*;
import io.axoniq.workflow.runtime.engine.execution.ExecutionSuspended;
import io.axoniq.workflow.runtime.engine.execution.WorkflowState;
import io.axoniq.workflow.runtime.engine.execution.WorkflowStatus;
import io.axoniq.workflow.runtime.engine.impl.ConversionDelegate;
import io.axoniq.workflow.runtime.engine.step.StepExecution;
import io.axoniq.workflow.runtime.engine.util.MetadataUtils;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.ProcessingLifecycle;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.axoniq.workflow.runtime.engine.util.EventMessageUtils.*;
import static io.axoniq.workflow.runtime.engine.util.MetadataUtils.getStepName;

public class WorkflowInstance implements WorkflowContext, WorkflowState {

  private static final Logger logger = LoggerFactory.getLogger(WorkflowInstance.class);
  private final String workflowId;
  private final Map<String, StepExecution> steps = new ConcurrentHashMap<>();
  private Map<String, Object> payload;
  private WorkflowStatus status = WorkflowStatus.NONE;
  private final Instant startTime;

  // primitive implementations
  private final transient ExecutePrimitive executePrimitive;
  private final transient WaitForPrimitive waitForPrimitive;
  private final transient ConversionDelegate conversionDelegate = new ConversionDelegate();

  private final WorkflowServices workflowServices;


  public WorkflowInstance(
    @Nonnull String workflowId,
    @Nonnull Map<String, Object> payload,
    @Nonnull Instant startTime,
    @Nonnull WorkflowServices workflowServices
  ) {
    this.workflowId = workflowId;
    this.payload = payload;
    this.workflowServices = workflowServices;
    this.executePrimitive = new ExecuteDelegate(this, this, workflowServices);
    this.waitForPrimitive = new WaitForDelegate(this, this, workflowServices);
    this.startTime = startTime;
  }

  @Override
  public StepExecutionResult execute(
    @Nonnull String stepName,
    @Nullable Map<String, Object> local,
    @Nonnull PayloadProcessor action,
    @Nonnull PayloadReducer parameterMapping,
    @Nonnull PayloadReducer resultMapping,
    @Nonnull Duration timeout,
    @Nonnull EventNameCustomizer eventNameCustomizer
  ) {
    return executePrimitive.execute(stepName, local, action, parameterMapping, resultMapping, timeout, eventNameCustomizer);
  }

  @Override
  public StepExecutionResult waitFor(
    @Nonnull String stepName,
    @Nonnull QualifiedName qualifiedName,
    @Nonnull Predicate<EventMessage> predicate,
    @Nonnull Duration timeout,
    @Nonnull EventNameCustomizer eventNameCustomizer
  ) {
    return waitForPrimitive.waitFor(stepName, qualifiedName, predicate, timeout, eventNameCustomizer);
  }

  @Override
  public Function<Object, Map<String, Object>> typeToPayloadConverter() {
    return conversionDelegate.typeToPayloadConverter();
  }

  @Override
  public <T> Function<Map<String, Object>, T> payloadToTypeConverter(@Nonnull Class<T> type) {
    return conversionDelegate.payloadToTypeConverter(type);
  }

  @Override
  public String getWorkflowId() {
    return workflowId;
  }

  @Override
  public Map<String, Object> getPayload() {
    return payload;
  }

  @Override
  public void applyPayloadModification(PayloadProcessor payloadModification) {
    this.payload = Objects.requireNonNull(payloadModification.apply(payload), "Payload must not be null");
  }

  @Override
  public StepExecution getStep(String stepName) {
    return steps.get(stepName);
  }

  @Override
  public WorkflowStatus getStatus() {
    return status;
  }

  @Override
  public List<String> getStepHistory() {
    return steps.values().stream().map(StepExecution::stepName).toList();
  }

  @Override
  public Instant getStartTime() {
    return startTime;
  }

  @Override
  public void onEvent(EventMessage eventMessage, ProcessingContext processingContext) {
    applyStateChange(eventMessage);
  }

  @Override
  public void addStep(StepExecution execution) {
    steps.put(execution.stepName(), execution);
  }

  @Override
  public <T extends WorkflowContext> T execute(
    WorkflowConfiguration<T> configuration,
    WorkflowContext workflowContext
  ) throws ExecutionSuspended {
    return CompletableFuture.supplyAsync(() -> {
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
      }, workflowServices.getExecutor())
      .join();
  }

  void started(WorkflowContext context, WorkflowConfiguration<?> configuration) {
    sendEvent(startedWorkflow(context, configuration.eventNameCustomizer()));
  }

  void completed(WorkflowContext context, WorkflowConfiguration<?> configuration) {
    sendEvent(completedWorkflow(context, configuration.eventNameCustomizer()));
  }

  void failed(WorkflowContext context, WorkflowConfiguration<?> configuration, Exception e) {
    sendEvent(failedWorkflow(context, e, configuration.eventNameCustomizer()));
  }

  private void sendEvent(EventMessage eventMessage) {

    workflowServices.getUnitOfWorkFactory().create().on(ProcessingLifecycle.DefaultPhases.PRE_INVOCATION, (c) -> {
      return CompletableFuture.completedFuture("");
    });

    // block
    workflowServices.getWorkflowEventAppender().appendEvent(
      eventMessage,
      null
    ).join();
  }

  @Override
  public void applyStateChange(EventMessage eventMessage) {
    Object eventPayload = eventMessage.payloadAs(Object.class);
    var metadata = eventMessage.metadata();
    // Apply step-level state changes
    MetadataUtils.getStepStatus(metadata).ifPresent(stepStatus -> {
      var stepName = getStepName(metadata);
      switch (stepStatus) {
        case STARTED:
          addStep(StepExecution.started(stepName, eventPayload, eventMessage.timestamp()));
          break;
        case FAILED:
          addStep(StepExecution.failed(stepName, (Throwable) eventPayload, eventMessage.timestamp()));
          break;
        case TIMED_OUT:
          addStep(StepExecution.timedOut(stepName, eventPayload, eventMessage.timestamp()));
          break;
        case COMPLETED:
          addStep(StepExecution.completed(stepName, eventPayload, eventMessage.timestamp()));
          break;
        default:
          break;
      }
    });

    // Apply workflow-level state changes
    MetadataUtils.getWorkflowStatus(metadata).ifPresent(status ->
      this.status = status
    );
  }

  /*
 Unused in this implementation
 */
  @Override
  public void awaitStateChange(Predicate<WorkflowState> predicate) throws InterruptedException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean containsStep(String stepName) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void appendTask(Consumer<WorkflowState> task) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public Consumer<WorkflowState> getNextTask() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean isExecutable() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean hasTasks() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void registerWaitCondition(String stepName, QualifiedName qualifiedName, Predicate<EventMessage> predicate, EventNameCustomizer eventNameCustomizer) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void removeWaitCondition(String stepName) {
    throw new UnsupportedOperationException("Not implemented");
  }
}
