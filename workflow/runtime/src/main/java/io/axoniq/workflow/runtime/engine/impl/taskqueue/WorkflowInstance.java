package io.axoniq.workflow.runtime.engine.impl.taskqueue;

import io.axoniq.workflow.runtime.api.primitives.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.primitives.PayloadReducer;
import io.axoniq.workflow.runtime.api.primitives.StepExecutionResult;
import io.axoniq.workflow.runtime.api.workflow.PayloadProcessor;
import io.axoniq.workflow.runtime.api.workflow.WorkflowConfiguration;
import io.axoniq.workflow.runtime.api.workflow.WorkflowContext;
import io.axoniq.workflow.runtime.api.workflow.WorkflowServices;
import io.axoniq.workflow.runtime.engine.execution.ExecutionSuspended;
import io.axoniq.workflow.runtime.engine.execution.WorkflowState;
import io.axoniq.workflow.runtime.engine.execution.WorkflowStatus;
import io.axoniq.workflow.runtime.engine.impl.ConversionDelegate;
import io.axoniq.workflow.runtime.engine.step.StepExecution;
import io.axoniq.workflow.runtime.engine.util.MetadataUtils;
import jakarta.annotation.Nonnull;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.axoniq.workflow.runtime.engine.util.EventMessageUtils.*;
import static io.axoniq.workflow.runtime.engine.util.MetadataUtils.getStepName;

public class WorkflowInstance implements WorkflowState, WorkflowContext {

  private static final Logger logger = LoggerFactory.getLogger(WorkflowInstance.class);

  private final WorkflowServices workflowServices;
  // primitive implementations
  private final ExecuteDelegate executeDelegate;
  private final WaitForDelegate waitForDelegate;
  private final ConversionDelegate conversionDelegate;

  // State variables
  private final BlockingQueue<Consumer<WorkflowState>> taskQueue = new ArrayBlockingQueue<>(1000); // FIXME
  private final Map<String, StepExecution> steps = new ConcurrentHashMap<>();
  private final Map<String, EventWaitCondition> waitConditions = new ConcurrentHashMap<>();
  private WorkflowStatus status = WorkflowStatus.NONE;
  private boolean executable = false;
  private boolean suspended = false;
  private final String workflowId;
  private final Instant startTime;
  private Map<String, Object> payload;

  public WorkflowInstance(String workflowId, Map<String, Object> initial, Instant startTime, WorkflowServices workflowServices) {
    this.workflowId = workflowId;
    this.workflowServices = workflowServices;
    this.startTime = startTime;
    this.executeDelegate = new ExecuteDelegate(this, this, workflowServices);
    this.waitForDelegate = new WaitForDelegate(this, this, workflowServices);
    this.conversionDelegate = new ConversionDelegate();
    this.payload = initial;
  }

  @Override
  public <T extends WorkflowContext> T execute(WorkflowConfiguration<T> configuration, WorkflowContext workflowContext) throws ExecutionSuspended {
    // FIXME
    switchToExecutable();
    return CompletableFuture.supplyAsync(() -> {
        @SuppressWarnings("unchecked")
        var ctx = (T) workflowContext;
        // TODO: discuss
        appendTask((i) -> sendEvent(startedWorkflow(workflowContext, configuration.eventNameCustomizer())));

        logger.trace("Executing workflow with initial payload {} from thread {}", workflowContext.getPayload(), Thread.currentThread());
        try {
          configuration.workflowDefinition().execute(ctx);
          logger.trace("Workflow executed. Resulting workflow payload {}.", workflowContext.getPayload());
          appendTask((i) -> completed(workflowContext, configuration));
        } catch (Exception we) {
          // TODO: discuss
          if (we instanceof TimeoutException) {
            appendTask(i -> sendEvent(timeoutWorkflow(workflowContext, workflowServices.getClock().instant(), configuration.eventNameCustomizer())));
          } else if (we instanceof InterruptedException) {
            appendTask(i -> sendEvent(cancelledWorkflow(workflowContext, configuration.eventNameCustomizer())));
          }
          // TODO: discuss
          appendTask(i -> sendEvent(failedWorkflow(workflowContext, we, configuration.eventNameCustomizer())));
        }

        return ctx;
      }, workflowServices.getExecutor())
      .thenApply((c) -> {
        try {
          // FIXME -> tell the coordinator to clean up and wait for final .
          // Process tasks until workflow reaches terminal status
          runNextStateChange(s -> s.getStatus().isTerminal());
        } catch (Throwable t) {
          // FIXME?
        }
        return c;
      })
      .join();

  }

  @Override
  public void applyStateChange(EventMessage eventMessage) {
    logger.trace("Applying event {}", eventMessage.type());
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

  @Override
  public void runNextStateChange(Predicate<WorkflowState> predicate) throws InterruptedException {
    do {
      taskQueue.take().accept(this);
    } while (!predicate.test(this));
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
  public boolean containsStep(String stepName) {
    return steps.containsKey(stepName);
  }

  @Override
  public void addStep(StepExecution stepExecution) {
    this.steps.put(stepExecution.stepName(), stepExecution);
  }

  @Override
  public void onEvent(EventMessage eventMessage, ProcessingContext processingContext) {
    logger.info("On event {}, wait condition size is {}", eventMessage.type(), waitConditions.size());
    for (var entry : waitConditions.entrySet()) {
      var condition = entry.getValue();
      if (eventMessage.type().qualifiedName().equals(condition.qualifiedName()) && condition.predicate().test(eventMessage)) {
        String stepName = entry.getKey();
        waitConditions.remove(stepName);
        Object payload = eventMessage.payload();
        Map<String, Object> resultMap = conversionDelegate.typeToPayloadConverter().apply(payload);
        appendTask(i -> sendEvent(completedStep(this, stepName, resultMap, condition.eventNameCustomizer())));
      }
    }
    appendTask(i -> i.applyStateChange(eventMessage));
  }

  @Override
  public String getWorkflowId() {
    return this.workflowId;
  }

  @Override
  public Map<String, Object> getPayload() {
    return this.payload;
  }

  @Override
  public WorkflowStatus getStatus() {
    return this.status;
  }

  @Override
  public List<String> getStepHistory() {
    return new ArrayList<>(steps.keySet());
  }

  @Override
  public Instant getStartTime() {
    return startTime;
  }

  // delegation
  @Override
  public StepExecutionResult execute(@NotNull String stepName, @Nullable Map<String, Object> local, @NotNull PayloadProcessor action, @NotNull PayloadReducer parameterMapping, @NotNull PayloadReducer resultMapping, @NotNull Duration timeout, @NotNull EventNameCustomizer eventNameCustomizer) {
    return executeDelegate.execute(stepName, local, action, parameterMapping, resultMapping, timeout, eventNameCustomizer);
  }

  @Override
  public StepExecutionResult waitFor(@NotNull String stepName, @NotNull QualifiedName qualifiedName, @NotNull Predicate<EventMessage> predicate, @NotNull Duration timeout, @NotNull EventNameCustomizer eventNameCustomizer) {
    return waitForDelegate.waitFor(stepName, qualifiedName, predicate, timeout, eventNameCustomizer);
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
  public Consumer<WorkflowState> getNextTask() {
    return this.taskQueue.poll();
  }

  @Override
  public void appendTask(Consumer<WorkflowState> task) {
    if (!this.taskQueue.offer(task)) {
      // whoops, we're overloading this workflow with events. STOP!!!
      throw new RuntimeException("Too many events for this workflow instance"); // FIXME <- task queue is full, backpressure?
    }
  }

  @Override
  public boolean hasTasks() {
    return this.taskQueue.isEmpty();
  }

  @Override
  public boolean isExecutable() {
    return executable;
  }

  public void suspend() {
    suspended = true;
  }

  public boolean isSuspended() {
    return suspended;
  }

  public void switchToExecutable() {
    executable = true;
  }

  void completed(WorkflowContext context, WorkflowConfiguration<?> configuration) {
    sendEvent(completedWorkflow(context, configuration.eventNameCustomizer()));
  }


  private void sendEvent(EventMessage eventMessage) {
    workflowServices.getEventSink().publish(null, eventMessage);
  }

  record EventWaitCondition(QualifiedName qualifiedName, Predicate<EventMessage> predicate,
                            EventNameCustomizer eventNameCustomizer) {
  }

  @Override
  public void registerWaitCondition(String stepName, QualifiedName qualifiedName, Predicate<EventMessage> predicate, EventNameCustomizer eventNameCustomizer) {
    waitConditions.put(stepName, new EventWaitCondition(qualifiedName, predicate, eventNameCustomizer));
  }

  @Override
  public void removeWaitCondition(String stepName) {
    waitConditions.remove(stepName);
  }

}
