package io.axoniq.workflow.runtime.engine.impl.single;

import io.axoniq.workflow.runtime.api.primitives.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.primitives.PayloadReducer;
import io.axoniq.workflow.runtime.api.primitives.StepExecutionResult;
import io.axoniq.workflow.runtime.api.workflow.PayloadProcessor;
import io.axoniq.workflow.runtime.api.workflow.WorkflowContext;
import io.axoniq.workflow.runtime.api.workflow.WorkflowDefinition;
import io.axoniq.workflow.runtime.api.workflow.WorkflowServices;
import io.axoniq.workflow.runtime.engine.execution.WorkflowState;
import io.axoniq.workflow.runtime.engine.execution.WorkflowStatus;
import io.axoniq.workflow.runtime.engine.impl.multi.ConversionDelegate;
import io.axoniq.workflow.runtime.engine.step.StepExecution;
import io.axoniq.workflow.runtime.engine.util.MetadataUtils;
import jakarta.annotation.Nonnull;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.axoniq.workflow.runtime.engine.util.MetadataUtils.getStepName;

public class WorkflowInstance implements WorkflowState, WorkflowContext {

  private final WorkflowServices workflowServices;
  private final ExecuteDelegate executeDelegate;
  private final WaitForDelegate waitForDelegate;
  private final ConversionDelegate conversionDelegate;

  // State variables
  private final BlockingQueue<Consumer<WorkflowState>> taskQueue = new ArrayBlockingQueue<>(1000);
  private final Map<String, StepExecution> steps = new ConcurrentHashMap<>();
  private final String workflowId;

  private Map<String, Object> payload;
  private boolean executable = false;
  private boolean suspended = false;
  private WorkflowStatus status = WorkflowStatus.NONE;

  public WorkflowInstance(String workflowId, Map<String, Object> initial, WorkflowServices workflowServices) {
    this.workflowId = workflowId;
    this.workflowServices = workflowServices;
    this.executeDelegate = new ExecuteDelegate(this, this, workflowServices);
    this.waitForDelegate = new WaitForDelegate(this, this, workflowServices);
    this.conversionDelegate = new ConversionDelegate();
    this.payload = initial;
  }

  public void execute(WorkflowDefinition<WorkflowContext> workflowDefinition) {
    CompletableFuture.runAsync(() -> {
      workflowDefinition.execute(this);
    }, workflowServices.getExecutor());
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

  @Override
  public void runNextStateChange() throws InterruptedException {
    taskQueue.take().accept(this);
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
    if (!taskQueue.offer(i -> i.applyStateChange(eventMessage))) {
      // whoops, we're overloading this workflow with events. STOP!!!
      throw new RuntimeException("Too many events for this workflow instance");
    }
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
  public boolean appendTask(Consumer<WorkflowState> task) {
    return this.taskQueue.offer(task);
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

  @Override
  public boolean isExecutable() {
    return executable;
  }

  @Override
  public boolean hasTasks() {
    return this.taskQueue.isEmpty();
  }
}
