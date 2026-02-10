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
import io.axoniq.workflow.runtime.engine.util.ContextUtils;
import io.axoniq.workflow.runtime.engine.util.MetadataUtils;
import jakarta.annotation.Nonnull;
import org.axonframework.common.TypeReference;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWork;
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

  private final BlockingQueue<Consumer<WorkflowState>> taskQueue = new ArrayBlockingQueue<>(1000); // FIXME size
  // State variables
  private final Map<String, StepExecution> steps = new ConcurrentHashMap<>();
  private final Map<String, EventWaitCondition> waitConditions = new ConcurrentHashMap<>();
  private WorkflowStatus status = WorkflowStatus.NONE;
  private final ProcessingContext processingContext;
  private boolean executable = false;
  private boolean suspended = false;
  private final String workflowId;
  private final Instant startTime;
  private Map<String, Object> payload;

  public WorkflowInstance(@Nonnull String workflowId,
                          @Nonnull Map<String, Object> initial,
                          @Nonnull Instant startTime,
                          @Nonnull ProcessingContext processingContext,
                          @Nonnull WorkflowServices workflowServices) {
    this.workflowId = workflowId;
    this.workflowServices = workflowServices;
    this.startTime = startTime;
    this.processingContext = processingContext;
    this.executeDelegate = new ExecuteDelegate(this, this, workflowServices);
    this.waitForDelegate = new WaitForDelegate(this, this, workflowServices);
    this.conversionDelegate = new ConversionDelegate();
    this.payload = initial;
  }

  @Override
  public ProcessingContext processingContext() {
    return processingContext;
  }

  @Override
  public <T extends WorkflowContext> T execute(WorkflowConfiguration<T> configuration, WorkflowContext workflowContext) throws ExecutionSuspended {
    // TODO: discuss when we switch to the executable
    switchToExecutable();
    UnitOfWork uow = workflowServices.getUnitOfWorkFactory()
      .create(workflowId, customize -> customize.workScheduler(workflowServices.getExecutor()));
    return uow.executeWithResult(processingContext1 -> {
        var processingContext2 = ContextUtils.copyResources(workflowContext.processingContext(), processingContext1);

        @SuppressWarnings("unchecked")
        var ctx = (T) workflowContext;
        // FIXME -> if terminal -> finish execution
        // FIXME -> if none -> start
        sendWorkflowEvent(startedWorkflow(workflowContext, configuration.eventNameCustomizer()), processingContext2).join(); // FIXME join

        logger.trace("Executing workflow with initial payload {} from thread {}", workflowContext.getPayload(), Thread.currentThread());
        try {
          configuration.workflowDefinition().execute(ctx);
          logger.trace("Workflow executed. Resulting workflow payload {}.", workflowContext.getPayload());
          sendWorkflowEvent(completedWorkflow(workflowContext, configuration.eventNameCustomizer()), processingContext2).get(5, TimeUnit.SECONDS); // FIXME constant
        } catch (Exception we) {
          if (we instanceof TimeoutException) {
            sendWorkflowEvent(timeoutWorkflow(workflowContext, workflowServices.getClock().instant(), configuration.eventNameCustomizer()), processingContext()).join(); // FIXME join
          } else if (we instanceof InterruptedException) {
            sendWorkflowEvent(cancelledWorkflow(workflowContext, configuration.eventNameCustomizer()), processingContext2).join(); // FIXME join;
          }
          // FIXME -> check for `WorkflowFailedException`
          sendWorkflowEvent(failedWorkflow(workflowContext, we, configuration.eventNameCustomizer()), processingContext2).join(); // FIXME join;
        }
        return CompletableFuture.completedFuture(ctx);
      })
      .thenApply((c) -> {
        try {
          // FIXME -> tell the coordinator to clean up and wait for terminal workflow status.
          // Process tasks until workflow reaches terminal status
          awaitStateChange(s -> s.getStatus().isTerminal());
        } catch (Throwable t) {
          // FIXME? discuss if we can react to this
        }
        return c;
      })
      .join();
  }

  @Override
  public void applyStateChange(EventMessage eventMessage, ProcessingContext processingContext) {
    logger.trace("Applying event {}", eventMessage.type());
    Object eventPayload = eventMessage.payloadAs(Object.class);
    var metadata = eventMessage.metadata();
    // Apply step-level state changes
    MetadataUtils.getStepStatus(metadata).ifPresent(stepStatus -> {
      var stepName = getStepName(metadata);
      switch (stepStatus) {
        case STARTED:
          addStep(StepExecution.started(stepName, eventPayload, eventMessage.timestamp(), processingContext)); // TODO copy resources of the context
          break;
        case FAILED:
          addStep(StepExecution.failed(stepName, (Throwable) eventPayload, eventMessage.timestamp(), processingContext)); // TODO copy resources of the context
          break;
        case TIMED_OUT:
          addStep(StepExecution.timedOut(stepName, eventPayload, eventMessage.timestamp(), processingContext)); // TODO copy resources of the context
          break;
        case COMPLETED:
          addStep(StepExecution.completed(stepName, eventPayload, eventMessage.timestamp(), processingContext)); // TODO copy resources of the context
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
  public void applyPayloadModification(PayloadProcessor payloadModification) {
    this.payload = Objects.requireNonNull(payloadModification.apply(payload), "Payload must not be null");
  }

  @Override
  public void awaitStateChange(Predicate<WorkflowState> predicate) throws InterruptedException {
    do {
      taskQueue.take().accept(this);
    } while (!predicate.test(this));
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
      // synchronized ?
      var condition = entry.getValue();
      if (eventMessage.type().qualifiedName().equals(condition.qualifiedName()) && condition.predicate().test(eventMessage)) {
        String stepName = entry.getKey();
        waitConditions.remove(stepName);
        Map<String, Object> resultMap = eventMessage.payloadAs(new TypeReference<>() {
        }, processingContext.component(Converter.class));
        appendTask(state -> sendWorkflowEvent(completedStep(this, stepName, resultMap, condition.eventNameCustomizer()), state.processingContext()));
      }
    }
    appendTask(i -> i.applyStateChange(eventMessage, processingContext));
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


  private CompletableFuture<Void> sendWorkflowEvent(EventMessage eventMessage, ProcessingContext processingContext) {
    // TODO: make sure the consistency marker is used
    return workflowServices.getEventSink()
      .publish(processingContext, eventMessage);
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
