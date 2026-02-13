package io.axoniq.workflow.runtime.engine.impl;

import io.axoniq.workflow.runtime.api.primitives.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.primitives.PayloadReducer;
import io.axoniq.workflow.runtime.api.primitives.WorkflowStepResult;
import io.axoniq.workflow.runtime.api.workflow.*;
import io.axoniq.workflow.runtime.engine.execution.WorkflowState;
import io.axoniq.workflow.runtime.engine.execution.WorkflowStatus;
import io.axoniq.workflow.runtime.engine.step.WorkflowStep;
import io.axoniq.workflow.runtime.engine.util.ContextUtils;
import io.axoniq.workflow.runtime.engine.util.MetadataUtils;
import jakarta.annotation.Nonnull;
import org.axonframework.common.TypeReference;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static io.axoniq.workflow.runtime.engine.impl.DefaultEventNameCustomizer.Builder.merge;
import static io.axoniq.workflow.runtime.engine.util.EventMessageUtils.*;
import static io.axoniq.workflow.runtime.engine.util.MetadataUtils.getStepName;

public class WorkflowInstance implements WorkflowState, WorkflowContext {

  private static final Logger logger = LoggerFactory.getLogger(WorkflowInstance.class);

  private final WorkflowServices workflowServices;
  // primitive implementations
  private final ExecuteDelegate executeDelegate;
  private final WaitForDelegate waitForDelegate;

  private final BlockingQueue<Consumer<WorkflowState>> taskQueue = new ArrayBlockingQueue<>(1000); // FIXME size
  // State variables
  private final Map<String, WorkflowStep> steps = new ConcurrentHashMap<>();
  private final Map<String, EventWaitCondition> waitConditions = new ConcurrentHashMap<>();
  private WorkflowStatus status = WorkflowStatus.NONE;
  private final ProcessingContext processingContext;
  private boolean executable = false;
  private final String workflowId;
  private Map<String, Object> payload;

  public WorkflowInstance(@Nonnull String workflowId,
                          @Nonnull Map<String, Object> initial,
                          @Nonnull ProcessingContext processingContext,
                          @Nonnull EventNameCustomizer parentCustomizer,
                          @Nonnull WorkflowServices workflowServices) {
    this.workflowId = Objects.requireNonNull(workflowId, "Workflow id must not be null");
    this.payload = Objects.requireNonNull(initial, "Payload must not be null");
    this.processingContext = Objects.requireNonNull(processingContext, "Processing context is mandatory");
    this.workflowServices = Objects.requireNonNull(workflowServices, "Workflow service aare mandatory");
    this.executeDelegate = new ExecuteDelegate(this, this, workflowServices, parentCustomizer);
    this.waitForDelegate = new WaitForDelegate(this, this, workflowServices, parentCustomizer);
  }


  @Override
  @Nonnull
  public <T extends WorkflowContext> T execute(
    @Nonnull WorkflowConfiguration<T> configuration,
    @Nonnull WorkflowContext workflowContext
  ) {
    // TODO: discuss when we switch to the executable
    switchToExecutable();

    return ContextUtils.executeWithResult(
        workflowId,
        workflowServices,
        workflowContext.processingContext(),
        pc -> {

          @SuppressWarnings("unchecked")
          var ctx = (T) workflowContext;
          if (ctx.getStatus().isTerminal()) {
            logger.trace("Workflow instance has reached terminal state {}, skipping execution.", ctx.getStatus());
            return CompletableFuture.completedFuture(ctx);
          }
          if (ctx.getStatus() == WorkflowStatus.NONE) {
            sendWorkflowEvent(startedWorkflow(workflowContext, configuration.eventNameCustomizer()), pc).join(); // FIXME join
          }

          try {
            logger.trace("Executing workflow with initial payload {} from thread {}", workflowContext.getPayload(), Thread.currentThread());
            configuration.workflowDefinition().execute(ctx);
            logger.trace("Workflow executed. Resulting workflow payload {}.", workflowContext.getPayload());

            sendWorkflowEvent(completedWorkflow(workflowContext, configuration.eventNameCustomizer()), pc).get(5, TimeUnit.SECONDS); // FIXME constant

          } catch (WorkflowFailedException wfe) {
            sendWorkflowEvent(failedWorkflow(workflowContext, wfe, configuration.eventNameCustomizer()), pc).join(); // FIXME join;
          } catch (Exception e) {
            if (e instanceof TimeoutException) {
              sendWorkflowEvent(timeoutWorkflow(workflowContext, workflowServices.getClock().instant(), configuration.eventNameCustomizer()), processingContext()).join(); // FIXME join
            } else if (e instanceof InterruptedException) {
              sendWorkflowEvent(cancelledWorkflow(workflowContext, configuration.eventNameCustomizer()), pc).join(); // FIXME join;
            } else {
              logger.error("Error occurred in workflow {}", workflowId, e);
            }
          }

          return CompletableFuture.completedFuture(ctx);
        }
      ).thenApply(wc -> {
        try {
          // FIXME -> tell the coordinator to clean up and wait for terminal workflow status.
          awaitStateChange(s -> s.getStatus().isTerminal());
        } catch (Exception te) {
          logger.error("Error waiting for workflow instance termination", te);
        }
        return wc;
      })
      .join();
  }

  @Override
  public void applyStateChange(
    @Nonnull EventMessage eventMessage,
    @Nonnull ProcessingContext processingContext
  ) {
    logger.trace("Applying event {}", eventMessage.type());
    Object eventPayload = eventMessage.payloadAs(Object.class);
    var metadata = eventMessage.metadata();
    // Apply step-level state changes
    MetadataUtils.getStepStatus(metadata).ifPresent(stepStatus -> {
      var stepName = getStepName(metadata);
      switch (stepStatus) {
        case STARTED:
          addStep(WorkflowStep.started(stepName, eventPayload, eventMessage.timestamp(), processingContext)); // TODO copy resources of the context
          break;
        case FAILED:
          addStep(WorkflowStep.failed(stepName, (Throwable) eventPayload, eventMessage.timestamp(), processingContext)); // TODO copy resources of the context
          break;
        case TIMED_OUT:
          addStep(WorkflowStep.timedOut(stepName, eventPayload, eventMessage.timestamp(), processingContext)); // TODO copy resources of the context
          break;
        case COMPLETED:
          addStep(WorkflowStep.completed(stepName, eventPayload, eventMessage.timestamp(), processingContext)); // TODO copy resources of the context
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
  public void applyPayloadModification(
    @Nonnull PayloadModification payloadModification
  ) {
    this.payload = Objects.requireNonNull(
      payloadModification.apply(payload),
      "Payload must not be null"
    );
  }

  @Override
  public void awaitStateChange(
    @Nonnull Predicate<WorkflowState> predicate
  ) throws InterruptedException {
    do {
      taskQueue.take().accept(this);
    } while (!predicate.test(this));
  }

  @Override
  public void onEvent(@Nonnull EventMessage eventMessage, @Nonnull ProcessingContext processingContext) {
    logger.trace("On event {}, wait condition size is {}", eventMessage.type(), waitConditions.size());
    for (var entry : waitConditions.entrySet()) {
      // TODO synchronized ?
      var condition = entry.getValue();
      if (eventMessage.type().qualifiedName().equals(condition.qualifiedName()) && condition.predicate().test(eventMessage)) {
        String stepName = entry.getKey();
        waitConditions.remove(stepName);
        Map<String, Object> resultMap = eventMessage.payloadAs(new TypeReference<>() {
        }, processingContext.component(Converter.class));
        appendTask(state ->
          ContextUtils.executeWithResult(
            stepName,
            workflowServices,
            state.getStep(stepName).context(),
            ctx ->
              workflowServices.getEventSink().publish(
                ctx,
                completedStep(this, stepName, resultMap,
                  merge(waitForDelegate.parentEventNameCustomizer, condition.eventNameCustomizer())
                )
              )
          ).join()
        );
      }
    }
    appendTask(i -> i.applyStateChange(eventMessage, processingContext));
  }


  // delegation
  @Override
  @Nonnull
  public WorkflowStepResult execute(@Nonnull String stepName, @Nullable Map<String, Object> local, @Nonnull PayloadProcessor action, @Nonnull PayloadReducer parameterMapping, @Nonnull PayloadReducer resultMapping, @Nonnull Duration timeout, @Nonnull EventNameCustomizer eventNameCustomizer) {
    return executeDelegate.execute(stepName, local, action, parameterMapping, resultMapping, timeout, eventNameCustomizer);
  }

  @Override
  @Nonnull
  public WorkflowStepResult waitFor(@Nonnull String stepName, @Nonnull QualifiedName qualifiedName, @Nonnull Predicate<EventMessage> predicate, @Nonnull Duration timeout, @Nonnull EventNameCustomizer eventNameCustomizer) {
    return waitForDelegate.waitFor(stepName, qualifiedName, predicate, timeout, eventNameCustomizer);
  }

  @Override
  @Nullable
  public Consumer<WorkflowState> getNextTask() {
    return this.taskQueue.poll(); // FIXME: forever?
  }

  @Override
  public void appendTask(Consumer<WorkflowState> task) {
    if (!this.taskQueue.offer(task)) {
      // whoops, we're overloading this workflow with events. STOP!!!
      throw new RuntimeException("Too many events for this workflow instance"); // FIXME <- task queue is full, backpressure?
    }
  }

  @Override
  public WorkflowStep getStep(String stepName) {
    return steps.get(stepName);
  }

  @Override
  public boolean containsStep(String stepName) {
    return steps.containsKey(stepName);
  }

  @Override
  public void addStep(WorkflowStep workflowStep) {
    this.steps.put(workflowStep.stepName(), workflowStep);
  }

  @Override
  public void registerWaitCondition(@Nonnull String stepName, @Nonnull QualifiedName qualifiedName, @Nonnull Predicate<EventMessage> predicate, @Nonnull EventNameCustomizer eventNameCustomizer) {
    waitConditions.put(stepName, new EventWaitCondition(qualifiedName, predicate, eventNameCustomizer));
  }

  @Override
  public void removeWaitCondition(@Nonnull String stepName) {
    waitConditions.remove(stepName);
  }


  @Override
  @Nonnull
  public String getWorkflowId() {
    return this.workflowId;
  }

  @Override
  @Nonnull
  public Map<String, Object> getPayload() {
    return this.payload;
  }

  @Override
  @Nonnull
  public WorkflowStatus getStatus() {
    return this.status;
  }

  @Override
  @Nonnull
  public List<String> getStepHistory() {
    return new ArrayList<>(steps.keySet());
  }

  @Override
  public boolean hasTasks() {
    return this.taskQueue.isEmpty();
  }

  @Override
  public boolean isExecutable() {
    return executable;
  }

  public void switchToExecutable() {
    executable = true;
  }


  private CompletableFuture<Void> sendWorkflowEvent(EventMessage eventMessage, ProcessingContext processingContext) {
    // TODO: make sure the consistency marker is used
    return workflowServices.getEventSink().publish(processingContext, eventMessage);
  }

  record EventWaitCondition(QualifiedName qualifiedName, Predicate<EventMessage> predicate,
                            EventNameCustomizer eventNameCustomizer) {
  }


  @Override
  @Nonnull
  public ProcessingContext processingContext() {
    return processingContext;
  }

  @Override
  public void describeTo(@Nonnull ComponentDescriptor descriptor) {
    // FIXME
  }
}
