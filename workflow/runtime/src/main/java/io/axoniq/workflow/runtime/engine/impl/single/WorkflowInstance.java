package io.axoniq.workflow.runtime.engine.impl.single;

import io.axoniq.workflow.runtime.api.primitives.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.primitives.PayloadReducer;
import io.axoniq.workflow.runtime.engine.result.StateBasedStepExecutionResult;
import io.axoniq.workflow.runtime.api.primitives.StepExecutionResult;
import io.axoniq.workflow.runtime.api.workflow.PayloadProcessor;
import io.axoniq.workflow.runtime.api.workflow.WorkflowContext;
import io.axoniq.workflow.runtime.api.workflow.WorkflowDefinition;
import io.axoniq.workflow.runtime.engine.execution.WorkflowState;
import io.axoniq.workflow.runtime.engine.execution.WorkflowStatus;
import io.axoniq.workflow.runtime.engine.step.StepExecution;
import io.axoniq.workflow.runtime.engine.step.StepStatus;
import io.axoniq.workflow.runtime.engine.util.EventMessageUtils;
import io.axoniq.workflow.runtime.engine.util.MetadataUtils;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.axoniq.workflow.runtime.engine.util.MetadataUtils.getStepName;

public class WorkflowInstance implements WorkflowState {

    private final BlockingQueue<Consumer<WorkflowInstance>> taskQueue = new ArrayBlockingQueue<>(1000);
    private final Map<String, StepExecution> steps = new ConcurrentHashMap<>();

    private boolean executable = false;
    private boolean suspended = false;
    private WorkflowStatus status = WorkflowStatus.NONE;

    public WorkflowInstance() {
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

    public void applyStateChange(EventMessage eventMessage) {
        Object eventPayload = eventMessage.payloadAs(Object.class);
        var metadata = eventMessage.metadata();
        // Apply step-level state changes
        MetadataUtils.getStepStatus(metadata).ifPresent(stepStatus -> {
            var stepName = getStepName(metadata);
            switch (stepStatus) {
                case STARTED:
                    steps.put(stepName, StepExecution.started(stepName, eventPayload, eventMessage.timestamp()));
                    break;
                case FAILED:
                    steps.put(stepName, StepExecution.failed(stepName, (Throwable) eventPayload, eventMessage.timestamp()));
                    break;
                case TIMED_OUT:
                    steps.put(stepName, StepExecution.timedOut(stepName, eventPayload, eventMessage.timestamp()));
                    break;
                case COMPLETED:
                    steps.put(stepName, StepExecution.completed(stepName, eventPayload,eventMessage.timestamp()));
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

    public void runNextStateChange() throws InterruptedException {
        taskQueue.take().accept(this);
    }

    public void execute(WorkflowDefinition<WorkflowContext> workflowDefinition, EventSink eventSink) {
        // Start a thread
        workflowDefinition.execute(new WorkflowContext() {
            @Override
            public String getWorkflowId() {
                return "";
            }

            @Override
            public Map<String, Object> getPayload() {
                return Map.of();
            }

            @Override
            public WorkflowStatus getStatus() {
                return null;
            }

            @Override
            public List<String> getStepHistory() {
                return List.of();
            }

            @Override
            public Function<Object, Map<String, Object>> typeToPayloadConverter() {
                return null;
            }

            @Override
            public <T> Function<Map<String, Object>, T> payloadToTypeConverter(@NotNull Class<T> payloadType) {
                return null;
            }

            @Override
            public StepExecutionResult execute(@NotNull String stepName, @Nullable Map<String, Object> local, @NotNull PayloadProcessor action, @NotNull PayloadReducer parameterMapping, @NotNull PayloadReducer resultMapping, @NotNull Duration timeout, @NotNull EventNameCustomizer eventNameCustomizer) {
                while ((!steps.containsKey(stepName) && !taskQueue.isEmpty()) || !executable) {
                    Consumer<WorkflowInstance> poll = taskQueue.poll();
                    if (poll != null) {
                        poll.accept(WorkflowInstance.this);
                    }
                }
                if (steps.get(stepName) == null) {
                    steps.put(stepName, new StepExecution(stepName, StepStatus.STARTED, null, null, getClock().instant()));
                    // run on ExecutorService
                }
                if (steps.get(stepName).status() == StepStatus.STARTED) {
                    Instant actualStartTime = steps.get(stepName).timestamp();
                    Duration remainingTimeout = Duration.between(Instant.now(getClock()), actualStartTime.plus(timeout));
                    // FIXME - This is where we capture our current consistency marker
                    CompletableFuture<Map<String, Object>> result = CompletableFuture.supplyAsync(() -> action.apply(local));
                    if (remainingTimeout.isNegative()) {
                        taskQueue.offer(i -> {
                            // TODO - Do one last check on the state to make sure we didn't have any concurrent state changes
                            // FIXME - This is where we should publish using an append condition
                            eventSink.publish(null, EventMessageUtils.timeoutStep(this, stepName, i.getClock().instant(), eventNameCustomizer));
                        });
                    } else {
                        result.orTimeout(remainingTimeout.toMillis(), TimeUnit.MILLISECONDS)
                              .whenComplete((r, e) -> {
                                  if (r != null) {
                                      taskQueue.offer(i -> {
                                          // FIXME - This is where we should publish using an append condition
                                          eventSink.publish(null, EventMessageUtils.completedStep(this, stepName, r, eventNameCustomizer));
                                      });
                                  } else {
                                      if (e instanceof TimeoutException) {
                                          // FIXME - This is where we should publish using an append condition
                                          eventSink.publish(null, EventMessageUtils.timeoutStep(this, stepName, Instant.now(), eventNameCustomizer));
                                      } else {
                                          // FIXME - This is where we should publish using an append condition
                                          eventSink.publish(null, EventMessageUtils.failStep(this, stepName, e, eventNameCustomizer));
                                      }
                                  }
                              });
                    }
                }

                return new StateBasedStepExecutionResult(stepName, () -> {WorkflowInstance.this.runNextStateChange(); return null;}, WorkflowInstance.this);
            }

            @Override
            public StepExecutionResult waitFor(@NotNull String stepName, @NotNull QualifiedName qualifiedName, @NotNull Predicate<EventMessage> predicate, @NotNull Duration timeout, @NotNull EventNameCustomizer eventNameCustomizer) {
                return null;
            }
        });
    }

    @Override
    public Clock getClock() {
        return Clock.systemUTC();
    }

    @Override
    public void applyPayloadModification(PayloadProcessor payloadModification) {
    }

    @Override
    public StepExecution getStep(String stepName) {
        return steps.get(stepName);
    }

    @Override
    public void onEvent(EventMessage eventMessage, ProcessingContext processingContext) {
        if (!taskQueue.offer(i -> i.applyStateChange(eventMessage))) {
            // whoops, we're overloading this workflow with events. STOP!!!
            throw new RuntimeException("Too many events for this workflow instance");
        }
    }
}
