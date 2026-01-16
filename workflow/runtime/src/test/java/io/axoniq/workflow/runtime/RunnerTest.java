package io.axoniq.workflow.runtime;

import io.axoniq.workflow.runtime.definition.Workflow;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.fail;

class RunnerTest {

    private static final Logger logger = LoggerFactory.getLogger(RunnerTest.class);

    @Test
    void shouldRunWorkflow() throws ExecutionException, InterruptedException {
        var startTime = Instant.now();
        Workflow myWorkFlow = () -> new SimpleWorkflow("Workflow 1",
                                                       new ParallelSteps("parallel1", Set.of(
                                                               new WaitStep("wait 1", Duration.ofSeconds(1), new RunStep("step1.1", () -> System.out.println("Wait 1 completed.... ms since start: " + Duration.between(startTime, Instant.now()).toMillis()), new Completed())),
                                                               new WaitStep("wait 2", Duration.ofSeconds(2), new Completed())
                                                       ),
                                                                         new RunStep("step2", () -> System.out.println("Hello world! ms since start: " + Duration.between(startTime, Instant.now()).toMillis()),
                                                                                     new Completed())));

        WorkflowState workflowInstance = myWorkFlow.initialState();
        Queue<EventMessage> events = new ConcurrentLinkedQueue<>();

        events.add(new GenericEventMessage(MessageType.fromString("io.axoniq.workflow.WorkflowStarted#0.1"), new StepStarted("Workflow 1")));

        CompletableFuture<Result> executionResult;
        do {
            while (!events.isEmpty()) {
                EventMessage event = events.poll();
                workflowInstance = workflowInstance.apply(event);
            }

            executionResult = workflowInstance.execute(eventMessages -> {
                events.addAll(eventMessages);
                return CompletableFuture.completedFuture(null);
            });
            long timeout = executionResult.get().timeout();
            if (events.isEmpty() && timeout > 0) {
                logger.info("Waiting for events or expiry of timeout: {}ms", timeout);
                Thread.sleep(timeout);
            }
        } while (!executionResult.get().isCompleted());

        executionResult.get().error().ifPresent(e -> fail(e.getMessage()));
    }

}