package io.axoniq.workflow.runtime;

import io.axoniq.workflow.runtime.definition.SimpleWorkflow;
import io.axoniq.workflow.runtime.definition.WorkflowDefinition;
import io.axoniq.workflow.runtime.engine.StateManager;
import io.axoniq.workflow.runtime.engine.WorkflowEngine;
import io.axoniq.workflow.runtime.event.StepCompleted;
import io.axoniq.workflow.runtime.event.StepStarted;
import io.axoniq.workflow.runtime.step.*;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static io.axoniq.workflow.runtime.engine.WorkflowEngine.WF_STARTED;

class RunnerTest {

  private static final Logger logger = LoggerFactory.getLogger(RunnerTest.class);

  private static final String WF_ID_1 = "Workflow 1";
  private final StateManager stateManager = new StateManager();
  private final WorkflowEngine engine = new WorkflowEngine(stateManager);

  Instant startTime = Instant.now();
  WorkflowDefinition myWorkFlow = () -> new SimpleWorkflow(WF_ID_1,
    new OnTimeoutStep(
      new ParallelSteps("parallel1",
        Set.of(
          new WaitStep("wait 1", Duration.ofMillis(1000),
            new RunStep("step1.1", () -> logger.info("Wait 1 completed. {} ms since start.", Duration.between(startTime, Instant.now()).toMillis()))
          ),
          new WaitStep("wait 2", Duration.ofMillis(2000))
        ),
        new RunStep("step2",
          () -> logger.info("Hello world from step2! {} ms since start.", Duration.between(startTime, Instant.now()).toMillis()))
      ),
      Duration.ofMillis(2500),
      new RunStep("onTimeout",
        () -> logger.info("Timeout happened.. {} ms since start: ", Duration.between(startTime, Instant.now()).toMillis())))
  );

  @BeforeEach
  void setUp() {
    stateManager.clearAll();
  }

  @Test
  void shouldRunFromStart() throws ExecutionException, InterruptedException {

    engine.execute(myWorkFlow);
    stateManager.print();
  }


  @Test
  void shouldRunFromFinishedParallel() throws ExecutionException, InterruptedException {
    stateManager.appendAll(WF_ID_1, List.of(
      new GenericEventMessage(MessageType.fromString(WF_STARTED), new StepStarted(WF_ID_1)),
      new GenericEventMessage(MessageType.fromString(StepStarted.ID), new StepStarted("step1.1")),
      new GenericEventMessage(MessageType.fromString(StepCompleted.ID), new StepCompleted("step1.1", Map.of()))
    ));
    engine.execute(myWorkFlow);
  }

}