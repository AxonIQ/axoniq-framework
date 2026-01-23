package io.axoniq.workflow.runtime;

import io.axoniq.workflow.dsl.simple.SimpleContext;
import io.axoniq.workflow.dsl.simple.SimpleDefinition;
import io.axoniq.workflow.runtime.api.workflow.WorkflowFailedException;
import io.axoniq.workflow.runtime.engine.SimpleStateManager;
import io.axoniq.workflow.runtime.engine.WorkflowEngine;
import jakarta.annotation.Nonnull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.CompletionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkflowFailedExceptionTest {

  private SimpleStateManager stateManager;
  private WorkflowEngine engine;

  @BeforeEach
  void setUp() {
    stateManager = new SimpleStateManager();
    engine = new WorkflowEngine(stateManager, stateManager);
  }

  // Workflow that throws WorkflowFailedException
  static class FailingWorkflow implements SimpleDefinition {
    static final String WORKFLOW_ID = "failing-workflow";

    @Override
    public String association(@Nonnull Map<String, Object> trigger) {
      return WORKFLOW_ID;
    }

    @Override
    public void execute(SimpleContext context) {
      context.execute("initialStep", () -> {});
      throw new WorkflowFailedException("Intentional failure"); //terminal
    }
  }

  // Workflow that throws generic RuntimeException
  static class CrashingWorkflow implements SimpleDefinition {
    static final String WORKFLOW_ID = "crashing-workflow";

    @Override
    public String association(@Nonnull Map<String, Object> trigger) {
      return WORKFLOW_ID;
    }

    @Override
    public void execute(SimpleContext context) {
      context.execute("initialStep", () -> {});
      throw new OutOfMemoryError("Unexpected error"); //non terminal
    }
  }

  @Test
  void shouldSetStatusToFailedWhenWorkflowFailedExceptionThrown() {
    var workflow = new FailingWorkflow();
    var future = engine.restoreAndExecute(workflow, Map.of());

    assertThatThrownBy(future::join)
      .isInstanceOf(CompletionException.class)
      .hasCauseInstanceOf(WorkflowFailedException.class);

    var events = stateManager.getHistory(FailingWorkflow.WORKFLOW_ID);
    var lastEvent = events.getLast();

    assertThat(lastEvent.type().name())
      .as("Last event should be a Failed workflow event")
      .contains("Failed");
  }

  @Test
  void shouldNotSetStatusToFailedWhenOtherRuntimeExceptionThrown() {
    var workflow = new CrashingWorkflow();
    var future = engine.restoreAndExecute(workflow, Map.of());

    assertThatThrownBy(future::join)
      .isInstanceOf(CompletionException.class);

    var events = stateManager.getHistory(CrashingWorkflow.WORKFLOW_ID);
    var lastEvent = events.getLast();

    assertThat(lastEvent.type().name())
      .as("Last event should NOT be a Failed workflow event for generic exception")
      .doesNotContain("Failed");
  }
}
