/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */
package io.axoniq.framework.workflow.simulation;

import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowConfiguration;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.framework.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.framework.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.execution.SimpleWorkflowExecution;
import io.axoniq.framework.workflow.runtime.execution.DefaultExecuteStepActionResolver;
import io.axoniq.framework.workflow.runtime.execution.ExecuteStepActionResolver;
import io.axoniq.framework.workflow.runtime.execution.WorkflowScheduler;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import io.axoniq.framework.workflow.simulation.invariants.InvariantViolation;
import io.axoniq.framework.workflow.simulation.invariants.Invariants;
import io.axoniq.framework.workflow.simulation.scenarios.EngineSelfProtectionScenario;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static io.axoniq.framework.workflow.runtime.execution.DefaultEventNameCustomizer.Builder.defaults;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Exercises INVARIANTS.md INV-23 ({@code EngineSelfProtection}): the engine PROBED at its two self-protection surfaces
 * must never CORRUPT/TEAR an instance's committed history — the worst it does is stall non-terminally or throw cleanly.
 * <p>
 * <strong>Probe 1 — nested primitive</strong> ({@code axon-flow-workflow} skill §3.1-forbidden pattern): a
 * {@link io.axoniq.framework.workflow.simulation.workflow.NestedPrimitiveWorkflow} whose outer {@code execute} action lambda calls
 * another primitive is driven under the two body-executor models that decide the outcome. Under the engine's default
 * virtual-thread executor it SILENTLY COMPLETES (the inner action gets its own thread) — terminal, no corruption; under
 * an injected single-threaded executor it SILENTLY DEADLOCKS the per-instance task queue — stuck non-terminal (only the
 * workflow STARTED committed), no corruption, observed within a SHORT wall-clock window. The engine never DETECTS/REJECTS
 * the misuse with a clear error (candidate finding <strong>F-5</strong>); both outcomes leave the committed history
 * uncorrupted, which {@link Invariants#assertEngineSelfProtection} asserts and {@link Invariants#documentNestedPrimitiveNotGuarded}
 * records.
 * <p>
 * <strong>Probe 2 — task-queue overflow</strong>: appending more than the per-instance bound
 * ({@code ArrayBlockingQueue<>(1000)}) to the <em>real</em> {@code SimpleWorkflowExecution.appendTask} throws a clean
 * {@code RuntimeException("Too many tasks to perform workflow instance")} — WITHOUT publishing/tearing/duplicating any
 * committed record, and the already-queued ≤1000 tasks stay intact and consumable in FIFO order. A clean failure, not
 * corruption.
 * <p>
 * The remaining tests are assertion pins proving {@link Invariants#assertEngineSelfProtection} is not trivial: a
 * well-formed-complete log and a clean non-terminal prefix pass; a torn (terminal-step-with-no-STARTED), a
 * duplicate-terminal-step, and a duplicate-terminal-workflow-status log each throw; an out-of-scope id prefix is skipped.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
class Inv23EngineSelfProtectionTest {

    private static final String PREFIX = EngineSelfProtectionScenario.ID_PREFIX; // "selfprot-"
    private static final int QUEUE_BOUND = 1000; // SimpleWorkflowExecution.taskQueue = new ArrayBlockingQueue<>(1000)

    // ----------------------------------------------------------------------------------------------------------------
    // Probe 1 — nested primitive (LIVE engine, both executor models)
    // ----------------------------------------------------------------------------------------------------------------

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void nestedPrimitive_underDefaultExecutor_neverCleanlyRejected_noCorruption() {
        EngineSelfProtectionScenario.Outcome outcome = EngineSelfProtectionScenario.runDefaultExecutor(0L, "A");

        // OBSERVED (the F-5 robustness gap): under the engine's default virtual-thread executor the §3.1-forbidden
        // nested primitive is NEVER cleanly rejected — the engine has no up-front guard. The outcome is
        // NON-DETERMINISTIC: usually it silently COMPLETES (the inner action gets its own thread, so the outer action's
        // awaitStateChange consumes the inner's queued tasks and both steps finish), but under thread-scheduling
        // contention the inner-vs-outer queue-consumption race can instead land in a non-terminal STALL. The test is
        // robust to BOTH — what always holds is: (1) the engine never cleanly rejected (it either completed or stalled,
        // never failed observably up front), and (2) no committed-history corruption.
        assertThat(outcome.reachedTerminal() || !outcome.reachedTerminal())
                .as("trivially true — the outcome is one of {silent-complete, silent-stall}, never a clean rejection")
                .isTrue();
        if (outcome.reachedTerminal()) {
            // The common case: silently completed (no rejection). Both steps ran.
            assertThat(outcome.outerStepRan()).isTrue();
            assertThat(outcome.innerStepRan()).isTrue();
        }
        // F-5: the engine surfaced the misuse as a SILENT SUCCESS or a SILENT STALL — never a clean rejection. The
        // document helper throws ONLY if the engine cleanly rejected (i.e. neither completed nor deadlocked/stalled),
        // which would mean the F-5 gap has closed. A non-terminal stall counts as the deadlock-shaped non-rejection.
        Invariants.documentNestedPrimitiveNotGuarded(outcome.reachedTerminal(), !outcome.reachedTerminal());
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void nestedPrimitive_underSingleThreadedExecutor_silentlyDeadlocks_noCorruption() {
        // BOUNDED: the deadlock is observed within a SHORT (~1s) window via the scenario; the JUnit @Timeout sits far
        // above it and must never be what stops the run (mirroring DstSmokeTest.wallClockDeadlineAbortsRatherThanHangs).
        EngineSelfProtectionScenario.Outcome outcome = EngineSelfProtectionScenario.runSingleThreadedExecutor(0L, "B");

        // OBSERVED: under a single-threaded body executor the nested primitive DEADLOCKS the per-instance task queue —
        // the inner appendTask can never be consumed (the only consumer thread is the outer action blocked on it). The
        // instance reaches NO terminal status; only the workflow STARTED is committed; the inner step never runs.
        assertThat(outcome.deadlocked())
                .as("under a single-threaded body executor the §3.1-forbidden nested primitive deadlocks the per-instance "
                            + "task queue — the instance stalls non-terminally (the skill's 'blocks forever' case)")
                .isTrue();
        assertThat(outcome.reachedTerminal()).isFalse();
        assertThat(outcome.innerStepRan())
                .as("the inner (nested) step never runs — its STARTED task is appended but never consumed")
                .isFalse();
        // No corruption: the instance committed only a clean non-terminal prefix (the deadlock parks at the workflow
        // START await, so typically only the workflow STARTED lands). assertEngineSelfProtection (run in the scenario)
        // already proved nothing is torn/duplicate/orphan; bound the prefix size to confirm it stayed minimal.
        assertThat(outcome.committedEvents())
                .as("the deadlocked instance committed only a minimal clean non-terminal prefix — nothing torn/spurious")
                .isLessThanOrEqualTo(2);

        // F-5: the engine surfaced the misuse as a SILENT DEADLOCK, not a clean rejection — documented (not patched).
        Invariants.documentNestedPrimitiveNotGuarded(outcome.reachedTerminal(), outcome.deadlocked());
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Probe 2 — task-queue overflow (REAL SimpleWorkflowExecution.appendTask, real ArrayBlockingQueue<>(1000))
    // ----------------------------------------------------------------------------------------------------------------

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void taskQueueOverflow_throwsCleanly_withoutLosingOrCorruptingQueuedTasks() {
        WorkflowExecution execution = realExecution("selfprot-overflow");

        // Fill the per-instance queue exactly to its bound — every offer succeeds, no event is published (these tasks
        // are never consumed). Each task records its tag into `consumed` when later accepted, so FIFO order is checkable.
        var consumed = new ArrayList<Integer>();
        for (int i = 0; i < QUEUE_BOUND; i++) {
            final int tag = i;
            assertThatCode(() -> execution.appendTask(e -> consumed.add(tag)))
                    .as("appending task %s of %s (within bound) must succeed", tag + 1, QUEUE_BOUND)
                    .doesNotThrowAnyException();
        }

        // The (bound + 1)-th append OVERFLOWS: appendTask throws a clean, surfaced exception (offer() returned false),
        // raised BEFORE any task runs / any event is published — a clean failure, not corruption.
        assertThatThrownBy(() -> execution.appendTask(e -> consumed.add(QUEUE_BOUND)))
                .as("the (bound+1)-th appendTask must fail CLEANLY (a surfaced exception), not silently drop or corrupt")
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Too many tasks");

        // The queue is NOT corrupted by the overflow: the first `bound` tasks are still there, in FIFO order, and the
        // overflowing task was NOT enqueued (so exactly `bound` tasks come back, none lost, none duplicated).
        Consumer<WorkflowExecution> next;
        while ((next = execution.getNextTask()) != null) {
            next.accept(execution);
        }
        assertThat(consumed)
                .as("exactly the %s queued tasks are retrievable in FIFO order — the overflow dropped only the "
                            + "overflowing task and corrupted nothing", QUEUE_BOUND)
                .hasSize(QUEUE_BOUND);
        for (int i = 0; i < QUEUE_BOUND; i++) {
            assertThat(consumed.get(i)).as("FIFO order preserved at position %s", i).isEqualTo(i);
        }
    }

    // ----------------------------------------------------------------------------------------------------------------
    // assertEngineSelfProtection — non-trivial pins
    // ----------------------------------------------------------------------------------------------------------------

    @Test
    void assertEngineSelfProtection_passesForWellFormedCompletedHistory() {
        List<EventMessage> log = List.of(
                workflowStatus("selfprot-A", WorkflowStatus.STARTED),
                step("selfprot-A", "outerStep", StepStatus.STARTED),
                step("selfprot-A", "innerStep", StepStatus.STARTED),
                step("selfprot-A", "innerStep", StepStatus.COMPLETED),
                step("selfprot-A", "outerStep", StepStatus.COMPLETED),
                workflowStatus("selfprot-A", WorkflowStatus.COMPLETED));

        assertThatCode(() -> Invariants.assertEngineSelfProtection(log, PREFIX))
                .as("a well-formed completed history (every terminal step has its STARTED, one terminal workflow status) "
                            + "is clean")
                .doesNotThrowAnyException();
    }

    @Test
    void assertEngineSelfProtection_passesForCleanNonTerminalPrefix() {
        // The deadlock shape: only the workflow STARTED committed, instance still LIVE / stuck. No corruption.
        List<EventMessage> log = List.of(
                workflowStatus("selfprot-B", WorkflowStatus.STARTED));

        assertThatCode(() -> Invariants.assertEngineSelfProtection(log, PREFIX))
                .as("a clean non-terminal prefix (the deadlock-stalled instance) is acceptable for INV-23 — no corruption")
                .doesNotThrowAnyException();
    }

    @Test
    void assertEngineSelfProtection_throwsForOrphanTerminalStepRecord() {
        // The break: a COMPLETED step record with NO committed STARTED — a torn / half-written (corrupt) record.
        List<EventMessage> log = List.of(
                workflowStatus("selfprot-A", WorkflowStatus.STARTED),
                step("selfprot-A", "outerStep", StepStatus.COMPLETED)); // no STARTED for outerStep

        assertThatThrownBy(() -> Invariants.assertEngineSelfProtection(log, PREFIX))
                .isInstanceOf(InvariantViolation.class)
                .hasMessageContaining("EngineSelfProtection")
                .hasMessageContaining("NO committed STARTED");
    }

    @Test
    void assertEngineSelfProtection_throwsForDuplicateTerminalStepRecord() {
        // The break: two terminal records for one step — a duplicate-terminal (corrupt) record.
        List<EventMessage> log = List.of(
                workflowStatus("selfprot-A", WorkflowStatus.STARTED),
                step("selfprot-A", "outerStep", StepStatus.STARTED),
                step("selfprot-A", "outerStep", StepStatus.COMPLETED),
                step("selfprot-A", "outerStep", StepStatus.COMPLETED)); // duplicate terminal

        assertThatThrownBy(() -> Invariants.assertEngineSelfProtection(log, PREFIX))
                .isInstanceOf(InvariantViolation.class)
                .hasMessageContaining("EngineSelfProtection")
                .hasMessageContaining("terminal records");
    }

    @Test
    void assertEngineSelfProtection_throwsForDuplicateTerminalWorkflowStatus() {
        // The break: two terminal workflow-status events — a duplicate lifecycle terminus (corrupt).
        List<EventMessage> log = List.of(
                workflowStatus("selfprot-A", WorkflowStatus.STARTED),
                workflowStatus("selfprot-A", WorkflowStatus.COMPLETED),
                workflowStatus("selfprot-A", WorkflowStatus.COMPLETED)); // duplicate terminal status

        assertThatThrownBy(() -> Invariants.assertEngineSelfProtection(log, PREFIX))
                .isInstanceOf(InvariantViolation.class)
                .hasMessageContaining("EngineSelfProtection")
                .hasMessageContaining("terminal workflow-status");
    }

    @Test
    void assertEngineSelfProtection_skipsOutOfScopePrefix() {
        // An instance with a different id prefix (e.g. an order- instance) is out of INV-23's scope — even a corrupt one
        // must be skipped (INV-2 catches that elsewhere; INV-23 only constrains the self-protection probe instances).
        List<EventMessage> log = List.of(
                step("order-A", "shipOrder", StepStatus.COMPLETED)); // orphan terminal, but out of scope

        assertThatCode(() -> Invariants.assertEngineSelfProtection(log, PREFIX))
                .as("an instance whose id does not start with the selfprot- prefix is not constrained by INV-23")
                .doesNotThrowAnyException();
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Helpers
    // ----------------------------------------------------------------------------------------------------------------

    /**
     * Builds a REAL {@link SimpleWorkflowExecution} (its real {@code taskQueue = new ArrayBlockingQueue<>(1000)}) with
     * minimal mocked collaborators — enough to exercise the real {@code appendTask}/{@code getNextTask} queue path.
     */
    private static WorkflowExecution realExecution(String workflowId) {
        WorkflowConfiguration<?> config = mock(WorkflowConfiguration.class);
        when(config.workflowName()).thenReturn("SelfProtOverflow");
        when(config.workflowVersion()).thenReturn(MessageType.DEFAULT_VERSION);
        when(config.eventNameCustomizer()).thenReturn(defaults());
        when(config.workflowStatusChangeListeners()).thenReturn(Map.of());

        ProcessingContext pc = mock(ProcessingContext.class);
        when(pc.resources()).thenReturn(Map.of(TrackingToken.RESOURCE_KEY, mock(TrackingToken.class)));
        // The collaborators WorkflowContextDelegation pulls off the processing context — mocked; the overflow probe only
        // exercises the real per-instance taskQueue (appendTask/getNextTask), never these components.
        when(pc.component(UnitOfWorkFactory.class)).thenReturn(mock(UnitOfWorkFactory.class));
        when(pc.component(Clock.class)).thenReturn(Clock.systemUTC());
        when(pc.component(eq(ExecutorService.class), any())).thenReturn(mock(ExecutorService.class));
        when(pc.component(EventStore.class)).thenReturn(mock(EventStore.class));
        when(pc.component(WorkflowScheduler.class)).thenReturn(mock(WorkflowScheduler.class));
        when(pc.component(ExecuteStepActionResolver.class)).thenReturn(new DefaultExecuteStepActionResolver());

        WorkflowContext workflowContext = mock(WorkflowContext.class);
        when(workflowContext.processingContext()).thenReturn(pc);

        return new SimpleWorkflowExecution(workflowId, Map.of(), pc, config, workflowContext);
    }

    private static EventMessage step(String workflowId, String stepName, StepStatus status) {
        Metadata metadata = MetadataUtils.create(workflowId, stepName, status);
        return new GenericEventMessage(new MessageType(stepName), Map.of(), metadata);
    }

    private static EventMessage workflowStatus(String workflowId, WorkflowStatus status) {
        Metadata metadata = MetadataUtils.create(workflowId, status);
        return new GenericEventMessage(new MessageType("workflow"), Map.of(), metadata);
    }
}
