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
package io.axoniq.framework.workflow.simulation.scenarios;

import io.axoniq.framework.workflow.simulation.harness.LogCapture;
import io.axoniq.framework.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import io.axoniq.framework.workflow.simulation.faults.FaultKind;
import io.axoniq.framework.workflow.simulation.harness.DstSimulation;
import io.axoniq.framework.workflow.simulation.harness.SimulationConfig;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The whole smoke workload under the {@link FaultKind#STALE_WRITER} fault: a foreign write tagged with a live
 * instance's id lands in the shared store while that instance is inside whichever feature path it happens to be in —
 * a parallel combinator branch, a retry backoff, a parked wait, a payload write, a version migration, a saga
 * compensation — and the next claim restores it.
 * <p>
 * Where {@code DcbFencingScenario} races two engines over one instance of one workflow, this covers the other axis:
 * one fence, every feature the default registrations drive. The oracle is the durable log the run ends with, and it
 * is the fencing contract, not a happy-path check — a fenced execution stops, so an instance may well take a
 * different route to its terminal state than it would have unfenced:
 * <ul>
 *   <li>at most one {@code STARTED} workflow record per instance;</li>
 *   <li>at most one terminal record per {@code (workflowId, stepName)};</li>
 *   <li>at most one terminal workflow record per instance.</li>
 * </ul>
 * The harness's always-on invariants run after every step of the same run, so INV-2, INV-5, INV-7 and INV-10 are
 * asserted on top of these by {@link DstSimulation} itself.
 * <p>
 * <strong>This is a campaign vehicle, not a per-PR gate, and deliberately has no test of its own.</strong> The
 * workload it drives is intermittently red on {@code PayloadReducerSemantics} without any stale writer at all: over
 * seeds 11..40 with an identical fault set minus {@link FaultKind#STALE_WRITER}, seed 11 broke on it, and seeds 12,
 * 22 and 37 broke on it when re-run on their own. A gate built on this shape would be red on a clean engine, which
 * the setup refuses to ship. Run it as a sweep, read the outcomes, and attribute against the same sweep with the
 * fault removed.
 * <p>
 * <strong>Landing evidence, two levels.</strong> Every injection records how many foreign writes the durable log
 * holds after it, read back from the store. Whether an injection actually fenced anybody is a race the fault cannot
 * control, so the second signal — a {@code "was rejected"} warning from
 * {@code io.axoniq.framework.workflow.runtime.execution.SimpleWorkflowExecution} — is counted over the whole run and reported
 * rather than required per seed. A campaign whose every seed reports zero rejections has not tested fencing and is
 * inconclusive, not a pass.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class StaleWriterFencingScenario {

    private static final String REJECTION_LOGGER = "io.axoniq.framework.workflow.runtime.execution.SimpleWorkflowExecution";

    private StaleWriterFencingScenario() {
    }

    /**
     * Outcome of one seed.
     *
     * @param injections                 stale-writer injections that reached the store (from the fault trace).
     * @param foreignWritesInLog         foreign writes the durable log held after the last injection.
     * @param rejectionsObserved         {@code "was rejected"} warnings logged during the run — the fence firing.
     * @param maxStartedRecords          highest number of {@code STARTED} workflow records for any one instance.
     * @param maxTerminalRecordsForAStep highest number of terminal step records for any one
     *                                   {@code (workflowId, stepName)}.
     * @param maxWorkflowTerminalRecords highest number of terminal workflow records for any one instance.
     */
    public record Outcome(int injections,
                          int foreignWritesInLog,
                          int rejectionsObserved,
                          int maxStartedRecords,
                          int maxTerminalRecordsForAStep,
                          int maxWorkflowTerminalRecords) {

    }

    /**
     * Runs the smoke workload for one seed with the stale-writer fault in the drawn fault set.
     *
     * @param seed the seed.
     * @return the observed outcome.
     */
        public static Outcome run(long seed) {
        var rejections = LogCapture.attach(REJECTION_LOGGER);
        try {
            var result = new DstSimulation(configFor(seed)).run();
            var log = result.committedLog();
            return new Outcome(countInjections(result.faultTrace()),
                               foreignWritesInLog(result.faultTrace()),
                               countRejections(rejections),
                               maxPerInstance(log, StaleWriterFencingScenario::isWorkflowStarted),
                               maxTerminalRecordsForAStep(log),
                               maxPerInstance(log, StaleWriterFencingScenario::isWorkflowTerminal));
        } finally {
            rejections.close();
        }
    }

    /**
     * The smoke shape with the stale writer added to the reproducible fault set. The deadline is the chaos one: every
     * injection performs a full crash and recovery of the world, so a seed that draws many of them does far more
     * recovery work than a plain smoke seed.
     */
        private static SimulationConfig configFor(long seed) {
        return new SimulationConfig(seed,
                                    3,
                                    80,
                                    0.5,
                                    new FaultKind[]{FaultKind.STALE_WRITER, FaultKind.WORKER_CRASH,
                                            FaultKind.MESSAGE_REORDER, FaultKind.RESTART, FaultKind.CLOCK_JUMP},
                                    Duration.ofSeconds(240));
    }

    private static int countInjections(List<String> faultTrace) {
        return (int) faultTrace.stream().filter(line -> line.startsWith("STALE_WRITER: foreign tagged append")).count();
    }

    /**
     * Returns the foreign-write count the last injection read back from the durable log, or 0 if none was injected.
     */
    private static int foreignWritesInLog(List<String> faultTrace) {
        int count = 0;
        for (String line : faultTrace) {
            int marker = line.indexOf("the durable log now holds ");
            if (marker < 0) {
                continue;
            }
            var tail = line.substring(marker + "the durable log now holds ".length());
            count = Integer.parseInt(tail.substring(0, tail.indexOf(' ')));
        }
        return count;
    }

    private static int countRejections(LogCapture rejections) {
        return (int) rejections.messages().stream()
                         .filter(message -> message != null && message.contains("was rejected"))
                         .count();
    }

    private static boolean isWorkflowStarted(EventMessage event) {
        return MetadataUtils.getWorkflowStatus(event.metadata())
                            .filter(status -> status == WorkflowStatus.STARTED)
                            .isPresent();
    }

    private static boolean isWorkflowTerminal(EventMessage event) {
        return MetadataUtils.getWorkflowStatus(event.metadata())
                            .filter(WorkflowStatus::isTerminal)
                            .isPresent();
    }

    private static int maxPerInstance(List<EventMessage> committedLog,
                                      java.util.function.Predicate<EventMessage> matches) {
        var counts = new HashMap<String, Integer>();
        for (EventMessage event : committedLog) {
            if (matches.test(event)) {
                counts.merge(MetadataUtils.getWorkflowId(event.metadata()), 1, Integer::sum);
            }
        }
        return counts.values().stream().max(Integer::compareTo).orElse(0);
    }

    private static int maxTerminalRecordsForAStep(List<EventMessage> committedLog) {
        Map<String, Integer> counts = new HashMap<>();
        for (EventMessage event : committedLog) {
            var status = MetadataUtils.getStepStatus(event.metadata());
            if (status.isEmpty() || !status.get().isTerminal()) {
                continue;
            }
            var key = MetadataUtils.getWorkflowId(event.metadata()) + "/" + MetadataUtils.getStepName(event.metadata());
            counts.merge(key, 1, Integer::sum);
        }
        return counts.values().stream().max(Integer::compareTo).orElse(0);
    }

}
