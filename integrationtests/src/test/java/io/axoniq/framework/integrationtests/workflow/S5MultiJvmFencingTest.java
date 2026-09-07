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
package io.axoniq.framework.integrationtests.workflow;

import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.messaging.eventstreaming.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.axoniq.framework.integrationtests.workflow.DcbFencingBackends.Backend.AXON_SERVER;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * S5: two REAL JVMs over one Axon Server DCB store, separate (in-memory, per-process) token stores, forced claim
 * loss via SIGSTOP of the whole owning JVM. The paused node's in-flight step straddles the takeover; when it resumes
 * it must be fenced by the store: no duplicate facts, one terminal outcome, and the stale writer's appends rejected.
 *
 * <p>A round is CONCLUSIVE only when both fault-landed proofs are present: node B completed the instance (store) AND
 * node A attempted an append after resuming and was rejected (its stdout carries the fencing warning). Rounds where
 * the race never materialized are re-drawn with a fresh workflow id, never counted as pass.</p>
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
class S5MultiJvmFencingTest {

    private static final Logger logger = LoggerFactory.getLogger(S5MultiJvmFencingTest.class);
    private static final int REQUIRED_CONCLUSIVE_ROUNDS = 3;
    private static final int MAX_ROUNDS = 8;

    @Test
    @Timeout(value = 1500, unit = TimeUnit.SECONDS)
    void staleJvmCannotRecordAfterTakeover() throws Exception {
        EventStorageEngine auditStore = DcbFencingBackends.freshStore(AXON_SERVER);
        String[] address = DcbFencingBackends.axonServerAddress();

        // Signals must go through an engine's sink: a raw store append does not wake a parked wait
        // (proven by S5SignalProbeTest — node.publish wakes, appendDirectly does not). This node registers
        // only a never-starting dummy workflow, so it publishes without ever owning the instance under test.
        try (var publisher = DcbFencingBackends.startNode(auditStore, "s5-publisher", new PublisherOnly())) {
            int conclusiveClean = 0;
            List<String> violations = new ArrayList<>();
            List<String> inconclusive = new ArrayList<>();
            for (int round = 1; round <= MAX_ROUNDS; round++) {
                String wfId = "s5-r" + round + "-" + System.nanoTime();
                RoundResult result = runRound(auditStore, publisher, address, wfId);
                logger.info("S5 round {} ({}): {}", round, wfId, result);
                if (result.violation != null) {
                    violations.add("round " + round + " (" + wfId + "): " + result.violation);
                } else if (result.conclusive) {
                    conclusiveClean++;
                } else {
                    inconclusive.add(wfId + ": " + result.note);
                }
            }
            logger.info("S5 SUMMARY: rounds={} violations={} conclusiveClean={} inconclusive={}",
                        MAX_ROUNDS, violations.size(), conclusiveClean, inconclusive.size());
            if (!violations.isEmpty()) {
                throw new AssertionError("S5 VIOLATIONS in " + violations.size() + "/" + MAX_ROUNDS + " rounds:\n"
                                                 + String.join("\n", violations));
            }
            assertThat(conclusiveClean)
                    .as("conclusive rounds (B completed AND A rejected after resume); inconclusive: " + inconclusive)
                    .isGreaterThanOrEqualTo(REQUIRED_CONCLUSIVE_ROUNDS);
        }
    }

    /** Start event of the never-spawned publisher workflow. */
    public record NeverStart(String id) {

    }

    /** Gives the publisher node a module without letting it own the instance under test. */
    public static final class PublisherOnly {

        @io.axoniq.framework.workflow.runtime.api.annotation.Workflow(
                workflowName = "S5Publisher", idProperty = "id", startOnEventClass = NeverStart.class)
        public void execute(io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext ctx) {
            // never spawned
        }
    }

    private record RoundResult(boolean conclusive, String note, String violation) {

    }

    private RoundResult runRound(EventStorageEngine auditStore, DcbFencingBackends.Node publisher,
                                 String[] address, String wfId) throws Exception {
        File outA = File.createTempFile("s5-A-", ".log");
        File outB = File.createTempFile("s5-B-", ".log");
        Process a = null;
        Process b = null;
        try {
            a = node("A", address, wfId, 0, outA);
            // A: prepare completes, instance parks on the GoSignal wait.
            if (!waitFor(outA, "WAITING role=A id=" + wfId, Duration.ofSeconds(90))) {
                return new RoundResult(false, "node A never parked on the wait", null);
            }
            // Force claim loss: freeze the whole owning JVM while the instance is parked.
            exec("kill", "-STOP", String.valueOf(a.pid()));

            b = node("B", address, wfId, 0, outB);
            // B restores the parked instance from the shared log and parks in the same wait.
            if (!waitFor(outB, "WAITING role=B id=" + wfId, Duration.ofSeconds(120))) {
                return new RoundResult(false, "node B never restored the parked instance", null);
            }

            // The signal races both nodes into shipFinal; B (running) must win and complete.
            publisher.publish(new S5NodeMain.GoSignal(wfId));
            if (!waitForStoreEvent(auditStore, wfId, "S5SlowWorkflowCompleted", Duration.ofSeconds(120))) {
                return new RoundResult(false, "node B never completed after the signal", null);
            }

            // Resume the stale writer: it receives the same signal, wakes the same wait, and must be fenced
            // at shipFinal's STARTED append — which also means its shipFinal action must never run (CL-3).
            exec("kill", "-CONT", String.valueOf(a.pid()));
            boolean rejected = waitFor(outA, "was rejected", Duration.ofSeconds(90));

            // Store audit: every fact exactly once, regardless of how the race fell.
            var started = DcbFencingBackends.eventsNamed(auditStore, wfId, "S5SlowWorkflowStarted");
            var completed = DcbFencingBackends.eventsNamed(auditStore, wfId, "S5SlowWorkflowCompleted");
            var all = DcbFencingBackends.eventsWithTags(auditStore, Tag.of("workflowId", wfId));
            var names = all.stream().map(e -> e.type().qualifiedName().toString()).toList();
            var duplicates = names.stream().distinct()
                                  .filter(n -> names.stream().filter(n::equals).count() > 1)
                                  .toList();
            long finalRunsA = countIn(outA, "FINAL-RUN role=A id=" + wfId);
            long finalRunsB = countIn(outB, "FINAL-RUN role=B id=" + wfId);
            if (started.size() != 1 || completed.size() != 1 || !duplicates.isEmpty()) {
                return new RoundResult(false, "",
                                       "started=" + started.size() + " completed=" + completed.size()
                                               + " duplicateFacts=" + duplicates + " events=" + names);
            }
            if (finalRunsA + finalRunsB > 1) {
                return new RoundResult(false, "",
                                       "CL-3 violated: shipFinal action ran " + (finalRunsA + finalRunsB)
                                               + " times across two writers (A=" + finalRunsA + " B=" + finalRunsB
                                               + ")");
            }
            if (!rejected) {
                return new RoundResult(false,
                                       "A resumed but no rejection observed (A may not have reached its append)",
                                       null);
            }
            logger.info("S5 {} conclusive: B completed, A fenced; shipFinal ran A={} B={}; A-log={} B-log={}",
                        wfId, finalRunsA, finalRunsB, outA, outB);
            return new RoundResult(true, "rejection observed, store consistent, action ran once", null);
        } finally {
            if (a != null) {
                exec("kill", "-CONT", String.valueOf(a.pid())); // never leave a stopped process behind
                a.destroyForcibly();
            }
            if (b != null) {
                b.destroyForcibly();
            }
        }
    }

    private static Process node(String role, String[] address, String wfId, long actionSleepMs, File out)
            throws IOException {
        String java = System.getProperty("java.home") + "/bin/java";
        var command = List.of(java, "-cp", System.getProperty("java.class.path"),
                              S5NodeMain.class.getName(),
                              role, address[0], address[1], wfId, String.valueOf(actionSleepMs));
        return new ProcessBuilder(command)
                .redirectOutput(ProcessBuilder.Redirect.to(out))
                .redirectErrorStream(true)
                .start();
    }

    private static boolean waitFor(File log, String marker, Duration budget) throws Exception {
        Instant deadline = Instant.now().plus(budget);
        while (Instant.now().isBefore(deadline)) {
            if (Files.exists(log.toPath()) && Files.readString(log.toPath()).contains(marker)) {
                return true;
            }
            Thread.sleep(250);
        }
        return false;
    }

    private static boolean waitForStoreEvent(EventStorageEngine store, String wfId, String localName,
                                             Duration budget) throws Exception {
        Instant deadline = Instant.now().plus(budget);
        while (Instant.now().isBefore(deadline)) {
            if (!DcbFencingBackends.eventsNamed(store, wfId, localName).isEmpty()) {
                return true;
            }
            Thread.sleep(500);
        }
        return false;
    }

    private static long countIn(File log, String marker) throws IOException {
        if (!Files.exists(log.toPath())) {
            return 0;
        }
        return Files.readString(log.toPath()).lines().filter(l -> l.contains(marker)).count();
    }

    private static void exec(String... cmd) throws Exception {
        new ProcessBuilder(cmd).start().waitFor(5, TimeUnit.SECONDS);
    }
}
