package io.axoniq.framework.integrationtests.workflow;

import io.axoniq.framework.integrationtests.workflow.DcbFencingBackends.Backend;
import io.axoniq.framework.integrationtests.workflow.DcbFencingBackends.Node;
import io.axoniq.framework.integrationtests.workflow.S4RestoreSeedTest.ParkingWorkflow;
import io.axoniq.framework.integrationtests.workflow.S4RestoreSeedTest.ReleaseParked;
import io.axoniq.framework.integrationtests.workflow.S4RestoreSeedTest.StartParking;
import io.axoniq.framework.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.execution.EventSourcedRunningWorkflows;
import io.axoniq.framework.workflow.runtime.execution.WorkflowEventTagResolver;
import io.axoniq.framework.workflow.runtime.execution.WorkflowEventTags;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import org.axonframework.eventsourcing.eventstore.AppendCondition;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.GenericTaggedEventMessage;
import org.axonframework.eventsourcing.eventstore.SourcingCondition;
import org.axonframework.eventsourcing.eventstore.TaggedEventMessage;
import org.axonframework.eventsourcing.eventstore.TerminalEventMessage;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.inmemory.InMemoryTokenStore;
import org.axonframework.messaging.eventstreaming.Tag;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static io.axoniq.framework.integrationtests.workflow.DcbFencingBackends.startNode;
import static io.axoniq.framework.integrationtests.workflow.DcbFencingBackends.workflowTag;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Scaling probes for the workflow engine on Axon Server, one sweep per cost axis. Not an assertion suite: each sweep
 * records wall-clock and heap numbers to a CSV ({@code -Dperf.out}) and only fails when the engine stops making
 * progress. Sweep sizes come from system properties so a pilot run can calibrate a full run.
 * <ul>
 *     <li>{@link #runningWorkflowCount()}: N parked instances, tiny history each. Start throughput, heap per
 *     instance, failover restore time, release-all time.</li>
 *     <li>{@link #runningListSourcingVsLifecycleHistory()}: the running-workflow registry is one event-sourced entity
 *     replayed from every lifecycle event ever written. Fixed running set, growing completed history.</li>
 *     <li>{@link #instanceStateSourcingVsEventsPerWorkflow()}: one instance with a growing own history. Step
 *     latency as history grows, failover restore time, first append after restore.</li>
 * </ul>
 */
@org.junit.jupiter.api.Tag("docker")
class WorkflowScalingPerfTest {

    private static final String MODULE = "perf";
    private static final Path OUT = Path.of(System.getProperty("perf.out", "target/perf-results.csv"));
    private static final Duration RESTORE_CEILING = Duration.ofSeconds(
            Long.getLong("perf.restoreCeilingSeconds", 120));

    private static int[] sizes(String property, String defaults) {
        return Arrays.stream(System.getProperty(property, defaults).split(","))
                     .map(String::trim).mapToInt(Integer::parseInt).toArray();
    }

    // ---------------------------------------------------------------------------------------------------------
    // Sweep A: how many running workflows before it gets ugly
    // ---------------------------------------------------------------------------------------------------------

    @Test
    @Timeout(7200)
    void runningWorkflowCount() {
        for (int n : sizes("perf.running", "100,500,1000,2000,5000")) {
            var store = DcbFencingBackends.freshStore(Backend.AXON_SERVER);
            List<String> ids = IntStream.range(0, n).mapToObj(i -> "a-" + n + "-" + i).toList();
            var tokens = new InMemoryTokenStore();
            long heapBefore = usedHeap();

            long startMs;
            long heapWithRunning;
            try (var node = startNode(store, MODULE, new ParkingWorkflow(), tokens)) {
                startMs = timed(() -> {
                    publishParallel(node, ids.stream().map(id -> (Object) new StartParking(id, 0)).toList());
                    awaitParked(node, store, n);
                });
                heapWithRunning = usedHeap();
            }
            record("running", n, "start_all_ms", startMs, n);
            record("running", n, "heap_per_instance_bytes", (heapWithRunning - heapBefore) / n, n);
            record("running", n, "raw_source_running_list_ms", timed(() -> sourceCount(store, lifecycleCriteria())), n);
            record("running", n, "raw_source_one_instance_ms",
                   timed(() -> sourceCount(store, EventCriteria.havingTags(workflowTag(ids.get(n / 2))))), n);

            long restoreMs;
            long releaseMs;
            try (var node = startNode(store, MODULE, new ParkingWorkflow(), tokens)) {
                restoreMs = timedOrCeiling("running", n, "failover_restore_ms", node, () -> awaitRunning(node, n));
                releaseMs = timedOrCeiling("running", n, "release_all_ms", node, () -> {
                    publishParallel(node, ids.stream().map(id -> (Object) new ReleaseParked(id)).toList());
                    await().atMost(RESTORE_CEILING).pollInterval(Duration.ofMillis(200))
                           .untilAsserted(() -> assertThat(node.runningWorkflowIds()).isEmpty());
                });
            }
            record("running", n, "failover_restore_ms", restoreMs, n);
            record("running", n, "release_all_ms", releaseMs, n);
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // Sweep B: running-workflow list sourcing vs total lifecycle history (completed workflows count too)
    // ---------------------------------------------------------------------------------------------------------

    @Test
    @Timeout(7200)
    void runningListSourcingVsLifecycleHistory() {
        int running = Integer.getInteger("perf.historyRunning", 10);
        for (int completed : sizes("perf.completed", "0,10000,50000,200000")) {
            var store = DcbFencingBackends.freshStore(Backend.AXON_SERVER);
            long seedMs = timed(() -> seedCompletedWorkflows(store, completed));
            record("history", completed, "seed_ms", seedMs, 2L * completed);

            List<String> ids = IntStream.range(0, running).mapToObj(i -> "b-" + completed + "-" + i).toList();
            var tokens = new InMemoryTokenStore();
            try (var node = startNode(store, MODULE, new ParkingWorkflow(), tokens)) {
                publishParallel(node, ids.stream().map(id -> (Object) new StartParking(id, 0)).toList());
                awaitParked(node, store, running);
            }
            record("history", completed, "raw_source_running_list_ms",
                   timed(() -> sourceCount(store, lifecycleCriteria())), 2L * completed + running);

            long restoreMs;
            try (var node = startNode(store, MODULE, new ParkingWorkflow(), tokens)) {
                restoreMs = timedOrCeiling("history", completed, "failover_restore_ms", node,
                                           () -> awaitRunning(node, running));
                if (restoreMs >= 0) {
                    publishParallel(node, ids.stream().map(id -> (Object) new ReleaseParked(id)).toList());
                    await().atMost(RESTORE_CEILING).untilAsserted(() -> assertThat(node.runningWorkflowIds()).isEmpty());
                }
            }
            record("history", completed, "failover_restore_ms", restoreMs, running);
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // Sweep C: one instance's state sourcing vs its own event count
    // ---------------------------------------------------------------------------------------------------------

    @Test
    @Timeout(7200)
    void instanceStateSourcingVsEventsPerWorkflow() {
        for (int steps : sizes("perf.steps", "0,10,50,100,250,500,1000")) {
            var store = DcbFencingBackends.freshStore(Backend.AXON_SERVER);
            String id = "c-" + steps;
            var tokens = new InMemoryTokenStore();

            long executeMs;
            try (var node = startNode(store, MODULE, new ParkingWorkflow(), tokens)) {
                executeMs = timed(() -> {
                    node.publish(new StartParking(id, steps));
                    awaitParked(node, store, 1);
                });
            }
            int events = sourceCount(store, EventCriteria.havingTags(workflowTag(id)));
            record("steps", steps, "execute_steps_ms", executeMs, events);
            record("steps", steps, "events_in_instance", events, events);
            record("steps", steps, "raw_source_one_instance_ms",
                   timed(() -> sourceCount(store, EventCriteria.havingTags(workflowTag(id)))), events);

            long restoreMs;
            long firstAppendMs;
            try (var node = startNode(store, MODULE, new ParkingWorkflow(), tokens)) {
                restoreMs = timedOrCeiling("steps", steps, "failover_restore_ms", node, () -> awaitRunning(node, 1));
                firstAppendMs = restoreMs < 0 ? -1 : timedOrCeiling("steps", steps, "release_after_restore_ms", node, () -> {
                    node.publish(new ReleaseParked(id));
                    await().atMost(RESTORE_CEILING).untilAsserted(() -> assertThat(node.runningWorkflowIds()).isEmpty());
                });
            }
            record("steps", steps, "failover_restore_ms", restoreMs, events);
            record("steps", steps, "release_after_restore_ms", firstAppendMs, events);
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // helpers
    // ---------------------------------------------------------------------------------------------------------

    /** All N wait steps started: one waitForStep-tagged event per parked instance, checked with a single source. */
    private static void awaitParked(Node node, EventStorageEngine store, int n) {
        await().atMost(RESTORE_CEILING).pollInterval(Duration.ofMillis(200)).untilAsserted(() -> {
            assertThat(node.runningWorkflowIds()).hasSize(n);
            assertThat(sourceCount(store, waitStepCriteria())).isGreaterThanOrEqualTo(n);
        });
    }

    private static void awaitRunning(Node node, int n) {
        await().atMost(RESTORE_CEILING).pollInterval(Duration.ofMillis(100))
               .untilAsserted(() -> assertThat(node.runningWorkflowIds()).hasSize(n));
    }

    /** Runs the timed action; a ceiling timeout records -1 for the metric plus how far the node got. */
    private static long timedOrCeiling(String sweep, long size, String metric, Node node, Runnable action) {
        long start = System.nanoTime();
        try {
            action.run();
            return (System.nanoTime() - start) / 1_000_000;
        } catch (ConditionTimeoutException e) {
            record(sweep, size, metric + "_running_at_ceiling", node.runningWorkflowIds().size(), size);
            return -1;
        }
    }

    private static void publishParallel(Node node, List<Object> events) {
        ExecutorService pool = Executors.newFixedThreadPool(16);
        try {
            var failures = new AtomicInteger();
            var futures = events.stream().map(event -> pool.submit(() -> {
                try {
                    node.publish(event);
                } catch (RuntimeException e) {
                    failures.incrementAndGet();
                    throw e;
                }
            })).toList();
            for (var future : futures) {
                try {
                    future.get(RESTORE_CEILING.toSeconds(), TimeUnit.SECONDS);
                } catch (Exception e) {
                    throw new IllegalStateException("publish failed (" + failures.get() + " failures)", e);
                }
            }
        } finally {
            pool.shutdownNow();
        }
    }

    /**
     * Writes {@code count} STARTED+COMPLETED lifecycle pairs straight to the store, bypassing the engine, in batches.
     * Same metadata and tags the engine writes, so the running-workflow registry replays them on the next restore.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static void seedCompletedWorkflows(EventStorageEngine store, int count) {
        var tagResolver = new WorkflowEventTagResolver();
        int batch = 500;
        ExecutorService pool = Executors.newFixedThreadPool(8);
        try {
            List<java.util.concurrent.Future<?>> pending = new ArrayList<>();
            for (int from = 0; from < count; from += batch) {
                int start = from;
                pending.add(pool.submit(() -> {
                    List<TaggedEventMessage<?>> events = new ArrayList<>();
                    for (int i = start; i < Math.min(start + batch, count); i++) {
                        String id = "seed-" + i;
                        events.add(lifecycle(tagResolver, id, WorkflowStatus.STARTED));
                        events.add(lifecycle(tagResolver, id, WorkflowStatus.COMPLETED));
                    }
                    EventStorageEngine.AppendTransaction transaction =
                            store.appendEvents(AppendCondition.none(), null, events)
                                 .orTimeout(60, TimeUnit.SECONDS).join();
                    var commitResult = transaction.commit().orTimeout(60, TimeUnit.SECONDS).join();
                    transaction.afterCommit(commitResult).orTimeout(60, TimeUnit.SECONDS).join();
                }));
            }
            for (var future : pending) {
                future.get(10, TimeUnit.MINUTES);
            }
        } catch (Exception e) {
            throw new IllegalStateException("seeding failed", e);
        } finally {
            pool.shutdownNow();
        }
    }

    private static TaggedEventMessage<?> lifecycle(WorkflowEventTagResolver tagResolver,
                                                   String id,
                                                   WorkflowStatus status) {
        EventMessage message = new GenericEventMessage(
                new MessageType("perf", "SeededLifecycle", "0.0.1"),
                Map.of("id", id, "status", status.name()),
                MetadataUtils.create(id, status)
        );
        return new GenericTaggedEventMessage<>(message, Set.copyOf(tagResolver.resolve(message)));
    }

    private static EventCriteria lifecycleCriteria() {
        return EventSourcedRunningWorkflows.criteriaBuilder();
    }

    private static EventCriteria waitStepCriteria() {
        return EventCriteria.havingTags(Tag.of(WorkflowEventTags.TAG_WORKFLOW_EVENT_TYPE,
                                               WorkflowEventTags.TAG_VALUE_EVENT_TYPE_WAIT_STEP));
    }

    private static int sourceCount(EventStorageEngine store, EventCriteria criteria) {
        var stream = store.source(SourcingCondition.conditionFor(criteria));
        try {
            return stream.reduce(0, (count, entry) ->
                    entry.message() instanceof TerminalEventMessage ? count : count + 1)
                         .orTimeout(RESTORE_CEILING.toSeconds(), TimeUnit.SECONDS).join();
        } finally {
            stream.close();
        }
    }

    private static long timed(Runnable action) {
        long start = System.nanoTime();
        action.run();
        return (System.nanoTime() - start) / 1_000_000;
    }

    private static long usedHeap() {
        System.gc();
        try {
            Thread.sleep(300);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        System.gc();
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }

    private static void record(String sweep, long size, String metric, long value, long events) {
        String line = String.join(",", sweep, Long.toString(size), metric, Long.toString(value),
                                  Long.toString(events)) + System.lineSeparator();
        System.out.println("PERF " + line.trim());
        try {
            Files.createDirectories(OUT.toAbsolutePath().getParent());
            if (!Files.exists(OUT)) {
                Files.writeString(OUT, "sweep,size,metric,value,events" + System.lineSeparator());
            }
            Files.writeString(OUT, line, StandardCharsets.UTF_8, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
