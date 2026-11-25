/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.integrationtests.workflow.perf;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.axonserver.connector.AxonServerConnectionFactory;
import io.axoniq.axonserver.connector.impl.ServerAddress;
import io.axoniq.framework.axonserver.connector.event.AxonServerEventStorageEngine;
import io.axoniq.framework.axonserver.connector.snapshot.AxonServerSnapshotStore;
import io.axoniq.framework.integrationtests.workflow.perf.LoadWorkflows.*;
import io.axoniq.framework.workflow.index.jpa.JpaWorkflowIndex;
import io.axoniq.framework.workflow.index.jpa.JpaWorkflowIndexConfiguration;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndex;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChanges;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexRows;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.Persistence;
import io.axoniq.framework.postgresql.PostgresqlEventStorageEngine;
import io.axoniq.framework.workflow.configuration.WorkflowConfigurer;
import io.axoniq.framework.workflow.configuration.WorkflowModule;
import io.axoniq.framework.workflow.dsl.api.EventConditions;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContextFactory;
import io.axoniq.framework.workflow.runtime.execution.HibernationProbe;
import io.axoniq.framework.workflow.runtime.execution.PayloadPropertyWorkflowIdProvider;
import io.axoniq.framework.workflow.runtime.execution.WorkflowEngine;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.common.jdbc.ConnectionExecutor;
import org.axonframework.common.jdbc.ConnectionProvider;
import org.axonframework.conversion.CachingSupplier;
import org.axonframework.conversion.GeneralConverter;
import org.axonframework.eventsourcing.eventstore.AppendCondition;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.SourcingCondition;
import org.axonframework.eventsourcing.eventstore.TaggedEventMessage;
import org.axonframework.eventsourcing.snapshot.api.Snapshot;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.ProcessingLifecycle;
import org.axonframework.messaging.core.unitofwork.TransactionalUnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.transaction.Transaction;
import org.axonframework.messaging.core.unitofwork.transaction.TransactionManager;
import org.axonframework.messaging.core.unitofwork.transaction.jdbc.JdbcTransactionalExecutorProvider;
import org.axonframework.messaging.core.unitofwork.transaction.jpa.EntityManagerTransactionManager;
import org.axonframework.messaging.core.unitofwork.transaction.jpa.JpaTransactionalExecutorProvider;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventSink;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.axonframework.messaging.eventhandling.processing.EventProcessor;
import org.axonframework.messaging.eventhandling.processing.streaming.pooled.PooledStreamingEventProcessor;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jpa.JpaTokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jpa.JpaTokenStoreConfiguration;
import org.axonframework.messaging.eventstreaming.StreamingCondition;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.stream.IntStream;
import javax.sql.DataSource;

/**
 * Load test of workflow hibernation on a real event store. One JVM runs one size of one arm, so heap numbers start
 * clean; {@code run.sh} starts the store container and runs the sizes.
 * <p>
 * Phases, each recorded as {@code store,arm,size,metric,value} rows in {@code -Dperf.out}:
 * <ol>
 *     <li>Start: publish N start events of 5 production-shaped workflows ({@link LoadWorkflows}), wait until each
 *     parked on its first wait, then until each hibernated. Heap per workflow after a full GC.</li>
 *     <li>Wake: publish the awaited payment for a sample of orders at a steady rate. Latency is publish to the start
 *     of the next execute step.</li>
 *     <li>Deadlines: subscriptions whose renewal sleep ends at spread instants (accuracy), then a batch whose sleeps all
 *     end at one instant (storm). Lateness is the deadline to the first charge attempt.</li>
 *     <li>Restart: stop the node, start a new one on the same store and token store. Time until every live workflow
 *     is restored, then hibernated again, and the peak heap meanwhile.</li>
 * </ol>
 * Event store read load is counted by decorating the storage engine and the snapshot store, and on Postgres also
 * from {@code pg_stat_database}. Not an assertion suite: it fails only when a phase stops making progress before its
 * ceiling, which is the wall the report looks for.
 *
 * @author Stefan Dragisic
 */
@Tag("perf")
class HibernationLoadTest {

    private static final Logger logger = LoggerFactory.getLogger(HibernationLoadTest.class);
    private static final String STORE = System.getProperty("perf.store", "postgres");
    private static final int SIZE = Integer.getInteger("perf.size", 100);
    private static final boolean HIBERNATION = Boolean.parseBoolean(System.getProperty("perf.hibernation", "true"));
    private static final boolean SNAPSHOTS = Boolean.parseBoolean(System.getProperty("perf.snapshots", "true"));
    private static final int SEGMENTS = Integer.getInteger("perf.segments", 16);
    private static final int WAKE_SAMPLE = Integer.getInteger("perf.wakeSample", 500);
    private static final int WAKE_RATE_PER_SECOND = Integer.getInteger("perf.wakeRate", 20);
    // Start events published per second, or 0 for one burst. A burst far above what the engine handles makes its
    // checkpoints time out, which stops the run before it measures anything.
    private static final int START_RATE_PER_SECOND = Integer.getInteger("perf.startRate", 0);
    private static final int ACCURACY_SAMPLE = Integer.getInteger("perf.accuracySample", 50);
    private static final int STORM_MAX = Integer.getInteger("perf.stormMax", 10_000);
    private static final boolean RESTART = Boolean.parseBoolean(System.getProperty("perf.restart", "true"));
    private static final boolean DEADLINES = Boolean.parseBoolean(System.getProperty("perf.deadlines", "true"));
    private static final Duration CEILING = Duration.ofSeconds(Long.getLong("perf.ceilingSeconds", 900));
    private static final Path OUT = Path.of(System.getProperty("perf.out", "target/perf-results.csv"));
    private static final String PG_URL = System.getProperty("perf.pg.url", "jdbc:postgresql://localhost:55440/perf");
    private static final String AXON_SERVER = System.getProperty("perf.axonserver", "localhost:8124");
    // jpa: the JPA workflow index. false: no index, the arm the index is compared with. Every arm keeps its tokens
    // in a JPA token store, so nothing a node stores lives in memory only.
    private static final boolean INDEX = switch (System.getProperty("perf.index", "false")) {
        case "jpa" -> true;
        case "false" -> false;
        default -> throw new IllegalArgumentException("perf.index is false or jpa.");
    };
    private static final String ARM = (HIBERNATION ? "hibernation" : "resident") + (SNAPSHOTS ? "+snap" : "-snap")
            + (INDEX ? "+jpaindex" : "+jpatokens");
    // A crash instead of a clean stop before the restart: the node loses its database first, so its stop writes no
    // last rows and releases no claim.
    private static final boolean CRASH = Boolean.getBoolean("perf.crash");
    // Seconds a node keeps its hands off a segment it was told to give to another node.
    private static final int MOVE_SECONDS = Integer.getInteger("perf.moveSeconds", 30);
    // Whether a node of this run already created the tables of the JPA side.
    private final AtomicBoolean schemaCreated = new AtomicBoolean();
    // Orders woken so far, over all wake phases of the run.
    private int wokenOrders;
    // Whether the scaling test published its start events yet. Before that no workflow is expected anywhere.
    private boolean scaleStarted;
    private static final String MODULE = "load";

    /** Local names of the first wait each workflow kind starts: one such event per workflow means it parked. */
    private static final List<String> FIRST_WAITS = List.of(
            "AwaitPaymentStarted", "UntilRenewalStarted", "AwaitDecisionStarted", "AwaitPayslipStarted",
            "AwaitDeliveredStarted"
    );

    private static final List<String> WORKFLOW_NAMES = List.of(
            "OrderFulfillment", "SubscriptionRenewal", "KycReview", "LoanApplication", "ShipmentTracking"
    );

    private final Counters counters = new Counters();
    private int expectedCharges;
    private final List<AutoCloseable> resources = new ArrayList<>();

    @AfterEach
    void tearDown() throws Exception {
        for (var resource : resources.reversed()) {
            resource.close();
        }
    }

    @Test
    @Timeout(value = 6, unit = TimeUnit.HOURS)
    void hibernatedWorkflowsAtScale() throws Exception {
        var rig = STORE.equals("axonserver") ? new AxonServerRig() : new PostgresRig();
        resources.add(rig);
        record("segments", SEGMENTS);

        long heapBeforeNode = liveHeap();
        var first = new Node(rig, "node-1");
        long heapEmptyNode = liveHeap();
        record("heap_empty_node_bytes", heapEmptyNode - heapBeforeNode);

        // 1. Start N workflows and wait until they park and hibernate.
        var before = counters.snapshot(rig);
        long startNanos = System.nanoTime();
        record("start_rate_per_second", START_RATE_PER_SECOND);
        publishParallel(first, IntStream.range(0, SIZE).mapToObj(HibernationLoadTest::startEvent),
                        START_RATE_PER_SECOND);
        record("start_publish_ms", millisSince(startNanos));
        awaitPhase("start_all_parked_ms", startNanos, () -> parked() + failed() >= SIZE, first);
        record("start_failed_workflows", failed());
        counters.recordEndedSteps("start");
        if (HIBERNATION) {
            long parkedNanos = System.nanoTime();
            awaitPhase("hibernate_all_ms", startNanos, () -> hibernated(first) >= live(), first);
            record("hibernate_after_parked_ms", millisSince(parkedNanos));
        }
        counters.recordDelta("start", before, rig, SIZE);
        long heapAll = liveHeap();
        record("heap_workflows_bytes", heapAll - heapEmptyNode);
        record("heap_per_workflow_bytes", (heapAll - heapEmptyNode) / SIZE);
        record("in_memory_after_start", first.engine.workflowExecutions().size());
        record("hibernated_after_start", hibernated(first));

        // 2. Wake a sample of orders with their payment.
        wake("wake", List.of(first), rig);

        // 3. Deadlines: spread for accuracy, aligned for the storm.
        if (DEADLINES) {
            deadlines(first, rig, "accuracy", ACCURACY_SAMPLE, true);
            int storm = Math.min(STORM_MAX, Math.max(100, SIZE / 10));
            deadlines(first, rig, "storm", storm, false);
        }

        // 4. Restart: a new node claims every segment and restores every live workflow. Last, because it is the
        // phase most likely to hit a wall.
        if (RESTART) {
            restart(first, rig);
        }
        record("failed_workflows_total", failed());
        record("done", 1);
    }

    // ---------------------------------------------------------------------------------------------------------
    // Phases
    // ---------------------------------------------------------------------------------------------------------

    /**
     * Wakes the next sample of orders with their payment, published through the first of the given nodes, and waits
     * until each ran its next step and parked again. Every call takes orders no earlier call woke.
     */
    private void wake(String phase, List<Node> nodes, Rig rig) throws InterruptedException {
        var node = nodes.getFirst();
        var sample = IntStream.range(0, SIZE).filter(i -> i % 5 == 0).skip(wokenOrders).limit(WAKE_SAMPLE)
                              .mapToObj(i -> "ord-" + i).toList();
        wokenOrders += sample.size();
        int woken = wokenOrders;
        record(phase + "_sample", sample.size());
        var before = counters.snapshot(rig);
        long phaseNanos = System.nanoTime();
        long intervalNanos = TimeUnit.SECONDS.toNanos(1) / WAKE_RATE_PER_SECOND;
        for (int i = 0; i < sample.size(); i++) {
            long due = phaseNanos + i * intervalNanos;
            long wait = due - System.nanoTime();
            if (wait > 0) {
                TimeUnit.NANOSECONDS.sleep(wait);
            }
            var orderId = sample.get(i);
            Probe.paymentPublished.put(orderId, System.nanoTime());
            node.publish(new PaymentReceived(orderId, "pay-" + orderId, new BigDecimal("149.90"), "EUR", "card"));
        }
        awaitPhase(phase + "_all_ms", phaseNanos, () -> Probe.stockReserved.keySet().containsAll(sample), node);
        long[] latencies = sample.stream()
                                 .mapToLong(id -> TimeUnit.NANOSECONDS.toMillis(
                                         Probe.stockReserved.get(id) - Probe.paymentPublished.get(id)))
                                 .sorted()
                                 .toArray();
        recordPercentiles(phase + "_latency", latencies);
        counters.recordDelta(phase, before, rig, sample.size());
        // The woken orders wait for their shipment next; let them park, and hibernate, again.
        awaitPhase(phase + "_reparked_ms", phaseNanos,
                   () -> counters.appended("AwaitShipmentStarted") >= woken, node);
        if (HIBERNATION) {
            awaitPhase(phase + "_rehibernated_ms", phaseNanos, () -> hibernated(nodes) >= live(), node);
        }
        // Each woken order ran its next step once: a second run by another node would show as more steps than orders.
        record(phase + "_orders_woken_so_far", woken);
        record(phase + "_reserve_stock_steps_so_far", counters.appended("ReserveStockCompleted"));
    }

    private void restart(Node first, Rig rig) throws InterruptedException {
        record("restart_after_crash", CRASH ? 1 : 0);
        long stopNanos = System.nanoTime();
        if (CRASH) {
            first.crash();
        } else {
            first.close();
        }
        record("restart_stop_ms", millisSince(stopNanos));
        liveHeap();
        var heap = new HeapSampler();
        var before = counters.snapshot(rig);
        long startNanos = System.nanoTime();
        var node = new Node(rig, "node-2");
        record("restart_node_start_ms", millisSince(startNanos));
        awaitPhase("restart_restore_all_ms", startNanos,
                   () -> node.engine.workflowExecutions().size() + hibernated(node) >= live(), node);
        // How many workflows the claims loaded into memory, and how many they kept out of it.
        record("restart_in_memory_after_claims", node.engine.workflowExecutions().size());
        record("restart_hibernated_after_claims", hibernated(node));
        if (HIBERNATION) {
            // A restored workflow runs only once its segment caught up with the store head. When no new event arrives
            // for a segment, its workflows may wait; one unrelated event shows whether that is what holds them.
            long stallDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            while (hibernated(node) < live() && System.nanoTime() < stallDeadline) {
                sleepMillis(200);
            }
            if (hibernated(node) < live()) {
                record("restart_not_hibernated_after_60s", live() - hibernated(node));
                node.publish(new ParcelLost("nudge-" + System.nanoTime(), "unrelated event after restart"));
                record("restart_nudged_at_ms", millisSince(startNanos));
            }
            awaitPhase("restart_rehibernate_ms", startNanos, () -> hibernated(node) >= live(), node);
        }
        heap.stop();
        record("restart_peak_heap_bytes", heap.peakUsed());
        record("restart_peak_old_gen_after_gc_bytes", heap.peakOldGenAfterGc());
        counters.recordDelta("restart", before, rig, SIZE);
        record("restart_heap_after_bytes", liveHeap());
        if (CRASH) {
            // After a crash, show that the workflows still work: wake a second sample of orders on the new node.
            wake("wake_after_crash", List.of(node), rig);
        }
    }

    /**
     * Scales a running system: 2 nodes, then 3, then 1. After every change it waits until each live workflow is known
     * by exactly 1 node, and wakes a sample of orders to show that events still reach their workflow, once.
     */
    @Test
    @Timeout(value = 3, unit = TimeUnit.HOURS)
    void scalesFromTwoNodesToThreeToOne() throws Exception {
        var rig = STORE.equals("axonserver") ? new AxonServerRig() : new PostgresRig();
        resources.add(rig);
        record("segments", SEGMENTS);
        long runNanos = System.nanoTime();

        // 2 nodes: the first claims every segment, then gives half of them to the second.
        var node1 = new Node(rig, "node-1");
        awaitPhase("scale_node1_claims_all_ms", runNanos, () -> node1.segments().size() == SEGMENTS, node1);
        var node2 = new Node(rig, "node-2");
        var two = List.of(node1, node2);
        move("scale_to_2", IntStream.range(SEGMENTS / 2, SEGMENTS).boxed().toList(), node2, two, rig);

        long startNanos = System.nanoTime();
        record("start_rate_per_second", START_RATE_PER_SECOND);
        scaleStarted = true;
        publishParallel(node1, IntStream.range(0, SIZE).mapToObj(HibernationLoadTest::startEvent),
                        START_RATE_PER_SECOND);
        awaitPhase("start_all_parked_ms", startNanos, () -> parked() + failed() >= SIZE, node1);
        record("start_failed_workflows", failed());
        if (HIBERNATION) {
            awaitPhase("hibernate_all_ms", startNanos, () -> hibernated(two) >= live(), node1);
        }
        verifyScale("two_nodes", two, rig);

        // 3 nodes: the first two each give segments to the third.
        var node3 = new Node(rig, "node-3");
        var three = List.of(node1, node2, node3);
        int third = SEGMENTS / 3;
        var toThird = new ArrayList<Integer>();
        IntStream.range(0, third / 2).forEach(toThird::add);
        IntStream.range(SEGMENTS / 2, SEGMENTS / 2 + third - third / 2).forEach(toThird::add);
        move("scale_to_3", toThird, node3, three, rig);
        verifyScale("three_nodes", three, rig);

        // 1 node: the second and the third stop, and the first claims what they held.
        var before = counters.snapshot(rig);
        long downNanos = System.nanoTime();
        node2.close();
        node3.close();
        var one = List.of(node1);
        awaitPhase("scale_to_1_segments_ms", downNanos, () -> node1.segments().size() == SEGMENTS, node1);
        awaitPhase("scale_to_1_all_known_once_ms", downNanos, () -> knownExactlyOnce(one), node1);
        counters.recordDelta("scale_to_1", before, rig, SIZE);
        verifyScale("one_node", one, rig);

        record("failed_workflows_total", failed());
        record("done", 1);
    }

    /**
     * Gives the given segments to the target: every other node releases them, and stays away from them long enough for
     * the target to claim them. Waits until the target holds them and every live workflow is known by exactly 1 node.
     */
    private void move(String phase, List<Integer> segments, Node target, List<Node> nodes, Rig rig) {
        var before = counters.snapshot(rig);
        long moveNanos = System.nanoTime();
        for (Node node : nodes) {
            if (node != target) {
                segments.forEach(segment -> node.processor()
                                                .releaseSegment(segment, MOVE_SECONDS, TimeUnit.SECONDS)
                                                .orTimeout(60, TimeUnit.SECONDS)
                                                .join());
            }
        }
        awaitPhase(phase + "_segments_ms", moveNanos, () -> target.segments().containsAll(segments), target);
        awaitPhase(phase + "_all_known_once_ms", moveNanos, () -> knownExactlyOnce(nodes), target);
        counters.recordDelta(phase, before, rig, SIZE);
    }

    /** Records who holds what, then wakes a sample of orders and checks each ran its next step once. */
    private void verifyScale(String phase, List<Node> nodes, Rig rig) throws InterruptedException {
        var owners = new HashMap<String, Integer>();
        for (Node node : nodes) {
            var known = node.knownWorkflowIds();
            record(phase + "_segments_of_" + node.id, node.segments().size());
            record(phase + "_workflows_of_" + node.id, known.size());
            known.forEach(id -> owners.merge(id, 1, Integer::sum));
        }
        record(phase + "_live_workflows", live());
        record(phase + "_known_workflows", owners.size());
        record(phase + "_known_by_2_or_more_nodes", owners.values().stream().filter(count -> count > 1).count());
        record(phase + "_in_memory", nodes.stream().mapToInt(n -> n.engine.workflowExecutions().size()).sum());
        wake(phase + "_wake", nodes, rig);
        if (counters.appended("ReserveStockCompleted") != wokenOrders) {
            throw new IllegalStateException(phase + ": " + wokenOrders + " orders were woken, but "
                                                    + counters.appended("ReserveStockCompleted")
                                                    + " reserve stock steps ran");
        }
    }

    /** Whether every live workflow is known by exactly 1 of the given nodes, in memory or hibernated. */
    private boolean knownExactlyOnce(List<Node> nodes) {
        var known = new HashSet<String>();
        int total = 0;
        for (Node node : nodes) {
            var ofNode = node.knownWorkflowIds();
            total += ofNode.size();
            known.addAll(ofNode);
        }
        return total == known.size() && known.size() >= (scaleStarted ? live() : 0);
    }

    /**
     * Starts subscriptions whose renewal sleep ends at a given instant and records how late each first charge attempt
     * ran. Spread deadlines are 200 ms apart; aligned ones all end at the same instant.
     */
    private void deadlines(Node node, Rig rig, String phase, int count, boolean spread) {
        record(phase + "_count", count);
        double startRatePerMs = Math.max(0.05, SIZE / (double) Math.max(1, metric("start_all_parked_ms")));
        long publishBudgetMs = (long) (count / startRatePerMs * 1.5);
        // Leave time to park and to hibernate (a pass runs every 10 seconds) before the first deadline.
        var base = Instant.now().plusMillis(publishBudgetMs).plusSeconds(35).truncatedTo(ChronoUnit.SECONDS);
        Map<String, Instant> deadlines = new ConcurrentHashMap<>();
        var events = IntStream.range(0, count).mapToObj(i -> {
            var id = phase + "-sub-" + i;
            var renewAt = spread ? base.plusMillis(200L * i) : base;
            deadlines.put(id, renewAt);
            return (Object) new SubscriptionActivated(id, "cust-" + i, "monthly-pro", 1999, "EUR", renewAt);
        });
        var before = counters.snapshot(rig);
        long publishNanos = System.nanoTime();
        publishParallel(node, events, 0);
        record(phase + "_publish_ms", millisSince(publishNanos));
        long untilFirst = Duration.between(Instant.now(), base).toMillis();
        record(phase + "_margin_before_first_deadline_ms", untilFirst);
        if (untilFirst > 1000) {
            sleepMillis(untilFirst - 1000);
        }
        record(phase + "_hibernated_1s_before_deadline", hibernated(node));
        record(phase + "_in_memory_1s_before_deadline", node.engine.workflowExecutions().size());
        long phaseNanos = System.nanoTime();
        awaitPhase(phase + "_all_charged_ms", phaseNanos,
                   () -> Probe.charged.keySet().containsAll(deadlines.keySet()), node);
        long[] lateness = deadlines.entrySet().stream()
                                   .mapToLong(e -> Probe.charged.get(e.getKey()) - e.getValue().toEpochMilli())
                                   .sorted()
                                   .toArray();
        recordPercentiles(phase + "_lateness", lateness);
        // Let the charged subscriptions complete, so the node does not stop them halfway.
        expectedCharges += count;
        awaitPhase(phase + "_all_completed_ms", phaseNanos,
                   () -> counters.appended("ChargeCardCompleted") >= expectedCharges, node);
        counters.recordDelta(phase, before, rig, count);
    }

    // ---------------------------------------------------------------------------------------------------------
    // Workload
    // ---------------------------------------------------------------------------------------------------------

    private static Object startEvent(int i) {
        return switch (i % 5) {
            case 0 -> new OrderPlaced("ord-" + i, "cust-" + i % 9973,
                                      List.of(new OrderLine("SKU-" + i % 500, 2, new BigDecimal("49.95")),
                                              new OrderLine("SKU-" + (i + 7) % 500, 1, new BigDecimal("50.00"))),
                                      new BigDecimal("149.90"), "EUR", Instant.now());
            case 1 -> new SubscriptionActivated("sub-" + i, "cust-" + i % 9973, "monthly-pro", 1999, "EUR",
                                                Instant.now().plus(30, ChronoUnit.DAYS));
            case 2 -> new KycDocumentsSubmitted("kyc-" + i, "cust-" + i % 9973, "NL",
                                                List.of("passport", "proof-of-address"));
            case 3 -> new LoanApplicationSubmitted("loan-" + i, "cust-" + i % 9973, new BigDecimal("25000"), 60,
                                                   "car");
            default -> new ParcelHandedToCarrier("shp-" + i, "ord-" + i, "DHL", "JJD" + (100000000L + i));
        };
    }

    private static WorkflowModule<SimpleWorkflowContext> module(TransactionManager processorTransactions) {
        var module = WorkflowModule.configure(MODULE, SimpleWorkflowContext.class)
                             .definition(d -> d.declarative(c -> new OrderFulfillment()::execute)
                                               .workflowName("OrderFulfillment")
                                               .on(EventConditions.fromType(OrderPlaced.class))
                                               .customized((c, w) -> w.workflowIdProvider(
                                                       new PayloadPropertyWorkflowIdProvider("orderId"))))
                             .definition(d -> d.declarative(c -> new SubscriptionRenewal()::execute)
                                               .workflowName("SubscriptionRenewal")
                                               .on(EventConditions.fromType(SubscriptionActivated.class))
                                               .customized((c, w) -> w.workflowIdProvider(
                                                       new PayloadPropertyWorkflowIdProvider("subscriptionId"))))
                             .definition(d -> d.declarative(c -> new KycReview()::execute)
                                               .workflowName("KycReview")
                                               .on(EventConditions.fromType(KycDocumentsSubmitted.class))
                                               .customized((c, w) -> w.workflowIdProvider(
                                                       new PayloadPropertyWorkflowIdProvider("caseId"))))
                             .definition(d -> d.declarative(c -> new LoanApplication()::execute)
                                               .workflowName("LoanApplication")
                                               .on(EventConditions.fromType(LoanApplicationSubmitted.class))
                                               .customized((c, w) -> w.workflowIdProvider(
                                                       new PayloadPropertyWorkflowIdProvider("applicationId"))))
                             .definition(d -> d.declarative(c -> new ShipmentTracking()::execute)
                                               .workflowName("ShipmentTracking")
                                               .on(EventConditions.fromType(ParcelHandedToCarrier.class))
                                               .customized((c, w) -> w.workflowIdProvider(
                                                       new PayloadPropertyWorkflowIdProvider("shipmentId"))))
                             .processorConfiguration(pc -> {
                                 pc.initialSegmentCount(SEGMENTS)
                                   .workerExecutor(() -> Executors.newScheduledThreadPool(SEGMENTS));
                                 // The JPA transaction of the token store and the index, around the unit of work
                                 // the processor already has.
                                 return pc.unitOfWorkFactory(
                                         new TransactionalUnitOfWorkFactory(processorTransactions,
                                                                            pc.unitOfWorkFactory()));
                             })
                             // The default history keeps every workflow's history in memory, which would hide what
                             // hibernation saves. Production setups persist it or turn it off.
                             .withoutHistory()
                             .contextFactory(c -> new SimpleWorkflowContextFactory());
        if (HIBERNATION) {
            // A pass every 10 seconds, the interval the phases of this test are timed for.
            module.withHibernation().hibernationInterval(10_000);
        }
        return module;
    }

    // ---------------------------------------------------------------------------------------------------------
    // Node and stores
    // ---------------------------------------------------------------------------------------------------------

    private final class Node implements AutoCloseable {

        private final String id;
        // The JPA side of this node: its token store and, with the index, its rows.
        private final Jpa jpa;
        private final AxonConfiguration configuration;
        private final WorkflowEngine engine;
        private final AtomicBoolean closed = new AtomicBoolean();

        private Node(Rig rig, String id) {
            this.id = id;
            // Connections of its own, so a crash of this node takes only its connections away.
            this.jpa = new Jpa(schemaCreated.compareAndSet(false, true));
            var configurer = WorkflowConfigurer.create();
            if (SNAPSHOTS) {
                configurer.snapshots(snapshots -> snapshots.enabled(true));
            } else {
                // Off explicitly: without a setting, snapshots are on whenever a snapshot store exists.
                configurer.snapshots(snapshots -> snapshots.enabled(false));
            }
            if (INDEX) {
                configurer.registerWorkflowIndex(cfg -> new CountingIndex(
                        new JpaWorkflowIndex(jpa.transactions,
                                             cfg.getComponent(GeneralConverter.class),
                                             JpaWorkflowIndexConfiguration.DEFAULT),
                        counters
                ));
            }
            var engineOfNode = new ConcurrentHashMap<String, EventStorageEngine>();
            Function<Configuration, EventStorageEngine> storeOfNode =
                    cfg -> engineOfNode.computeIfAbsent("engine", key -> rig.engine(cfg));
            configurer.componentRegistry(cr -> {
                cr.disableEnhancer(
                          "io.axoniq.framework.axonserver.connector.configuration.AxonServerConfigurationEnhancer")
                  .disableEnhancer("io.axoniq.framework.postgresql.PostgresqlConfigurationEnhancer")
                  .registerComponent(EventStorageEngine.class,
                                     cfg -> new CountingStore(storeOfNode.apply(cfg), counters))
                  .registerComponent(SnapshotStore.class,
                                     cfg -> new CountingSnapshots(rig.snapshots(cfg, storeOfNode.apply(cfg)),
                                                                  counters))
                  .registerComponent(TokenStore.class, cfg -> new JpaTokenStore(
                          jpa.transactions,
                          cfg.getComponent(GeneralConverter.class),
                          JpaTokenStoreConfiguration.DEFAULT.nodeId(id)))
                  .registerModule(module(jpa.transactionManager));
                var transactions = rig.transactionManager();
                if (transactions != null) {
                    cr.registerComponent(TransactionManager.class, cfg -> transactions);
                }
            });
            this.configuration = configurer.start();
            resources.add(this);
            this.engine = findEngine(configuration)
                    .orElseThrow(() -> new IllegalStateException("No WorkflowEngine found"));
        }

        /** The engine lives in a module nested in the workflow module. */
        private static java.util.Optional<WorkflowEngine> findEngine(Configuration configuration) {
            return configuration.getComponents(WorkflowEngine.class).values().stream().findFirst()
                                .or(() -> configuration.getModuleConfigurations().stream()
                                                       .map(Node::findEngine)
                                                       .flatMap(java.util.Optional::stream)
                                                       .findFirst());
        }

        void publish(Object payload) {
            var type = configuration.getComponent(MessageTypeResolver.class).resolveOrThrow(payload);
            var message = new GenericEventMessage(type, payload);
            var sink = configuration.getComponent(EventSink.class);
            configuration.getComponent(UnitOfWorkFactory.class)
                         .create()
                         .executeWithResult(ctx -> sink.publish(ctx, message))
                         .orTimeout(60, TimeUnit.SECONDS)
                         .join();
        }

        /** The event processor of the workflow engine. */
        PooledStreamingEventProcessor processor() {
            return findProcessor(configuration)
                    .orElseThrow(() -> new IllegalStateException("No workflow processor found"));
        }

        private static Optional<PooledStreamingEventProcessor> findProcessor(Configuration configuration) {
            return configuration.getComponents(EventProcessor.class).values().stream()
                                .filter(PooledStreamingEventProcessor.class::isInstance)
                                .map(PooledStreamingEventProcessor.class::cast)
                                .filter(processor -> MODULE.equals(processor.name()))
                                .findFirst()
                                .or(() -> configuration.getModuleConfigurations().stream()
                                                       .map(Node::findProcessor)
                                                       .flatMap(Optional::stream)
                                                       .findFirst());
        }

        /** The segments this node holds. */
        Set<Integer> segments() {
            return processor().processingStatus().keySet();
        }

        /** The workflows this node knows: those in memory and those it holds hibernated. */
        Set<String> knownWorkflowIds() {
            var known = new HashSet<String>(HibernationProbe.hibernatedIds(engine));
            engine.workflowExecutions().forEach(execution -> known.add(execution.workflowId()));
            return known;
        }

        /**
         * Stops the node the way a crash does for the database: its connections go first, so nothing its stop still
         * tries to write, or to release, arrives.
         */
        void crash() {
            if (closed.compareAndSet(false, true)) {
                jpa.close();
                try {
                    configuration.shutdown();
                } catch (RuntimeException e) {
                    logger.info("The crashed node stopped with {}", e.toString());
                }
            }
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                configuration.shutdown();
                jpa.close();
            }
        }
    }

    interface Rig extends AutoCloseable {

        EventStorageEngine engine(Configuration configuration);

        SnapshotStore snapshots(Configuration configuration, EventStorageEngine engine);

        @Nullable TransactionManager transactionManager();

        /** Server-side read counters, or an empty map when the store has none. */
        Map<String, Long> serverReads();
    }

    static final class PostgresRig implements Rig {

        private final HikariDataSource dataSource;

        PostgresRig() {
            var config = new HikariConfig();
            config.setJdbcUrl(PG_URL);
            config.setUsername("perf");
            config.setPassword("perf");
            config.setMaximumPoolSize(Integer.getInteger("perf.pg.pool", 40));
            config.setAutoCommit(false);
            this.dataSource = new HikariDataSource(config);
        }

        @Override
        public EventStorageEngine engine(Configuration configuration) {
            return new PostgresqlEventStorageEngine(dataSource, configuration.getComponent(EventConverter.class));
        }

        @Override
        public SnapshotStore snapshots(Configuration configuration, EventStorageEngine engine) {
            return (SnapshotStore) engine;
        }

        @Override
        public TransactionManager transactionManager() {
            return new UnitOfWorkConnections(dataSource);
        }

        @Override
        public Map<String, Long> serverReads() {
            var sql = "select tup_returned, tup_fetched, blks_read, blks_hit, xact_commit "
                    + "from pg_stat_database where datname = current_database()";
            try (var connection = dataSource.getConnection();
                 var statement = connection.createStatement();
                 var rows = statement.executeQuery(sql)) {
                rows.next();
                var reads = Map.of("pg_tup_returned", rows.getLong(1), "pg_tup_fetched", rows.getLong(2),
                                   "pg_blks_read", rows.getLong(3), "pg_blks_hit", rows.getLong(4),
                                   "pg_xact_commit", rows.getLong(5));
                connection.commit();
                return reads;
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public void close() {
            dataSource.close();
        }
    }

    static final class AxonServerRig implements Rig {

        private final AxonServerConnectionFactory factory;
        private final AxonServerConnection connection;

        AxonServerRig() {
            var address = AXON_SERVER.split(":");
            this.factory = AxonServerConnectionFactory.forClient("hibernation-perf")
                                                      .routingServers(new ServerAddress(address[0],
                                                                                        Integer.parseInt(address[1])))
                                                      .build();
            this.connection = factory.connect("default");
            // connect() returns before the channel is up; the first call would fail with UNAVAILABLE.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            while (!connection.isReady() && System.nanoTime() < deadline) {
                sleepMillis(200);
            }
            if (!connection.isReady()) {
                throw new IllegalStateException("Axon Server at " + AXON_SERVER + " is not ready");
            }
        }

        @Override
        public EventStorageEngine engine(Configuration configuration) {
            return new AxonServerEventStorageEngine(connection, configuration.getComponent(EventConverter.class));
        }

        @Override
        public SnapshotStore snapshots(Configuration configuration, EventStorageEngine engine) {
            return new AxonServerSnapshotStore(connection, configuration.getComponent(EventConverter.class));
        }

        @Override
        public @Nullable TransactionManager transactionManager() {
            return null;
        }

        @Override
        public Map<String, Long> serverReads() {
            return Map.of();
        }

        @Override
        public void close() {
            connection.disconnect();
            factory.shutdown();
        }
    }

    /**
     * Gives every unit of work one lazily opened JDBC connection, committed or rolled back with it, the way a Spring
     * transaction manager does in production. The Postgres storage engine appends through it.
     */
    private record UnitOfWorkConnections(DataSource dataSource) implements TransactionManager {

        @Override
        public Transaction startTransaction() {
            return new Transaction() {
                @Override
                public void commit() {
                }

                @Override
                public void rollback() {
                }
            };
        }

        @Override
        public void attachToProcessingLifecycle(ProcessingLifecycle lifecycle) {
            lifecycle.runOnPreInvocation(pc -> {
                var connection = new LazyConnection(dataSource);
                pc.putResource(JdbcTransactionalExecutorProvider.SUPPLIER_KEY,
                               CachingSupplier.of(() -> new ConnectionExecutor(connection)));
                pc.onCommit(p -> {
                    try {
                        connection.commit();
                        return CompletableFuture.completedFuture(null);
                    } catch (SQLException e) {
                        return CompletableFuture.failedFuture(e);
                    } finally {
                        connection.close();
                    }
                });
                pc.onError((p, phase, e) -> {
                    try {
                        connection.rollback();
                    } catch (SQLException se) {
                        e.addSuppressed(se);
                    } finally {
                        connection.close();
                    }
                });
            });
        }
    }

    private static final class LazyConnection implements ConnectionProvider {

        private final DataSource dataSource;
        private @Nullable Connection connection;

        private LazyConnection(DataSource dataSource) {
            this.dataSource = dataSource;
        }

        @Override
        public synchronized Connection getConnection() throws SQLException {
            if (connection == null) {
                connection = dataSource.getConnection();
                connection.setAutoCommit(false);
            }
            return connection;
        }

        synchronized void commit() throws SQLException {
            if (connection != null) {
                connection.commit();
            }
        }

        synchronized void rollback() throws SQLException {
            if (connection != null) {
                connection.rollback();
            }
        }

        synchronized void close() {
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                // the pool replaces a connection that fails to close
            }
        }
    }

    /**
     * The JPA side of a node: 1 persistence unit for the token store and the index, on a connection pool of its own, so
     * it never waits for a connection of the event store and a crash of the node takes only its own connections.
     */
    static final class Jpa implements AutoCloseable {

        private final HikariDataSource dataSource;
        private final EntityManagerFactory factory;
        final JpaTransactionalExecutorProvider transactions;
        final EntityManagerTransactionManager transactionManager;

        Jpa(boolean createSchema) {
            this(createSchema ? "create" : "none");
        }

        /**
         * Creates the JPA side of a node, with the given schema action of the persistence provider: {@code create},
         * {@code update} or {@code none}.
         */
        Jpa(String schemaAction) {
            var config = new HikariConfig();
            config.setJdbcUrl(PG_URL);
            config.setUsername("perf");
            config.setPassword("perf");
            config.setMaximumPoolSize(Integer.getInteger("perf.jpa.pool", SEGMENTS + 16));
            config.setAutoCommit(false);
            this.dataSource = new HikariDataSource(config);
            this.factory = Persistence.createEntityManagerFactory("workflow-perf", Map.of(
                    "jakarta.persistence.nonJtaDataSource", dataSource,
                    // Only the first node of a run creates the tables.
                    "jakarta.persistence.schema-generation.database.action", schemaAction
            ));
            this.transactions = new JpaTransactionalExecutorProvider(factory);
            // One entity manager per thread, with an empty persistence context for every new transaction.
            ThreadLocal<EntityManager> entityManagers = ThreadLocal.withInitial(factory::createEntityManager);
            this.transactionManager = new EntityManagerTransactionManager(() -> {
                var entityManager = entityManagers.get();
                if (!entityManager.getTransaction().isActive()) {
                    entityManager.clear();
                }
                return entityManager;
            });
        }

        @Override
        public void close() {
            factory.close();
            dataSource.close();
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // Read load
    // ---------------------------------------------------------------------------------------------------------

    private static final class Counters {

        private final LongAdder sourceCalls = new LongAdder();
        private final LongAdder sourcedEvents = new LongAdder();
        private final LongAdder streamCalls = new LongAdder();
        private final LongAdder appendCalls = new LongAdder();
        private final LongAdder appendedEvents = new LongAdder();
        private final LongAdder snapshotLoads = new LongAdder();
        private final LongAdder snapshotHits = new LongAdder();
        private final LongAdder snapshotStores = new LongAdder();
        private final LongAdder indexLoads = new LongAdder();
        private final LongAdder indexLoadedRows = new LongAdder();
        private final LongAdder indexLoadNanos = new LongAdder();
        private final LongAdder indexStores = new LongAdder();
        private final LongAdder indexStoredChanges = new LongAdder();
        private final LongAdder indexStoreNanos = new LongAdder();
        private final AtomicLong indexStoreMaxNanos = new AtomicLong();
        private final AtomicLong indexStoreMaxChanges = new AtomicLong();
        private final Map<String, AtomicInteger> appendedByName = new ConcurrentHashMap<>();

        /** Records how often each step or workflow ended in a timeout or a failure, by event name. */
        void recordEndedSteps(String phase) {
            appendedByName.forEach((name, count) -> {
                if (name.endsWith("TimedOut") || name.endsWith("Failed")) {
                    HibernationLoadTest.record(phase + "_appended_" + name, count.get());
                }
            });
        }

        int appended(String localName) {
            var count = appendedByName.get(localName);
            return count == null ? 0 : count.get();
        }

        Map<String, Long> snapshot(Rig rig) {
            var values = new java.util.LinkedHashMap<String, Long>();
            values.put("source_calls", sourceCalls.sum());
            values.put("sourced_events", sourcedEvents.sum());
            values.put("stream_calls", streamCalls.sum());
            values.put("append_calls", appendCalls.sum());
            values.put("appended_events", appendedEvents.sum());
            values.put("snapshot_loads", snapshotLoads.sum());
            values.put("snapshot_hits", snapshotHits.sum());
            values.put("snapshot_stores", snapshotStores.sum());
            if (INDEX) {
                values.put("index_loads", indexLoads.sum());
                values.put("index_loaded_rows", indexLoadedRows.sum());
                values.put("index_load_ms", indexLoadNanos.sum() / 1_000_000);
                values.put("index_stores", indexStores.sum());
                values.put("index_stored_changes", indexStoredChanges.sum());
                values.put("index_store_ms", indexStoreNanos.sum() / 1_000_000);
            }
            values.putAll(rig.serverReads());
            return values;
        }

        void recordDelta(String phase, Map<String, Long> before, Rig rig, int workflows) {
            snapshot(rig).forEach((name, value) -> {
                long delta = value - before.getOrDefault(name, 0L);
                HibernationLoadTest.record(phase + "_" + name, delta);
                if (name.equals("sourced_events") || name.equals("source_calls")
                        || name.equals("pg_tup_fetched") || name.equals("pg_tup_returned")) {
                    HibernationLoadTest.record(phase + "_" + name + "_per_workflow_x100",
                                               delta * 100 / Math.max(1, workflows));
                }
            });
            if (INDEX) {
                // The slowest single store since the previous phase, and the most changes 1 store carried.
                HibernationLoadTest.record(phase + "_index_store_max_ms",
                                           indexStoreMaxNanos.getAndSet(0) / 1_000_000);
                HibernationLoadTest.record(phase + "_index_store_max_changes", indexStoreMaxChanges.getAndSet(0));
            }
        }
    }

    private record CountingIndex(WorkflowIndex delegate, Counters counters) implements WorkflowIndex {

        @Override
        public CompletableFuture<Boolean> isComplete(String module, @Nullable ProcessingContext context) {
            return delegate.isComplete(module, context);
        }

        @Override
        public CompletableFuture<WorkflowIndexRows> load(String module, Segment segment,
                                                         @Nullable ProcessingContext context) {
            counters.indexLoads.increment();
            long start = System.nanoTime();
            return delegate.load(module, segment, context).whenComplete((rows, failure) -> {
                counters.indexLoadNanos.add(System.nanoTime() - start);
                if (rows != null) {
                    counters.indexLoadedRows.add(rows.runningWorkflowIds().size() + rows.waits().size());
                }
            });
        }

        @Override
        public CompletableFuture<Void> store(String module, WorkflowIndexChanges changes,
                                             @Nullable ProcessingContext context) {
            counters.indexStores.increment();
            counters.indexStoredChanges.add(changes.changes().size());
            counters.indexStoreMaxChanges.accumulateAndGet(changes.changes().size(), Math::max);
            long start = System.nanoTime();
            return delegate.store(module, changes, context).whenComplete((stored, failure) -> {
                long nanos = System.nanoTime() - start;
                counters.indexStoreNanos.add(nanos);
                counters.indexStoreMaxNanos.accumulateAndGet(nanos, Math::max);
            });
        }

        @Override
        public boolean storesWithToken() {
            return delegate.storesWithToken();
        }
    }

    private record CountingStore(EventStorageEngine delegate, Counters counters) implements EventStorageEngine {

        @Override
        public CompletableFuture<AppendTransaction<?>> appendEvents(AppendCondition condition,
                                                                    @Nullable ProcessingContext context,
                                                                    List<TaggedEventMessage<?>> events) {
            counters.appendCalls.increment();
            counters.appendedEvents.add(events.size());
            for (var event : events) {
                counters.appendedByName.computeIfAbsent(event.event().type().qualifiedName().localName(),
                                                        name -> new AtomicInteger())
                                       .incrementAndGet();
            }
            return delegate.appendEvents(condition, context, events);
        }

        @Override
        public MessageStream<EventMessage> source(SourcingCondition condition, @Nullable ProcessingContext context) {
            counters.sourceCalls.increment();
            return delegate.source(condition, context).onNext(entry -> counters.sourcedEvents.increment());
        }

        @Override
        public MessageStream<EventMessage> stream(StreamingCondition condition) {
            counters.streamCalls.increment();
            return delegate.stream(condition);
        }

        @Override
        public CompletableFuture<TrackingToken> firstToken() {
            return delegate.firstToken();
        }

        @Override
        public CompletableFuture<TrackingToken> latestToken() {
            return delegate.latestToken();
        }

        @Override
        public CompletableFuture<TrackingToken> tokenAt(Instant at) {
            return delegate.tokenAt(at);
        }

        @Override
        public void describeTo(ComponentDescriptor descriptor) {
            descriptor.describeWrapperOf(delegate);
        }
    }

    private record CountingSnapshots(SnapshotStore delegate, Counters counters) implements SnapshotStore {

        @Override
        public CompletableFuture<Void> store(QualifiedName name, Object id, Snapshot snapshot,
                                             @Nullable ProcessingContext context) {
            counters.snapshotStores.increment();
            return delegate.store(name, id, snapshot, context);
        }

        @Override
        public CompletableFuture<Snapshot> load(QualifiedName name, Object id, @Nullable ProcessingContext context) {
            counters.snapshotLoads.increment();
            return delegate.load(name, id, context).whenComplete((snapshot, failure) -> {
                if (snapshot != null) {
                    counters.snapshotHits.increment();
                }
            });
        }

        @Override
        public void describeTo(ComponentDescriptor descriptor) {
            descriptor.describeWrapperOf(delegate);
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------------------------------------

    /** Workflows of the main population that ended in FAILED, for example after an execute step timed out. */
    private int failed() {
        return WORKFLOW_NAMES.stream().mapToInt(name -> counters.appended(name + "Failed")).sum();
    }

    /** Workflows of the main population that should still be live: every one that did not fail. */
    private int live() {
        return SIZE - failed();
    }

    private int parked() {
        return FIRST_WAITS.stream().mapToInt(counters::appended).sum();
    }

    private static int hibernated(Node node) {
        return HibernationProbe.hibernatedCount(node.engine);
    }

    private static int hibernated(List<Node> nodes) {
        return nodes.stream().mapToInt(HibernationLoadTest::hibernated).sum();
    }

    /**
     * Waits until the condition holds and records the time since {@code fromNanos}. Past the ceiling it records the
     * progress it saw and fails the run: that is the wall.
     */
    private void awaitPhase(String metric, long fromNanos, BooleanSupplier done, Node node) {
        long deadline = System.nanoTime() + CEILING.toNanos();
        long lastProgress = System.nanoTime();
        while (!done.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                record(metric, -1);
                record(metric + "_parked_at_ceiling", parked());
                record(metric + "_in_memory_at_ceiling", node.engine.workflowExecutions().size());
                record(metric + "_hibernated_at_ceiling", hibernated(node));
                record(metric + "_failed_at_ceiling", failed());
                throw new IllegalStateException(metric + " did not finish within " + CEILING);
            }
            if (System.nanoTime() - lastProgress > TimeUnit.SECONDS.toNanos(15)) {
                lastProgress = System.nanoTime();
                logger.info("PERF-PROGRESS {} after {} s: parked={} in_memory={} hibernated={}",
                            metric, millisSince(fromNanos) / 1000, parked(),
                            node.engine.workflowExecutions().size(), hibernated(node));
            }
            sleepMillis(100);
        }
        record(metric, millisSince(fromNanos));
    }

    private static void publishParallel(Node node, java.util.stream.Stream<Object> events, int ratePerSecond) {
        ExecutorService pool = Executors.newFixedThreadPool(16);
        try {
            var failures = new AtomicInteger();
            long startNanos = System.nanoTime();
            var submitted = new AtomicLong();
            var futures = events.peek(event -> {
                if (ratePerSecond > 0) {
                    // Hold the next event until its turn at the given rate.
                    long dueNanos = startNanos + submitted.getAndIncrement() * 1_000_000_000L / ratePerSecond;
                    long waitMillis = (dueNanos - System.nanoTime()) / 1_000_000;
                    if (waitMillis > 0) {
                        sleepMillis(waitMillis);
                    }
                }
            }).map(event -> CompletableFuture.runAsync(() -> {
                try {
                    node.publish(event);
                } catch (RuntimeException e) {
                    if (failures.incrementAndGet() <= 5) {
                        e.printStackTrace();
                    }
                }
            }, pool)).toList();
            CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
                             .orTimeout(CEILING.toSeconds(), TimeUnit.SECONDS)
                             .join();
            record("publish_failures", failures.get());
        } finally {
            pool.shutdownNow();
        }
    }

    /** Samples the heap every 200 ms: the peak in use, and the peak of the old generation right after a GC. */
    private static final class HeapSampler {

        private final AtomicLong peakUsed = new AtomicLong();
        private final AtomicLong peakOldAfterGc = new AtomicLong();
        private final AtomicBoolean running = new AtomicBoolean(true);
        private final Thread thread;

        private HeapSampler() {
            var memory = ManagementFactory.getMemoryMXBean();
            var oldGen = ManagementFactory.getMemoryPoolMXBeans().stream()
                                          .filter(pool -> pool.getName().contains("Old Gen"))
                                          .findFirst();
            thread = Thread.ofPlatform().daemon().name("heap-sampler").start(() -> {
                while (running.get()) {
                    peakUsed.accumulateAndGet(memory.getHeapMemoryUsage().getUsed(), Math::max);
                    oldGen.map(MemoryPoolMXBean::getCollectionUsage)
                          .ifPresent(usage -> peakOldAfterGc.accumulateAndGet(usage.getUsed(), Math::max));
                    sleepMillis(200);
                }
            });
        }

        void stop() {
            running.set(false);
        }

        long peakUsed() {
            return peakUsed.get();
        }

        long peakOldGenAfterGc() {
            return peakOldAfterGc.get();
        }
    }

    private static long liveHeap() {
        for (int i = 0; i < 3; i++) {
            System.gc();
            sleepMillis(300);
        }
        var usage = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
        return usage.getUsed();
    }

    private static long millisSince(long nanos) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - nanos);
    }

    private static void sleepMillis(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private static void recordPercentiles(String metric, long[] sorted) {
        if (sorted.length == 0) {
            return;
        }
        record(metric + "_p50_ms", sorted[(int) Math.floor(0.50 * (sorted.length - 1))]);
        record(metric + "_p99_ms", sorted[(int) Math.floor(0.99 * (sorted.length - 1))]);
        record(metric + "_max_ms", sorted[sorted.length - 1]);
        record(metric + "_min_ms", sorted[0]);
    }

    private static final Map<String, Long> METRICS = new ConcurrentHashMap<>();

    private static long metric(String name) {
        return METRICS.getOrDefault(name, 0L);
    }

    private static void record(String metric, long value) {
        METRICS.put(metric, value);
        var line = String.join(",", STORE, ARM, Integer.toString(SIZE), metric, Long.toString(value));
        logger.info("PERF {}", line);
        try {
            Files.createDirectories(OUT.toAbsolutePath().getParent());
            if (!Files.exists(OUT)) {
                Files.writeString(OUT, "store,arm,size,metric,value" + System.lineSeparator());
            }
            Files.writeString(OUT, line + System.lineSeparator(), StandardCharsets.UTF_8, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
