/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.integrationtests.workflow.perf;

import io.axoniq.framework.integrationtests.workflow.perf.HibernationLoadTest.AxonServerRig;
import io.axoniq.framework.integrationtests.workflow.perf.HibernationLoadTest.Jpa;
import io.axoniq.framework.integrationtests.workflow.perf.HibernationLoadTest.PostgresRig;
import io.axoniq.framework.integrationtests.workflow.perf.HibernationLoadTest.Rig;
import io.axoniq.framework.workflow.configuration.WorkflowConfigurer;
import io.axoniq.framework.workflow.configuration.WorkflowModule;
import io.axoniq.framework.workflow.dsl.api.EventConditions;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContextFactory;
import io.axoniq.framework.workflow.index.jpa.JpaWorkflowIndex;
import io.axoniq.framework.workflow.index.jpa.JpaWorkflowIndexConfiguration;
import io.axoniq.framework.workflow.runtime.execution.HibernationProbe;
import io.axoniq.framework.workflow.runtime.execution.PayloadPropertyWorkflowIdProvider;
import io.axoniq.framework.workflow.runtime.execution.WorkflowEngine;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndex;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChanges;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexRows;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.conversion.GeneralConverter;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.TransactionalUnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.transaction.TransactionManager;
import org.axonframework.messaging.eventhandling.EventSink;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.processing.EventProcessor;
import org.axonframework.messaging.eventhandling.processing.streaming.pooled.PooledStreamingEventProcessor;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jpa.JpaTokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jpa.JpaTokenStoreConfiguration;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.axoniq.framework.workflow.dsl.api.EventAssociationsUtils.equalsTo;
import static io.axoniq.framework.workflow.dsl.api.EventAssociationsUtils.payloadProperty;
import static io.axoniq.framework.workflow.runtime.association.Associations.associate;
import static org.assertj.core.api.Assertions.fail;

/**
 * A seeded fault campaign for the workflow index, on real infrastructure only: events on Axon Server, the token store
 * and the workflow index in PostgreSQL through JPA.
 * <p>
 * One seed gives one sequence of operations on a small cluster: cases are opened, approved and closed, while nodes
 * crash, restart, join and leave, and the whole cluster goes down. A crash cuts the database connections of the node
 * first, so nothing its stop still writes arrives. Signals are also published while the owner of a case is down.
 * <p>
 * The oracle is the case itself. Each case is one workflow that waits for its approval, runs one effect, waits for its
 * close, and runs one more effect. A workflow or a wait that a segment claim loses shows as an effect that never runs.
 * A workflow that two nodes drive shows as an effect that runs twice. At the end one node claims every segment and
 * must know exactly the cases that are still open, every open case is driven to its end, and the index must hold no
 * row.
 * <p>
 * The arm picks what the nodes run with, so a run without an index is the control of the campaign itself:
 * {@code noindex}, {@code indexed}, {@code owntx} (an index that does not store with the token), {@code toggled} (a
 * seeded mix of nodes with and without an index), {@code enable} (the index is switched on for a running
 * population, on 2 nodes that start at once) and {@code readfaults} (an index whose reads fail now and then, so a
 * segment claim falls back to sourcing).
 *
 * @author Stefan Dragisic
 */
class WorkflowIndexFaultCampaignTest {

    private static final Logger logger = LoggerFactory.getLogger(WorkflowIndexFaultCampaignTest.class);

    private static final long SEED = Long.getLong("campaign.seed", 1L);
    private static final String ARM = System.getProperty("campaign.arm", "indexed");
    // Where the events are: axonserver, or postgres for the PostgreSQL event store.
    private static final String STORE = System.getProperty("campaign.store", "axonserver");
    private static final int OPERATIONS = Integer.getInteger("campaign.ops", 60);
    private static final Duration SETTLE = Duration.ofSeconds(Long.getLong("campaign.settleSeconds", 90L));
    private static final Path TRACE = Path.of(System.getProperty("campaign.trace", "target/campaign-trace.log"));
    private static final String PG_URL = System.getProperty("perf.pg.url", "jdbc:postgresql://localhost:55440/perf");

    private static final String MODULE = "campaign";
    private static final int SEGMENTS = 4;
    private static final Duration BRIEF_TIMEOUT = Duration.ofSeconds(20);
    // A crash window lasts until the claims of the crashed node time out, so an expiry may run this much late.
    private static final Duration EXPIRY_GRACE = Duration.ofSeconds(60);
    // How long an approved case sleeps before it waits for its close. Segment claims run during that sleep.
    private static final Duration COOLDOWN = Duration.ofSeconds(8);
    // The share of the index reads that fail in the readfaults arm.
    private static final double READ_FAULT_RATE =
            Double.parseDouble(System.getProperty("campaign.readFaultRate", "0.3"));

    private static final Map<String, AtomicInteger> APPROVED = new ConcurrentHashMap<>();
    private static final Map<String, AtomicInteger> ARCHIVED = new ConcurrentHashMap<>();
    private static final Map<String, AtomicInteger> EXPIRED = new ConcurrentHashMap<>();

    private final Random random = new Random(SEED);
    private final long startedAt = System.nanoTime();
    private final List<String> trace = new ArrayList<>();
    private final Map<String, Case> cases = new LinkedHashMap<>();
    private final List<Node> nodes = new ArrayList<>();
    private final List<AutoCloseable> resources = java.util.Collections.synchronizedList(new ArrayList<>());
    private final AtomicBoolean schemaCreated = new AtomicBoolean();
    private int nodeCount;
    private int toggledNodes;
    // The evidence that the faults of this run landed.
    private int crashesOfANodeWithCases;
    private int signalsWhileOwnerDown;
    private int segmentMoves;
    private int splitsAndMerges;
    // Seeded apart from the operations, so the faults do not shift the sequence of operations.
    private final Random readFaults = new Random(SEED * 31 + 7);
    private final AtomicInteger injectedReadFaults = new AtomicInteger();
    // Grows with a split and shrinks with a merge.
    private int segments = SEGMENTS;
    private int caseCount;
    private boolean indexEnabled;

    record CaseOpened(String caseId, boolean brief) {

    }

    record CaseApproved(String caseId) {

    }

    record CaseClosed(String caseId) {

    }

    private enum IndexMode {
        NONE, WITH_TOKEN, OWN_TRANSACTION, FAILING_READS
    }

    private enum Stage {
        OPENED, AWAITING_APPROVAL, APPROVAL_SENT, COOLING_DOWN, AWAITING_CLOSE, CLOSE_SENT, EXPIRING, DONE
    }

    private static final class Case {

        private final String id;
        private final boolean brief;
        private final Instant openedAt = Instant.now();
        private Stage stage;
        private @Nullable Instant approvedAt;

        private Case(String id, boolean brief) {
            this.id = id;
            this.brief = brief;
            this.stage = brief ? Stage.EXPIRING : Stage.OPENED;
        }
    }

    @AfterEach
    void tearDown() throws Exception {
        for (var resource : resources.reversed()) {
            try {
                resource.close();
            } catch (Exception e) {
                logger.warn("A resource of the campaign did not close: {}", e.toString());
            }
        }
        if (!trace.isEmpty()) {
            // The test that the arm skips has no trace, and must not replace the one of the test that ran.
            Files.createDirectories(TRACE.toAbsolutePath().getParent());
            Files.write(TRACE, trace);
        }
    }

    private static Rig newRig() {
        return STORE.equals("postgres") ? new PostgresRig() : new AxonServerRig();
    }

    /**
     * The smallest run that breaks the rollout rule of the workflow index: a node without an index runs between 2
     * nodes with one. The case that opened on it has no row, so the last node must not find it. The test fails while
     * that is so.
     */
    @Test
    @Timeout(value = 10, unit = TimeUnit.MINUTES)
    void aCaseOpenedWhileTheIndexWasOffIsFoundWhenItIsOnAgain() throws Exception {
        Assumptions.assumeTrue(ARM.equals("offon"));
        var rig = newRig();
        resources.add(rig);
        var client = new Client(rig);

        // given: a case that opened on a node with an index, and a case that opened on a node without one
        nodes.add(new Node(rig, "with-index", IndexMode.WITH_TOKEN));
        open(client, false);
        settle("after the first case opened");
        stopAllNodes();
        nodes.add(new Node(rig, "without-index", IndexMode.NONE));
        open(client, false);
        settle("after the second case opened");
        stopAllNodes();

        // when: a node with an index claims every segment
        var last = new Node(rig, "with-index-again", IndexMode.WITH_TOKEN);
        nodes.add(last);

        // then: it knows both cases
        awaitOrFail("the last node knows both open cases",
                    () -> last.knownWorkflowIds().equals(openCaseIds()),
                    () -> "open but unknown (lost): " + minus(openCaseIds(), last.knownWorkflowIds()));
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.MINUTES)
    void casesRunExactlyOnceUnderCrashesRestartsAndScaling() throws Exception {
        Assumptions.assumeFalse(ARM.equals("offon"));
        var rig = newRig();
        resources.add(rig);
        var client = new Client(rig);
        note("campaign arm=%s seed=%d operations=%d", ARM, SEED, OPERATIONS);
        indexEnabled = !ARM.equals("enable");
        startNode(rig);
        startNode(rig);

        for (int operation = 0; operation < OPERATIONS; operation++) {
            if (ARM.equals("enable") && operation == OPERATIONS / 2) {
                enableIndexOnTwoNodesAtOnce(rig);
            }
            int pick = random.nextInt(20);
            if (pick < 4) {
                open(client, 1 + random.nextInt(3));
            } else if (pick < 7) {
                approve(client, 1 + random.nextInt(2));
            } else if (pick < 10) {
                close(client, 1 + random.nextInt(2));
            } else if (pick < 12) {
                crashWithSignals(rig, client);
            } else if (pick < 13) {
                restart(rig);
            } else if (pick < 14) {
                outage(rig, client);
            } else if (pick < 15) {
                scale(rig);
            } else if (pick < 17) {
                moveSegment();
            } else if (pick < 19) {
                splitOrMerge();
            } else {
                pause(1_500);
            }
            // Time for a case to park and hibernate, so later operations find cases to signal.
            pause(300 + random.nextInt(900));
            observe();
        }
        note("evidence: %d crashes of a node with cases, %d signals while the owner was down, %d segment moves, "
                     + "%d splits and merges, %d failed index reads",
             crashesOfANodeWithCases, signalsWhileOwnerDown, segmentMoves, splitsAndMerges, injectedReadFaults.get());
        if (ARM.equals("readfaults") && injectedReadFaults.get() == 0) {
            violation("INCONCLUSIVE: no index read failed");
        }
        if (OPERATIONS >= 40 && (crashesOfANodeWithCases == 0 || signalsWhileOwnerDown == 0)) {
            violation("INCONCLUSIVE: no crash hit a node with cases, or no signal was sent while an owner was down");
        }

        // given: every node stopped, so the rows are at rest
        settle("before the final claim");
        stopAllNodes();
        if (finalMode() != IndexMode.NONE) {
            // Evidence that the index was in use: its rows for the cases that are still open.
            var rows = indexRows();
            note("rows before the final claim: %s, open cases: %d", rows, openCaseIds().size());
            if (ARM.equals("indexed") && rows.workflows() != openCaseIds().size()) {
                violation("The index holds " + rows.workflows() + " workflow rows for " + openCaseIds().size()
                                  + " open cases, after every node released its segments");
            }
        }

        // when: one node claims every segment
        var last = new Node(rig, "final", finalMode());
        nodes.add(last);

        // then: it knows exactly the cases that are still open
        awaitOrFail("the final node knows exactly the open cases",
                    () -> {
                        observe();
                        return last.knownWorkflowIds().equals(openCaseIds());
                    },
                    () -> "open but unknown (lost): " + minus(openCaseIds(), last.knownWorkflowIds())
                            + ", known but not open (stale): " + minus(last.knownWorkflowIds(), openCaseIds()));

        // and: every open case runs to its end, each effect exactly once
        settle("before the drain");
        awaitOrFail("every case ended",
                    () -> {
                        observe();
                        approve(client, Integer.MAX_VALUE);
                        close(client, Integer.MAX_VALUE);
                        return cases.values().stream().allMatch(c -> c.stage == Stage.DONE);
                    },
                    () -> "not ended: " + casesIn(c -> c.stage != Stage.DONE));

        // and: an index that every node kept loses the row of each case once the end of its workflow is handled
        boolean rowsChecked = finalMode() != IndexMode.NONE;
        if (rowsChecked) {
            // The last effect of a case runs before its workflow ends, so the rows empty a moment after the cases.
            awaitOrFail("the index holds no row once every workflow ended",
                        () -> noRows(indexRowsOrFail()),
                        () -> "rows while the node runs: " + indexRowsOrFail());
        }
        stopAllNodes();
        if (finalMode() != IndexMode.NONE) {
            var rows = indexRows();
            note("rows at rest: %s", rows);
            if (rowsChecked && !noRows(rows)) {
                violation("The index holds rows while no workflow runs: " + rows);
            }
        }
        note("campaign passed: %d cases, %d nodes started", cases.size(), nodeCount);
    }

    // ---------------------------------------------------------------------------------------------------------
    // Operations
    // ---------------------------------------------------------------------------------------------------------

    private void open(Client client, int count) {
        for (int i = 0; i < count; i++) {
            open(client, random.nextInt(5) == 0);
        }
    }

    private void open(Client client, boolean brief) {
        var opened = new Case("case-" + SEED + "-" + caseCount++, brief);
        cases.put(opened.id, opened);
        client.publish(new CaseOpened(opened.id, brief));
        note("open %s%s", opened.id, brief ? " (brief)" : "");
    }

    /**
     * Splits a segment while few exist, and merges one otherwise. The rows are keyed by workflow, so they stay as they
     * are, while the segment a case belongs to changes.
     */
    private void splitOrMerge() {
        settle("before a split or a merge");
        var holder = nodes.stream().filter(node -> !node.segments().isEmpty()).findFirst().orElseThrow();
        int segment = new TreeSet<>(holder.segments()).first();
        boolean split = segments <= SEGMENTS;
        boolean changed;
        try {
            changed = split ? holder.processor().splitSegment(segment).orTimeout(60, TimeUnit.SECONDS).join()
                    : holder.processor().mergeSegment(segment).orTimeout(60, TimeUnit.SECONDS).join();
        } catch (RuntimeException e) {
            // A merge needs both halves, and another node may hold the other half. Nothing changed then.
            note("%s of segment %d on %s was refused: %s", split ? "split" : "merge", segment, holder.id,
                 String.valueOf(e.getCause() == null ? e : e.getCause()).replace('%', ' '));
            return;
        }
        note("%s segment %d on %s: %s", split ? "split" : "merge", segment, holder.id, changed);
        if (changed) {
            segments += split ? 1 : -1;
            splitsAndMerges++;
        }
    }

    private void approve(Client client, int limit) {
        casesIn(Stage.AWAITING_APPROVAL, limit).forEach(approved -> {
            approved.stage = Stage.APPROVAL_SENT;
            client.publish(new CaseApproved(approved.id));
            note("approve %s", approved.id);
        });
    }

    private void close(Client client, int limit) {
        casesIn(Stage.AWAITING_CLOSE, limit).forEach(closed -> {
            closed.stage = Stage.CLOSE_SENT;
            client.publish(new CaseClosed(closed.id));
            note("close %s", closed.id);
        });
    }

    /**
     * Crashes one of at least 2 nodes and publishes signals for cases it held while nobody owns their segments.
     */
    private void crashWithSignals(Rig rig, Client client) {
        if (nodes.size() < 2) {
            return;
        }
        settle("before a crash");
        var crashed = removeANodeWithSegments();
        var held = crashed.knownWorkflowIds();
        note("crash %s: segments %s, %d cases", crashed.id, crashed.segments(), held.size());
        if (!held.isEmpty()) {
            crashesOfANodeWithCases++;
        }
        crashed.crash();
        signalSome(client, held, 2);
        pause(500);
        startNode(rig);
    }

    /**
     * Makes a node that holds segments release one of them for a while, so another node claims it.
     */
    private void moveSegment() {
        if (nodes.size() < 2) {
            return;
        }
        settle("before a segment move");
        var donor = nodes.stream().filter(node -> !node.segments().isEmpty()).findFirst().orElseThrow();
        int segment = new TreeSet<>(donor.segments()).first();
        note("move segment %d away from %s", segment, donor.id);
        donor.release(segment);
        awaitOrFail("another node claims segment " + segment,
                    () -> nodes.stream().anyMatch(node -> node != donor && node.segments().contains(segment)),
                    () -> "holders: " + nodes.stream().map(node -> node.id + node.segments()).toList());
        segmentMoves++;
    }

    /**
     * Removes and returns a node that holds segments, picked by the seed among those that do.
     */
    private Node removeANodeWithSegments() {
        var holders = nodes.stream().filter(node -> !node.segments().isEmpty()).toList();
        var picked = holders.get(random.nextInt(holders.size()));
        nodes.remove(picked);
        return picked;
    }

    private void restart(Rig rig) {
        settle("before a restart");
        var restarted = removeANodeWithSegments();
        note("restart %s: segments %s", restarted.id, restarted.segments());
        restarted.close();
        startNode(rig);
    }

    /**
     * Crashes every node, publishes signals and a new case while no node runs, and starts 2 nodes at once.
     */
    private void outage(Rig rig, Client client) {
        settle("before an outage");
        var held = new HashSet<String>();
        nodes.forEach(node -> held.addAll(node.knownWorkflowIds()));
        note("outage: %d nodes, %d cases", nodes.size(), held.size());
        if (!held.isEmpty()) {
            crashesOfANodeWithCases++;
        }
        nodes.forEach(Node::crash);
        nodes.clear();
        signalSome(client, held, 3);
        open(client, 1);
        startTwoNodesAtOnce(rig);
    }

    private void scale(Rig rig) {
        settle("before scaling");
        if (nodes.size() < 3) {
            startNode(rig);
        } else {
            var stopped = removeANodeWithSegments();
            note("scale down: stop %s, segments %s", stopped.id, stopped.segments());
            stopped.close();
        }
    }

    /**
     * Stops the nodes that ran without an index and starts 2 nodes with one at the same moment, so both complete the
     * rows of the running cases at once.
     */
    private void enableIndexOnTwoNodesAtOnce(Rig rig) {
        settle("before the index is enabled");
        note("enable the index: stop %d nodes without one", nodes.size());
        stopAllNodes();
        indexEnabled = true;
        startTwoNodesAtOnce(rig);
    }

    private void startTwoNodesAtOnce(Rig rig) {
        // The names and the modes are drawn first, in a fixed order, so the seed decides them.
        var firstId = "n" + nodeCount++;
        var firstMode = modeOfNextNode();
        var secondId = "n" + nodeCount++;
        var secondMode = modeOfNextNode();
        var first = CompletableFuture.supplyAsync(() -> new Node(rig, firstId, firstMode));
        var second = CompletableFuture.supplyAsync(() -> new Node(rig, secondId, secondMode));
        nodes.add(first.orTimeout(120, TimeUnit.SECONDS).join());
        nodes.add(second.orTimeout(120, TimeUnit.SECONDS).join());
        note("start %s with index %s and %s with index %s at once", firstId, firstMode, secondId, secondMode);
    }

    private void signalSome(Client client, Set<String> held, int limit) {
        int sent = 0;
        for (var id : new TreeSet<>(held)) {
            var signalled = cases.get(id);
            if (signalled == null || sent == limit) {
                continue;
            }
            if (signalled.stage == Stage.AWAITING_APPROVAL) {
                signalled.stage = Stage.APPROVAL_SENT;
                client.publish(new CaseApproved(id));
                note("approve %s while its owner is down", id);
                signalsWhileOwnerDown++;
                sent++;
            } else if (signalled.stage == Stage.AWAITING_CLOSE) {
                signalled.stage = Stage.CLOSE_SENT;
                client.publish(new CaseClosed(id));
                note("close %s while its owner is down", id);
                signalsWhileOwnerDown++;
                sent++;
            }
        }
    }

    private void startNode(Rig rig) {
        var mode = modeOfNextNode();
        var node = new Node(rig, "n" + nodeCount++, mode);
        nodes.add(node);
        note("start %s with index %s", node.id, mode);
    }

    private void stopAllNodes() {
        nodes.forEach(Node::close);
        nodes.clear();
    }

    private IndexMode modeOfNextNode() {
        return switch (ARM) {
            case "noindex" -> IndexMode.NONE;
            case "indexed" -> IndexMode.WITH_TOKEN;
            case "owntx" -> IndexMode.OWN_TRANSACTION;
            case "readfaults" -> IndexMode.FAILING_READS;
            case "enable" -> indexEnabled ? IndexMode.WITH_TOKEN : IndexMode.NONE;
            // The first node has an index, so the rows are complete before a node without one runs.
            case "toggled" -> !firstNodeStarted() || random.nextInt(3) != 0
                    ? IndexMode.WITH_TOKEN
                    : IndexMode.NONE;
            default -> throw new IllegalArgumentException("Unknown campaign arm: " + ARM);
        };
    }

    private boolean firstNodeStarted() {
        return toggledNodes++ > 0;
    }

    private static IndexMode finalMode() {
        return switch (ARM) {
            case "noindex" -> IndexMode.NONE;
            case "owntx" -> IndexMode.OWN_TRANSACTION;
            default -> IndexMode.WITH_TOKEN;
        };
    }

    // ---------------------------------------------------------------------------------------------------------
    // Oracle
    // ---------------------------------------------------------------------------------------------------------

    /**
     * Moves each case on by what the cluster shows, and fails on an effect that ran more than once.
     * <p>
     * A case counts as waiting for a signal only once a node holds it hibernated on the wait for that signal: a
     * workflow hibernates only while it waits, so that wait is open in the event store from then on, whatever happens
     * to the node. A case that sleeps also hibernates, and an event published before its wait opened is not delivered
     * to it.
     */
    private void observe() {
        var hibernated = new java.util.HashMap<String, Set<String>>();
        nodes.forEach(node -> hibernated.putAll(node.openWaitsOfHibernated()));
        for (var observed : cases.values()) {
            var id = observed.id;
            failOnSecondRun("approve", id, APPROVED);
            failOnSecondRun("archive", id, ARCHIVED);
            failOnSecondRun("expire", id, EXPIRED);
            switch (observed.stage) {
                case OPENED -> {
                    if (hibernated.getOrDefault(id, Set.of()).contains("awaitApproval")) {
                        observed.stage = Stage.AWAITING_APPROVAL;
                    }
                }
                case APPROVAL_SENT -> {
                    if (count(APPROVED, id) == 1) {
                        observed.stage = Stage.COOLING_DOWN;
                        observed.approvedAt = Instant.now();
                    }
                }
                case COOLING_DOWN -> {
                    if (hibernated.getOrDefault(id, Set.of()).contains("awaitClosed")) {
                        observed.stage = Stage.AWAITING_CLOSE;
                    } else if (observed.approvedAt != null
                            && Instant.now().isAfter(observed.approvedAt.plus(COOLDOWN).plus(EXPIRY_GRACE))
                            && !nodes.isEmpty()) {
                        violation("Case " + id + " did not wait for its close within " + COOLDOWN.plus(EXPIRY_GRACE)
                                          + " of its approval: its cooldown never ended. " + whereIs(id));
                    }
                }
                case CLOSE_SENT -> {
                    if (count(ARCHIVED, id) == 1) {
                        observed.stage = Stage.DONE;
                    }
                }
                case EXPIRING -> {
                    if (count(EXPIRED, id) == 1) {
                        observed.stage = Stage.DONE;
                    } else if (Instant.now().isAfter(observed.openedAt.plus(BRIEF_TIMEOUT).plus(EXPIRY_GRACE))
                            && !nodes.isEmpty()) {
                        violation("Case " + id + " did not expire within " + BRIEF_TIMEOUT.plus(EXPIRY_GRACE)
                                          + " of its opening: its timeout never ran");
                    }
                }
                default -> {
                    // Nothing to wait for.
                }
            }
        }
    }

    /**
     * Waits until every segment is claimed and no case waits for the engine anymore.
     */
    private void settle(String when) {
        awaitOrFail("the cluster settles " + when,
                    () -> {
                        observe();
                        return claimedSegments() == segments && casesIn(WorkflowIndexFaultCampaignTest::pending)
                                .isEmpty();
                    },
                    () -> "claimed segments: " + claimedSegments() + " of " + segments + ", stuck cases: "
                            + casesIn(WorkflowIndexFaultCampaignTest::pending));
    }

    private static boolean pending(Case candidate) {
        return candidate.stage == Stage.OPENED
                || candidate.stage == Stage.APPROVAL_SENT
                || candidate.stage == Stage.CLOSE_SENT;
    }

    private int claimedSegments() {
        var claimed = new HashSet<Integer>();
        nodes.forEach(node -> claimed.addAll(node.segments()));
        return claimed.size();
    }

    private Set<String> openCaseIds() {
        var open = new TreeSet<String>();
        cases.values().stream().filter(c -> c.stage != Stage.DONE).forEach(c -> open.add(c.id));
        return open;
    }

    private List<Case> casesIn(Stage stage, int limit) {
        return cases.values().stream().filter(c -> c.stage == stage).limit(limit).toList();
    }

    private List<String> casesIn(Predicate<Case> filter) {
        return cases.values().stream().filter(filter).map(c -> c.id + "=" + c.stage).toList();
    }

    private static Set<String> minus(Set<String> left, Set<String> right) {
        var difference = new TreeSet<>(left);
        difference.removeAll(right);
        return difference;
    }

    private void failOnSecondRun(String effect, String id, Map<String, AtomicInteger> counter) {
        if (count(counter, id) > 1) {
            violation("The " + effect + " effect of case " + id + " ran " + count(counter, id) + " times");
        }
    }

    private static int count(Map<String, AtomicInteger> counter, String id) {
        var counted = counter.get(id);
        return counted == null ? 0 : counted.get();
    }

    private void awaitOrFail(String what, java.util.function.BooleanSupplier done,
                             java.util.function.Supplier<String> detail) {
        long deadline = System.nanoTime() + SETTLE.toNanos();
        while (!done.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                violation("Not within " + SETTLE.toSeconds() + " s: " + what + ". " + detail.get());
            }
            pause(200);
        }
    }

    /**
     * Says which node holds the segment of the given case, and whether it holds the case in memory or hibernated.
     */
    private String whereIs(String id) {
        var where = new ArrayList<String>();
        for (var node : nodes) {
            var waits = node.openWaitsOfHibernated().get(id);
            boolean inMemory = node.engine.workflowExecutions().stream().anyMatch(e -> e.workflowId().equals(id));
            where.add(node.id + " segments " + new TreeSet<>(node.segments()) + (inMemory ? " holds it in memory" : "")
                              + (waits != null ? " holds it hibernated on " + waits : ""));
        }
        return String.join("; ", where);
    }

    private void violation(String message) {
        note("VIOLATION: %s", message);
        fail("arm=" + ARM + " seed=" + SEED + ": " + message + System.lineSeparator()
                     + String.join(System.lineSeparator(), trace));
    }

    private void note(String format, Object... arguments) {
        var line = String.format("%7.1fs  ", (System.nanoTime() - startedAt) / 1e9) + String.format(format, arguments);
        synchronized (trace) {
            trace.add(line);
        }
    }

    private static void pause(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private record IndexRows(int workflows, int waits, int wakeTimes) {

    }

    private static IndexRows indexRows() throws SQLException {
        try (var connection = DriverManager.getConnection(PG_URL, "perf", "perf");
             var statement = connection.createStatement()) {
            return new IndexRows(
                    countOf(statement, "select count(*) from workflowindexentry where workflowmodule = '" + MODULE
                            + "'"),
                    countOf(statement, "select count(*) from workflowindexwaitentry where workflowmodule = '"
                            + MODULE + "'"),
                    countOf(statement, "select count(*) from workflowindexentry where workflowmodule = '" + MODULE
                            + "' and wakesat is not null")
            );
        }
    }

    private static boolean noRows(IndexRows rows) {
        return rows.workflows() == 0 && rows.waits() == 0;
    }

    private static IndexRows indexRowsOrFail() {
        try {
            return indexRows();
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    private static int countOf(java.sql.Statement statement, String sql) throws SQLException {
        try (var rows = statement.executeQuery(sql)) {
            rows.next();
            return rows.getInt(1);
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // Workflow
    // ---------------------------------------------------------------------------------------------------------

    private static void run(SimpleWorkflowContext ctx) {
        var caseId = String.valueOf(ctx.workflowPayload().get("caseId"));
        boolean brief = Boolean.parseBoolean(String.valueOf(ctx.workflowPayload().get("brief")));
        var byCase = associate(payloadProperty("caseId"), equalsTo(caseId));
        var approval = ctx.waitForEvent("awaitApproval", CaseApproved.class, byCase,
                                        step -> step.timeout(brief ? BRIEF_TIMEOUT : Duration.ofDays(3)));
        approval.await();
        if (!approval.success()) {
            ctx.awaitExecute("expire", Map.of("caseId", caseId), (pc, in) -> {
                EXPIRED.computeIfAbsent(caseId, id -> new AtomicInteger()).incrementAndGet();
                return Map.of("expired", true);
            });
            return;
        }
        ctx.awaitExecute("approve", Map.of("caseId", caseId), (pc, in) -> {
            APPROVED.computeIfAbsent(caseId, id -> new AtomicInteger()).incrementAndGet();
            return Map.of("approved", true);
        });
        // In memory and not hibernated while it sleeps: a segment claim must restore it, never adopt it.
        ctx.awaitSleep("cooldown", COOLDOWN);
        ctx.awaitEvent("awaitClosed", CaseClosed.class, byCase, step -> step.timeout(Duration.ofDays(3)));
        ctx.awaitExecute("archive", Map.of("caseId", caseId), (pc, in) -> {
            ARCHIVED.computeIfAbsent(caseId, id -> new AtomicInteger()).incrementAndGet();
            return Map.of("archived", true);
        });
    }

    private static WorkflowModule<SimpleWorkflowContext> module(Jpa jpa) {
        return WorkflowModule.configure(MODULE, SimpleWorkflowContext.class)
                             .definition(d -> d.declarative(c -> WorkflowIndexFaultCampaignTest::run)
                                               .workflowName("Case")
                                               .on(EventConditions.fromType(CaseOpened.class))
                                               .customized((c, w) -> w.workflowIdProvider(
                                                       new PayloadPropertyWorkflowIdProvider("caseId"))))
                             .processorConfiguration(pc -> {
                                 pc.initialSegmentCount(SEGMENTS)
                                   .workerExecutor(() -> Executors.newScheduledThreadPool(SEGMENTS))
                                   // A node that lost its claims must notice within the claim timeout below.
                                   .tokenClaimInterval(500)
                                   .claimExtensionThreshold(1_000);
                                 // The JPA transaction of the token store and the index, around the unit of work
                                 // the processor already has.
                                 return pc.unitOfWorkFactory(new TransactionalUnitOfWorkFactory(
                                         jpa.transactionManager, pc.unitOfWorkFactory()
                                 ));
                             })
                             .withoutHistory()
                             .contextFactory(c -> new SimpleWorkflowContextFactory())
                             // A pass every second, so a waiting case hibernates soon after it parks.
                             .withHibernation()
                             .hibernationInterval(1_000);
    }

    // ---------------------------------------------------------------------------------------------------------
    // Nodes
    // ---------------------------------------------------------------------------------------------------------

    private static WorkflowConfigurer configurerOn(Rig rig) {
        var configurer = WorkflowConfigurer.create();
        var engineOfNode = new ConcurrentHashMap<String, EventStorageEngine>();
        // Not looked up as a component: the snapshot store decorates the storage engine, so that would be a cycle.
        Function<Configuration, EventStorageEngine> storeOfNode =
                cfg -> engineOfNode.computeIfAbsent("engine", key -> rig.engine(cfg));
        configurer.componentRegistry(cr -> cr
                .disableEnhancer(
                        "io.axoniq.framework.axonserver.connector.configuration.AxonServerConfigurationEnhancer")
                .disableEnhancer("io.axoniq.framework.postgresql.PostgresqlConfigurationEnhancer")
                .registerComponent(EventStorageEngine.class, storeOfNode::apply)
                .registerComponent(SnapshotStore.class, cfg -> rig.snapshots(cfg, storeOfNode.apply(cfg))));
        var transactions = rig.transactionManager();
        if (transactions != null) {
            configurer.componentRegistry(cr -> cr.registerComponent(TransactionManager.class, cfg -> transactions));
        }
        return configurer;
    }

    /**
     * The JPA index of a node, whose reads of the rows of a segment fail at the seeded rate. A segment claim then falls
     * back to sourcing, as it does when the database fails the read.
     */
    private final class FailingReadsIndex implements WorkflowIndex {

        private final WorkflowIndex delegate;

        private FailingReadsIndex(WorkflowIndex delegate) {
            this.delegate = delegate;
        }

        @Override
        public CompletableFuture<WorkflowIndexRows> load(String module,
                                                         Segment segment,
                                                         @Nullable ProcessingContext context) {
            boolean fail;
            synchronized (readFaults) {
                fail = readFaults.nextDouble() < READ_FAULT_RATE;
            }
            if (fail) {
                injectedReadFaults.incrementAndGet();
                return CompletableFuture.failedFuture(
                        new IllegalStateException("Injected failure of the read of segment " + segment.getSegmentId())
                );
            }
            return delegate.load(module, segment, context);
        }

        @Override
        public CompletableFuture<Void> store(String module,
                                             WorkflowIndexChanges changes,
                                             @Nullable ProcessingContext context) {
            return delegate.store(module, changes, context);
        }

        @Override
        public boolean storesWithToken() {
            return delegate.storesWithToken();
        }
    }

    /**
     * Publishes the events of the campaign. It runs no workflow, so it stays up while every node is down.
     */
    private final class Client implements AutoCloseable {

        private final AxonConfiguration configuration;

        private Client(Rig rig) {
            this.configuration = configurerOn(rig).start();
            resources.add(this);
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

        @Override
        public void close() {
            configuration.shutdown();
        }
    }

    private final class Node implements AutoCloseable {

        private final String id;
        private final Jpa jpa;
        private final AxonConfiguration configuration;
        private final WorkflowEngine engine;
        private final AtomicBoolean closed = new AtomicBoolean();

        private Node(Rig rig, String id, IndexMode mode) {
            this.id = id;
            // Connections of its own, so a crash of this node takes only its connections away.
            this.jpa = new Jpa(schemaCreated.compareAndSet(false, true));
            var configurer = configurerOn(rig);
            if (mode != IndexMode.NONE) {
                configurer.registerWorkflowIndex(cfg -> {
                    WorkflowIndex index = new JpaWorkflowIndex(
                            jpa.transactions,
                            cfg.getComponent(GeneralConverter.class),
                            JpaWorkflowIndexConfiguration.DEFAULT.storesWithToken(mode != IndexMode.OWN_TRANSACTION)
                    );
                    return mode == IndexMode.FAILING_READS ? new FailingReadsIndex(index) : index;
                });
            }
            configurer.componentRegistry(cr -> cr
                    .registerComponent(TokenStore.class, cfg -> new JpaTokenStore(
                            jpa.transactions,
                            cfg.getComponent(GeneralConverter.class),
                            // Short, so the segments of a crashed node are free again within seconds.
                            JpaTokenStoreConfiguration.DEFAULT.nodeId(id).claimTimeout(Duration.ofSeconds(4))))
                    .registerModule(module(jpa)));
            this.configuration = configurer.start();
            resources.add(this);
            this.engine = find(configuration, WorkflowEngine.class, engine -> true)
                    .orElseThrow(() -> new IllegalStateException("No WorkflowEngine found"));
        }

        private static <T> Optional<T> find(Configuration configuration, Class<T> type, Predicate<T> filter) {
            return configuration.getComponents(type).values().stream().filter(filter).findFirst()
                                .or(() -> configuration.getModuleConfigurations().stream()
                                                       .map(module -> find(module, type, filter))
                                                       .flatMap(Optional::stream)
                                                       .findFirst());
        }

        /** The event processor of the workflow engine. */
        PooledStreamingEventProcessor processor() {
            return find(configuration, EventProcessor.class,
                        processor -> processor instanceof PooledStreamingEventProcessor pooled
                                && MODULE.equals(pooled.name()))
                    .map(PooledStreamingEventProcessor.class::cast)
                    .orElseThrow(() -> new IllegalStateException("No workflow processor found"));
        }

        /** The segments this node holds. */
        Set<Integer> segments() {
            return processor().processingStatus().keySet();
        }

        /** Releases the given segment and stays away from it long enough for another node to claim it. */
        void release(int segment) {
            processor().releaseSegment(segment, 3, TimeUnit.SECONDS).orTimeout(60, TimeUnit.SECONDS).join();
        }

        Set<String> hibernatedIds() {
            return HibernationProbe.hibernatedIds(engine);
        }

        Map<String, Set<String>> openWaitsOfHibernated() {
            return HibernationProbe.openWaitsOfHibernated(engine);
        }

        /** The workflows this node knows: those in memory and those it holds hibernated. */
        Set<String> knownWorkflowIds() {
            var known = new TreeSet<>(hibernatedIds());
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
                    logger.warn("The crashed node {} stopped with {}", id, e.toString());
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
}
