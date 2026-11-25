/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.configuration;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.axoniq.framework.workflow.configuration.AbstractEventSourcedEntityRepositoryTestBase.TestContext;
import io.axoniq.framework.workflow.configuration.JpaWorkflowIndexConnectionPoolTest.EntityManagerPerTransaction;
import io.axoniq.framework.workflow.dsl.api.EventConditions;
import io.axoniq.framework.workflow.dsl.api.PayloadMapping;
import io.axoniq.framework.workflow.dsl.api.PrimitiveMetadata;
import io.axoniq.framework.workflow.dsl.api.StepFailedException;
import io.axoniq.framework.workflow.dsl.api.Timing;
import io.axoniq.framework.workflow.dsl.api.WaitForStepDefinition;
import io.axoniq.framework.workflow.dsl.api.WorkflowState;
import io.axoniq.framework.workflow.index.api.WorkflowIndex;
import io.axoniq.framework.workflow.index.api.WorkflowIndex.Entry;
import io.axoniq.framework.workflow.index.api.WorkflowIndex.HibernationHint;
import io.axoniq.framework.workflow.index.api.WorkflowIndex.IndexDelta;
import io.axoniq.framework.workflow.index.api.WorkflowIndex.OpenWait;
import io.axoniq.framework.workflow.index.jpa.JpaWorkflowIndex;
import io.axoniq.framework.workflow.index.jpa.WorkflowIndexEntry;
import io.axoniq.framework.workflow.index.jpa.WorkflowIndexInitialization;
import io.axoniq.framework.workflow.runtime.association.Associations;
import io.axoniq.framework.workflow.runtime.execution.DefaultEventNameCustomizer;
import io.axoniq.framework.workflow.runtime.execution.PayloadPropertyWorkflowIdProvider;
import io.axoniq.framework.workflow.runtime.execution.RunningWorkflows;
import io.axoniq.framework.workflow.runtime.execution.WorkflowEngine;
import io.axoniq.framework.workflow.runtime.execution.WorkflowEngineHibernation;
import io.axoniq.framework.workflow.runtime.execution.WorkflowStore;
import io.axoniq.framework.workflow.runtime.execution.payload.GlobalOnlyPayloadReducer;
import io.axoniq.framework.workflow.runtime.execution.payload.LocalOnlyPayloadReducer;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import jakarta.persistence.EntityManagerFactory;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.configuration.DecoratorDefinition;
import org.axonframework.conversion.GeneralConverter;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.TransactionalUnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.transaction.jpa.JpaTransactionalExecutorProvider;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jpa.JpaTokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jpa.JpaTokenStoreConfiguration;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jpa.TokenEntry;
import org.hibernate.jpa.HibernatePersistenceConfiguration;
import org.hibernate.tool.schema.Action;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.axoniq.framework.workflow.runtime.association.PayloadPropertyValueRetriever.payloadProperty;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Restarts a workflow module with hibernation enabled, whose event processor handles every batch in a JPA transaction,
 * with a {@link JpaWorkflowIndex} and a JPA token store in the same database, so the index writes the hibernation hints
 * of its workflows within the transaction the processor stores its token in.
 * <p>
 * The oracles are which workflows a claim loads from the event store, whether they wake and end on their events and
 * deadlines, and which hints the index holds.
 *
 * @author Allard Buijze
 */
class JpaWorkflowIndexHibernationRestartTest {

    private static final String MODULE = "jpa-index-hibernation";
    private static final int SEGMENTS = 2;
    private static final Duration AWAIT = Duration.ofSeconds(30);
    private static final Duration IDLE_DELAY = Duration.ofMillis(200);
    // Longer than any test, so no hibernation pass asks a workflow to hibernate: the test hibernates it itself.
    private static final Duration IDLE_DELAY_BEYOND_THE_TEST = Duration.ofHours(1);
    // Marks the reads of the test itself, which the counting store does not count.
    private static final Context.ResourceKey<Boolean> TEST_READ = Context.ResourceKey.withLabel("test-read");

    private final HikariDataSource dataSource = dataSource();
    private final EntityManagerFactory entityManagerFactory =
            new HibernatePersistenceConfiguration("workflow-index-hibernation")
                    .managedClasses(List.of(TokenEntry.class,
                                            WorkflowIndexEntry.class,
                                            WorkflowIndexInitialization.class))
                    .property("hibernate.connection.datasource", dataSource)
                    .schemaToolingAction(Action.CREATE_DROP)
                    .createEntityManagerFactory();
    private final EventStorageEngine storageEngine = new InMemoryEventStorageEngine();
    private final JpaWorkflowIndex reader = JpaWorkflowIndex.builder()
                                                            .entityManagerFactory(entityManagerFactory)
                                                            .build();
    private final AtomicInteger fullLoads = new AtomicInteger();
    private final Map<String, AtomicInteger> loads = new ConcurrentHashMap<>();
    private final AtomicInteger loadsBeforeCatchUp = new AtomicInteger();
    private final AtomicInteger hintsWrittenWithinTheProcessorTransaction = new AtomicInteger();
    private final AtomicInteger transactionsWithHintsEnded = new AtomicInteger();
    private final AtomicInteger hintsWrittenOtherwise = new AtomicInteger();
    private final Map<Integer, AtomicInteger> releasedClaims = new ConcurrentHashMap<>();
    private final AtomicBoolean failWrites = new AtomicBoolean();
    private final AtomicBoolean refuseToJoin = new AtomicBoolean();
    private final Set<String> completed = ConcurrentHashMap.newKeySet();
    private final Set<String> timedOut = ConcurrentHashMap.newKeySet();
    private final Map<String, List<Boolean>> bodyStartsCaughtUp = new ConcurrentHashMap<>();
    private final Map<AxonConfiguration, DisconnectableStorageEngine> configurations = new ConcurrentHashMap<>();
    private volatile @Nullable AxonConfiguration current;

    private static HikariDataSource dataSource() {
        var config = new HikariConfig();
        config.setJdbcUrl("jdbc:hsqldb:mem:" + UUID.randomUUID());
        config.setUsername("sa");
        config.setPassword("");
        config.setMaximumPoolSize(10);
        return new HikariDataSource(config);
    }

    @AfterEach
    void tearDown() {
        List.copyOf(configurations.keySet()).forEach(this::stop);
        entityManagerFactory.close();
        dataSource.close();
    }

    @Test
    void leavesTheHibernatedWorkflowsOutOfMemoryOnRestartAndWakesThemByTheirEvents() {
        // given - waiting workflows that hibernated, with their hints in the index
        var first = start(true, true);
        var waiting = startWorkflows(first, "pay", 20);
        awaitHibernated(first, waiting);
        awaitHinted(first, waiting);
        stop(first);
        resetCounters();

        // when
        var restarted = start(true, true);

        // then - every claim leaves them hibernated, without loading any of them
        awaitHibernated(restarted, waiting);
        assertThat(loads).isEmpty();
        assertThat(fullLoads).hasValue(0);
        assertThat(hintsWrittenOtherwise).hasValue(0);

        // when
        waiting.forEach(orderId -> publish(restarted, "paid", orderId));

        // then - each wakes for its event, loading it once, and ends
        await().atMost(AWAIT).until(() -> completed.containsAll(waiting));
        assertThat(loads.keySet()).containsExactlyInAnyOrderElementsOf(waiting);
        assertThat(loads.values()).allSatisfy(count -> assertThat(count).hasValue(1));
        await().atMost(AWAIT).until(() -> {
            publish(restarted, "tick", "none");
            return entries().isEmpty();
        });
    }

    @Test
    void wakesTheAdoptedWorkflowsAtTheirDeadlinesOnlyOnceTheirClaimsCaughtUp() {
        // given - workflows whose deadlines pass while no node runs
        var first = start(true, true);
        var waiting = startWorkflows(first, "deadline", 6);
        awaitHibernated(first, waiting);
        awaitHinted(first, waiting);
        stop(first);
        resetCounters();
        // and - a backlog to catch up with
        appendDirectly("tick", "none");
        // and - their deadlines passed
        await().atMost(AWAIT).until(() -> entries().stream().allMatch(
                entry -> entry.hint() != null && !entry.hint().wakeAt().isAfter(Instant.now())
        ));

        // when
        start(true, true);

        // then - each wakes for its deadline, times out and ends, loaded only once its claim caught up
        await().atMost(AWAIT).until(() -> timedOut.containsAll(waiting));
        assertThat(loads.keySet()).containsExactlyInAnyOrderElementsOf(waiting);
        assertThat(loadsBeforeCatchUp).hasValue(0);
        assertThat(fullLoads).hasValue(0);
    }

    @Test
    void wakesAnAdoptedWorkflowByAnEventOfTheBacklogAndStartsItsBodyOnlyOnceItsClaimCaughtUp() {
        // given
        var first = start(true, true);
        var waiting = startWorkflows(first, "pay", 1);
        var orderId = waiting.iterator().next();
        awaitHibernated(first, waiting);
        awaitHinted(first, waiting);
        stop(first);
        resetCounters();
        bodyStartsCaughtUp.clear();
        // and - the awaited event arrives while no node runs
        appendDirectly("paid", orderId);

        // when
        start(true, true);

        // then
        await().atMost(AWAIT).until(() -> completed.contains(orderId));
        assertThat(bodyStartsCaughtUp.get(orderId)).isNotEmpty().containsOnly(true);
        assertThat(fullLoads).hasValue(0);
    }

    @Test
    void neitherAReleaseNorAWriteOutsideTheProcessorTransactionWritesAHint() {
        // given - writes within a checkpoint that cannot join the processor's transaction
        refuseToJoin.set(true);
        var first = start(true, true);
        var waiting = startWorkflows(first, "pay", 3);
        awaitHibernated(first, waiting);
        tick(first, 5);

        // when - the release writes the pending changes
        stop(first);

        // then - the workflows are running, without hints
        assertThat(entries()).containsExactlyInAnyOrderElementsOf(
                waiting.stream().map(Entry::running).collect(Collectors.toSet())
        );
        assertThat(hintsWrittenWithinTheProcessorTransaction).hasValue(0);
        assertThat(hintsWrittenOtherwise).hasValue(0);

        // when - a node whose writes join the transaction again restarts
        refuseToJoin.set(false);
        resetCounters();
        var restarted = start(true, true);

        // then - its claims restore them, and they end on their events
        await().atMost(AWAIT).until(() -> loads.keySet().containsAll(waiting));
        waiting.forEach(orderId -> publish(restarted, "paid", orderId));
        await().atMost(AWAIT).until(() -> completed.containsAll(waiting));
    }

    @Test
    void restoresAWorkflowThatMovedOnAfterItsHintWasWrittenAndBeforeItsTokenWasStored() {
        // given - a workflow waiting for its payment and then its shipment, hibernated with its hint written
        var first = start(true, true);
        var waiting = startWorkflows(first, "payThenShip", 1);
        var orderId = waiting.iterator().next();
        awaitHibernated(first, waiting);
        awaitHinted(first, waiting);
        var hint = entries().iterator().next().hint();
        // and - from now on, the index cannot be written, so no token passes what happens next
        failWrites.set(true);

        // when - the payment wakes it, it moves on to wait for the shipment, and the node goes down
        publish(first, "paid", orderId);
        await().atMost(AWAIT).ignoreExceptions().until(
                () -> durableState(first, orderId).getStep("awaitShipped") != null
        );
        stop(first);
        failWrites.set(false);
        assertThat(entries()).containsExactly(new Entry(orderId, hint));
        resetCounters();
        var restarted = start(true, true);

        // then - the event of its own after the stored token shows the hint is stale: it is restored and cleared
        await().atMost(AWAIT).until(() -> loads.containsKey(orderId));
        publish(restarted, "shipped", orderId);
        await().atMost(AWAIT).until(() -> completed.contains(orderId));
    }

    @Test
    void neverWritesAHintWithACheckpointOfANodeThatLostTheClaimOfItsSegment() {
        // given - a running workflow in the index, without a hint
        // A workflow that hibernated before the index listed it as running would get a hint right away, so this node
        // hibernates it only when the test asks it to.
        var first = start(true, true, IDLE_DELAY_BEYOND_THE_TEST);
        var waiting = startWorkflows(first, "pay", 1);
        var orderId = waiting.iterator().next();
        await().atMost(AWAIT).until(() -> {
            tick(first, 1);
            return entries().equals(Set.of(Entry.running(orderId)));
        });
        // and - no checkpoint of those ticks is still in flight, which would notice the stolen claims below
        awaitCheckpointsAtTheHead(first);
        // and - another node took over every segment, without this node noticing yet
        var releasesBefore = releasesOf(segmentOf(orderId));
        stealClaims();

        // when - the workflow hibernates, and the node tries to checkpoint its hint
        hibernateNow(first, waiting);
        assertThat(releasesOf(segmentOf(orderId)))
                .as("the node must not have noticed the stolen claim before it checkpoints the hint")
                .isEqualTo(releasesBefore);
        await().atMost(AWAIT).until(() -> {
            tick(first, 1);
            return hintsWrittenWithinTheProcessorTransaction.get() > 0;
        });

        // then - the token store refused the token of the lost claim, which rolled the hint back with it, and the
        // release of the segment that followed wrote no hint either
        awaitTransactionsWithHintsEnded();
        awaitReleased(segmentOf(orderId), releasesBefore);
        assertThat(entries()).containsExactly(Entry.running(orderId));
        assertThat(hintsWrittenOtherwise).hasValue(0);
    }

    @Test
    void neverWritesAHintWithACheckpointAtTheStoredTokenOfANodeThatLostTheClaimOfItsSegment() {
        // given - a running workflow in the index, without a hint
        // A workflow that hibernated before the index listed it as running would get a hint right away, so this node
        // hibernates it only when the test asks it to.
        var first = start(true, true, IDLE_DELAY_BEYOND_THE_TEST);
        var waiting = startWorkflows(first, "pay", 1);
        var orderId = waiting.iterator().next();
        await().atMost(AWAIT).until(() -> {
            tick(first, 1);
            return entries().equals(Set.of(Entry.running(orderId)));
        });
        // and - no checkpoint of those ticks is still in flight, which would notice the stolen claims below
        awaitCheckpointsAtTheHead(first);
        // and - another node took over every segment, without this node noticing yet
        var releasesBefore = releasesOf(segmentOf(orderId));
        stealClaims();

        // when - the workflow hibernates, and the node checkpoints its hint at the token it already stored: no token
        // is stored
        hibernateNow(first, waiting);
        assertThat(releasesOf(segmentOf(orderId)))
                .as("the node must not have noticed the stolen claim before it checkpoints the hint")
                .isEqualTo(releasesBefore);
        await().atMost(AWAIT).until(() -> {
            WorkflowEngineHibernation.requestCheckpoint(engine(first), segmentOf(orderId), TrackingToken.LATEST);
            return hintsWrittenWithinTheProcessorTransaction.get() > 0;
        });

        // then - the claim check of that checkpoint failed, which rolled the hint back with it, and the release of the
        // segment that followed wrote no hint either
        awaitTransactionsWithHintsEnded();
        awaitReleased(segmentOf(orderId), releasesBefore);
        assertThat(entries()).containsExactly(Entry.running(orderId));
        assertThat(hintsWrittenOtherwise).hasValue(0);
    }

    @Test
    void clearsTheHintsThatANodeWithoutHibernationDoesNotAdopt() {
        // given
        var first = start(true, true);
        var waiting = startWorkflows(first, "pay", 3);
        awaitHibernated(first, waiting);
        awaitHinted(first, waiting);
        stop(first);
        resetCounters();

        // when - a node without hibernation claims the segments
        var withoutHibernation = start(false, true);

        // then - it restores the workflows, and clears their hints
        await().atMost(AWAIT).until(() -> loads.keySet().containsAll(waiting));
        await().atMost(AWAIT).until(() -> {
            tick(withoutHibernation, 1);
            return entries().equals(waiting.stream().map(Entry::running).collect(Collectors.toSet()));
        });
        stop(withoutHibernation);

        // when - a node with hibernation claims the segments again
        resetCounters();
        var restarted = start(true, true);

        // then - it restores them as well, as no hint is left
        await().atMost(AWAIT).until(() -> loads.keySet().containsAll(waiting));
        waiting.forEach(orderId -> publish(restarted, "paid", orderId));
        await().atMost(AWAIT).until(() -> completed.containsAll(waiting));
    }

    @Test
    void removesTheEntryOfAnAdoptedWorkflowThatTurnsOutToHaveEndedWhenItWakes() {
        // given - a workflow that ended
        var first = start(true, true);
        var ended = startWorkflows(first, "pay", 1);
        var orderId = ended.iterator().next();
        publish(first, "paid", orderId);
        await().atMost(AWAIT).until(() -> {
            tick(first, 1);
            return completed.contains(orderId) && entries().isEmpty();
        });
        stop(first);
        // and - a hint of it that a late write brought back, whose deadline passed
        var running = new HashMap<String, @Nullable HibernationHint>();
        running.put(orderId, new HibernationHint(Instant.now().minusSeconds(60), List.of(
                new OpenWait("awaitPaid", "paid", Set.of("payload:orderId=" + orderId))
        )));
        reader.store(MODULE, new IndexDelta(running, Set.of())).orTimeout(5, TimeUnit.SECONDS).join();
        resetCounters();
        completed.clear();

        // when
        var restarted = start(true, true);

        // then - it wakes for its deadline, finds it ended, and removes its entry
        await().atMost(AWAIT).until(() -> {
            tick(restarted, 1);
            return entries().isEmpty();
        });
        assertThat(loads.keySet()).containsExactly(orderId);
        assertThat(engine(restarted).hibernatedWorkflowIds()).isEmpty();
        assertThat(completed).isEmpty();
    }

    @Test
    void storesNoHintWithoutAProcessorTransactionSoARestartRestoresEveryWorkflow() {
        // given - a processor whose batches run without a JPA transaction to join
        var first = start(true, false);
        var waiting = startWorkflows(first, "pay", 3);
        awaitHibernated(first, waiting);
        tick(first, 5);
        stop(first);
        assertThat(entries()).allSatisfy(entry -> assertThat(entry.hint()).isNull());
        resetCounters();

        // when
        var restarted = start(true, false);

        // then
        await().atMost(AWAIT).until(() -> loads.keySet().containsAll(waiting));
        waiting.forEach(orderId -> publish(restarted, "paid", orderId));
        await().atMost(AWAIT).until(() -> completed.containsAll(waiting));
        assertThat(hintsWrittenWithinTheProcessorTransaction).hasValue(0);
        assertThat(hintsWrittenOtherwise).hasValue(0);
    }

    /**
     * Waits for the payment of its order, the payment and then the shipment, or the payment with a deadline of 3
     * seconds, which it handles by ending.
     */
    private void body(TestContext ctx) {
        recordBodyStart(ctx.workflowId());
        var orderId = String.valueOf(ctx.workflowPayload().get("orderId"));
        switch (String.valueOf(ctx.workflowPayload().get("kind"))) {
            case "deadline" -> {
                try {
                    ctx.awaitEvent(waitFor("awaitPaid", "paid", orderId, Duration.ofSeconds(3)));
                } catch (StepFailedException e) {
                    timedOut.add(ctx.workflowId());
                }
            }
            case "payThenShip" -> {
                ctx.awaitEvent(waitFor("awaitPaid", "paid", orderId, Duration.ofMinutes(5)));
                ctx.awaitEvent(waitFor("awaitShipped", "shipped", orderId, Duration.ofMinutes(5)));
            }
            default -> ctx.awaitEvent(waitFor("awaitPaid", "paid", orderId, Duration.ofMinutes(5)));
        }
        completed.add(ctx.workflowId());
    }

    private void recordBodyStart(String workflowId) {
        var configuration = current;
        if (configuration != null) {
            bodyStartsCaughtUp.computeIfAbsent(workflowId, id -> new CopyOnWriteArrayList<>())
                              .add(WorkflowEngineHibernation.isCaughtUp(engine(configuration), segmentOf(workflowId)));
        }
    }

    private static WaitForStepDefinition waitFor(String stepName, String eventName, String orderId, Duration timeout) {
        return new WaitForStepDefinition(
                new PrimitiveMetadata(stepName, DefaultEventNameCustomizer.Builder.defaults()),
                EventConditions.fromQualifiedName(new QualifiedName(eventName),
                                                  Associations.associate(payloadProperty("orderId"), "=", orderId)),
                new PayloadMapping(LocalOnlyPayloadReducer.INSTANCE, GlobalOnlyPayloadReducer.INSTANCE),
                new Timing(timeout)
        );
    }

    /**
     * Starts a node.
     *
     * @param hibernation whether the node hibernates idle workflows
     * @param joined      whether the event processor handles its batches in a JPA transaction the index can join;
     *                    otherwise the token store runs every operation in a transaction of its own
     */
    private AxonConfiguration start(boolean hibernation, boolean joined) {
        return start(hibernation, joined, IDLE_DELAY);
    }

    private AxonConfiguration start(boolean hibernation, boolean joined, Duration idleDelay) {
        var module = WorkflowModule.configure(MODULE, TestContext.class)
                                   .definition(d -> d
                                           .declarative(c -> this::body)
                                           .workflowName(MODULE)
                                           .on(c -> EventConditions.fromQualifiedName(new QualifiedName("start")))
                                           .customized((c, wc) -> wc.workflowIdProvider(
                                                   new PayloadPropertyWorkflowIdProvider("orderId")
                                           ))
                                   )
                                   .processorConfiguration(pc -> {
                                       var configured = pc.initialSegmentCount(SEGMENTS).tokenClaimInterval(500);
                                       return joined
                                               ? configured.unitOfWorkFactory(new TransactionalUnitOfWorkFactory(
                                                       new EntityManagerPerTransaction(entityManagerFactory),
                                                       pc.unitOfWorkFactory()
                                               ))
                                               : configured;
                                   })
                                   .checkpointInterval(0L)
                                   .contextFactory(c -> TestContext::new);
        var transactions = new JpaTransactionalExecutorProvider(entityManagerFactory);
        // Its own connection to the shared event store, so a stopped node no longer hears of appended events.
        var nodeStorageEngine = new DisconnectableStorageEngine(storageEngine);
        var configuration = WorkflowConfigurer.create()
                .hibernation(h -> h.enabled(hibernation).idleDelay(idleDelay).wakeTimeout(Duration.ofSeconds(10)))
                .registerWorkflowIndex(cfg -> new ObservedWorkflowIndex(JpaWorkflowIndex.builder()
                                                                                         .entityManagerFactory(
                                                                                                 entityManagerFactory
                                                                                         )
                                                                                         .build()))
                .componentRegistry(cr -> cr
                        .registerComponent(EventStorageEngine.class, cfg -> nodeStorageEngine)
                        .registerComponent(TokenStore.class, cfg -> new ReleaseCountingTokenStore(new JpaTokenStore(
                                joined ? transactions : context -> transactions.findTransactionalExecutor(null),
                                cfg.getComponent(GeneralConverter.class),
                                JpaTokenStoreConfiguration.DEFAULT
                        )))
                        .registerDecorator(DecoratorDefinition.forType(WorkflowStore.class)
                                                              .with((cfg, name, delegate) -> new CountingWorkflowStore(
                                                                      delegate
                                                              )))
                        .registerModule(module)
                )
                .build();
        configurations.put(configuration, nodeStorageEngine);
        current = configuration;
        configuration.start();
        return configuration;
    }

    private void stop(AxonConfiguration configuration) {
        configuration.shutdown();
        configurations.remove(configuration).disconnect();
        if (current == configuration) {
            current = null;
        }
    }

    private void resetCounters() {
        loads.clear();
        fullLoads.set(0);
        loadsBeforeCatchUp.set(0);
        hintsWrittenWithinTheProcessorTransaction.set(0);
        transactionsWithHintsEnded.set(0);
        hintsWrittenOtherwise.set(0);
    }

    /**
     * Waits until every processor transaction that wrote a hint so far ended, by a commit or a rollback, so the index
     * holds what those transactions leave behind.
     */
    private void awaitTransactionsWithHintsEnded() {
        await().atMost(AWAIT).until(
                () -> transactionsWithHintsEnded.get() == hintsWrittenWithinTheProcessorTransaction.get()
        );
    }

    private int releasesOf(Segment segment) {
        var released = releasedClaims.get(segment.getSegmentId());
        return released == null ? 0 : released.get();
    }

    /**
     * Waits until the claim of the given segment was released more often than the given number of times. The claim is
     * released last: after the segment wrote, through the index, what its checkpoints did not, and after the engine
     * dropped its workflows.
     */
    private void awaitReleased(Segment segment, int releasesBefore) {
        await().atMost(AWAIT).until(() -> releasesOf(segment) > releasesBefore);
    }

    private Set<String> startWorkflows(AxonConfiguration configuration, String kind, int count) {
        var orderIds = new LinkedHashSet<String>();
        IntStream.range(0, count).forEach(i -> {
            var orderId = kind + "-" + i;
            orderIds.add(orderId);
            var event = new GenericEventMessage(new MessageType("start"), Map.of("orderId", orderId, "kind", kind));
            publish(configuration, event);
        });
        await().atMost(AWAIT).ignoreExceptions().until(
                () -> orderIds.stream().allMatch(orderId -> durableState(configuration, orderId).getStep("awaitPaid")
                        != null)
        );
        return orderIds;
    }

    private static void publish(AxonConfiguration configuration, String eventName, String orderId) {
        publish(configuration, new GenericEventMessage(new MessageType(eventName), Map.of("orderId", orderId)));
    }

    private static void publish(AxonConfiguration configuration, GenericEventMessage event) {
        configuration.getComponent(UnitOfWorkFactory.class)
                     .create("publish-" + event.type().name())
                     .executeWithResult(ctx -> configuration.getComponent(EventStore.class)
                                                            .publish(ctx, event)
                                                            .thenApply(ignored -> event))
                     .orTimeout(5, TimeUnit.SECONDS)
                     .join();
    }

    /**
     * Publishes events that wake no workflow, so every segment takes a checkpoint.
     */
    private static void tick(AxonConfiguration configuration, int count) {
        IntStream.range(0, count).forEach(i -> publish(configuration, "tick", "none"));
    }

    /**
     * Appends an event while no node runs, through a configuration of its own that runs no workflow module.
     */
    private void appendDirectly(String eventName, String orderId) {
        var connection = new DisconnectableStorageEngine(storageEngine);
        var configuration = WorkflowConfigurer.create()
                                              .componentRegistry(cr -> cr.registerComponent(EventStorageEngine.class,
                                                                                            cfg -> connection))
                                              .build();
        configuration.start();
        try {
            publish(configuration, eventName, orderId);
        } finally {
            configuration.shutdown();
            connection.disconnect();
        }
    }

    private void awaitHibernated(AxonConfiguration configuration, Set<String> orderIds) {
        await().atMost(AWAIT).until(() -> isHibernated(configuration, orderIds));
    }

    /**
     * Asks the given workflows to hibernate at their next safe point, as a hibernation pass asks a workflow that made
     * no progress for the idle delay, until they hibernated. A request made while the workflow is not at a safe point
     * keeps it in memory, so the request is made again until it hibernates.
     */
    private void hibernateNow(AxonConfiguration configuration, Set<String> orderIds) {
        await().atMost(AWAIT).until(() -> {
            orderIds.forEach(orderId -> WorkflowEngineHibernation.requestHibernation(engine(configuration), orderId));
            return isHibernated(configuration, orderIds);
        });
    }

    private static boolean isHibernated(AxonConfiguration configuration, Set<String> orderIds) {
        var engine = engine(configuration);
        var inMemory = engine.workflowExecutions().stream().map(WorkflowExecution::workflowId)
                             .collect(Collectors.toSet());
        return engine.hibernatedWorkflowIds().containsAll(orderIds)
                && orderIds.stream().noneMatch(inMemory::contains);
    }

    /**
     * Takes checkpoints until the index holds a hint for each of the given workflows.
     */
    private void awaitHinted(AxonConfiguration configuration, Set<String> orderIds) {
        await().atMost(AWAIT).pollInterval(Duration.ofMillis(100)).until(() -> {
            tick(configuration, 1);
            var hinted = entries().stream()
                                  .filter(entry -> entry.hint() != null)
                                  .map(Entry::workflowId)
                                  .collect(Collectors.toSet());
            return hinted.containsAll(orderIds);
        });
    }

    private Set<Entry> entries() {
        return reader.load(MODULE, Segment.ROOT_SEGMENT)
                     .orTimeout(5, TimeUnit.SECONDS)
                     .join()
                     .orElse(Set.of());
    }

    /**
     * Waits until every segment stored a token at the head of the event store, so no checkpoint of the events published
     * so far is still in flight. A checkpoint that commits after {@link #stealClaims()} fails its claim check and
     * releases its segment, which the node claims again once the stolen claim expired.
     */
    private void awaitCheckpointsAtTheHead(AxonConfiguration configuration) {
        var head = storageEngine.latestToken().orTimeout(5, TimeUnit.SECONDS).join();
        var converter = configuration.getComponent(GeneralConverter.class);
        await().atMost(AWAIT).until(() -> {
            var stored = storedTokens(converter);
            return stored.size() == SEGMENTS && stored.stream().allMatch(token -> token != null && token.covers(head));
        });
    }

    private List<@Nullable TrackingToken> storedTokens(GeneralConverter converter) {
        try (var entityManager = entityManagerFactory.createEntityManager()) {
            return entityManager.createQuery("select t from TokenEntry t where t.processorName = :processor",
                                             TokenEntry.class)
                                .setParameter("processor", MODULE)
                                .getResultList()
                                .stream()
                                .<@Nullable TrackingToken>map(entry -> entry.getToken(converter))
                                .toList();
        }
    }

    /**
     * Makes another node the owner of every segment, as a node that claimed them after this one stalled.
     */
    private void stealClaims() {
        try (var entityManager = entityManagerFactory.createEntityManager()) {
            entityManager.getTransaction().begin();
            entityManager.createQuery("update TokenEntry t set t.owner = :owner, t.timestamp = :timestamp "
                                              + "where t.processorName = :processor")
                         .setParameter("owner", "another-node")
                         .setParameter("timestamp", TokenEntry.computeTokenTimestamp())
                         .setParameter("processor", MODULE)
                         .executeUpdate();
            entityManager.getTransaction().commit();
        }
    }

    private static Segment segmentOf(String workflowId) {
        return IntStream.range(0, SEGMENTS)
                        .mapToObj(segmentId -> new Segment(segmentId, SEGMENTS - 1))
                        .filter(segment -> segment.matches(workflowId))
                        .findFirst()
                        .orElseThrow();
    }

    private static WorkflowEngine engine(AxonConfiguration configuration) {
        return configuration.getComponents(WorkflowEngine.class).get("WorkflowEngine[" + MODULE + "]");
    }

    private static WorkflowState durableState(AxonConfiguration configuration, String workflowId) {
        return configuration.getComponent(UnitOfWorkFactory.class)
                            .create("read-" + workflowId)
                            .executeWithResult(ctx -> {
                                ctx.putResource(TEST_READ, true);
                                return configuration.getComponent(WorkflowStore.class).loadWorkflow(workflowId, ctx);
                            })
                            .orTimeout(5, TimeUnit.SECONDS)
                            .join();
    }

    /**
     * Counts the loads of the full running-workflow list, the fallback without a stored index, and the loads of each
     * workflow, noting those of a workflow whose claim did not catch up yet.
     */
    private final class CountingWorkflowStore implements WorkflowStore {

        private final WorkflowStore delegate;

        private CountingWorkflowStore(WorkflowStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public CompletableFuture<RunningWorkflows> loadRunningWorkflows(ProcessingContext processingContext) {
            fullLoads.incrementAndGet();
            return delegate.loadRunningWorkflows(processingContext);
        }

        @Override
        public CompletableFuture<WorkflowState> loadWorkflow(String workflowId, ProcessingContext processingContext) {
            if (!processingContext.containsResource(TEST_READ)) {
                loads.computeIfAbsent(workflowId, id -> new AtomicInteger()).incrementAndGet();
                var configuration = current;
                if (configuration != null
                        && !WorkflowEngineHibernation.isCaughtUp(engine(configuration), segmentOf(workflowId))) {
                    loadsBeforeCatchUp.incrementAndGet();
                }
            }
            return delegate.loadWorkflow(workflowId, processingContext);
        }

        @Override
        public CompletableFuture<Optional<WorkflowState>> findWorkflow(String workflowId,
                                                                       ProcessingContext processingContext) {
            return delegate.findWorkflow(workflowId, processingContext);
        }
    }

    /**
     * Counts the writes that set a hint, by whether they joined the processor's transaction, and fails every write
     * while {@link #failWrites} is set: a write within the processor's transaction then fails within it, as a write the
     * database rejects does, which makes that transaction roll back. While {@link #refuseToJoin} is set, it writes
     * nothing within the processor's transaction, as an index in another database.
     */
    private final class ObservedWorkflowIndex implements WorkflowIndex {

        private final WorkflowIndex delegate;

        private ObservedWorkflowIndex(WorkflowIndex delegate) {
            this.delegate = delegate;
        }

        @Override
        public CompletableFuture<Optional<Set<Entry>>> load(String indexName, Segment segment) {
            return delegate.load(indexName, segment);
        }

        @Override
        public CompletableFuture<Void> store(String indexName, IndexDelta delta) {
            if (setsHints(delta)) {
                hintsWrittenOtherwise.incrementAndGet();
            }
            if (failWrites.get()) {
                return CompletableFuture.failedFuture(new IllegalStateException("database down"));
            }
            return delegate.store(indexName, delta);
        }

        @Override
        public Optional<CompletableFuture<Void>> storeWithin(String indexName,
                                                             IndexDelta delta,
                                                             ProcessingContext context) {
            if (refuseToJoin.get()) {
                return Optional.empty();
            }
            if (failWrites.get()) {
                // An entry whose index name is longer than its column, which the database rejects.
                return delegate.storeWithin("i".repeat(256), new IndexDelta(Set.of("rejected"), Set.of()), context);
            }
            var written = delegate.storeWithin(indexName, delta, context);
            if (written.isPresent() && setsHints(delta)) {
                hintsWrittenWithinTheProcessorTransaction.incrementAndGet();
                context.doFinally(ended -> transactionsWithHintsEnded.incrementAndGet());
            }
            return written;
        }

        @Override
        public CompletableFuture<Void> initialize(String indexName, Set<String> runningWorkflowIds) {
            return delegate.initialize(indexName, runningWorkflowIds);
        }

        /**
         * Returns whether the given delta writes an entry with a hibernation hint.
         */
        private static boolean setsHints(IndexDelta delta) {
            return delta.running().values().stream().anyMatch(Objects::nonNull);
        }
    }

    /**
     * Counts the releases of the claim of each segment, which the processor makes once the release of that segment
     * completed.
     */
    private final class ReleaseCountingTokenStore implements TokenStore {

        private final TokenStore delegate;

        private ReleaseCountingTokenStore(TokenStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public CompletableFuture<List<Segment>> initializeTokenSegments(String processorName,
                                                                        int segmentCount,
                                                                        @Nullable TrackingToken initialToken,
                                                                        ProcessingContext context) {
            return delegate.initializeTokenSegments(processorName, segmentCount, initialToken, context);
        }

        @Override
        public CompletableFuture<Void> storeToken(@Nullable TrackingToken token,
                                                  String processorName,
                                                  int segmentId,
                                                  ProcessingContext context) {
            return delegate.storeToken(token, processorName, segmentId, context);
        }

        @Override
        public CompletableFuture<TrackingToken> fetchToken(String processorName,
                                                           int segmentId,
                                                           ProcessingContext context) {
            return delegate.fetchToken(processorName, segmentId, context);
        }

        @Override
        public CompletableFuture<TrackingToken> fetchToken(String processorName,
                                                           Segment segment,
                                                           ProcessingContext context) {
            return delegate.fetchToken(processorName, segment, context);
        }

        @Override
        public CompletableFuture<Void> extendClaim(String processorName, int segmentId, ProcessingContext context) {
            return delegate.extendClaim(processorName, segmentId, context);
        }

        @Override
        public CompletableFuture<Void> releaseClaim(String processorName, int segmentId, ProcessingContext context) {
            releasedClaims.computeIfAbsent(segmentId, id -> new AtomicInteger()).incrementAndGet();
            return delegate.releaseClaim(processorName, segmentId, context);
        }

        @Override
        public CompletableFuture<Void> initializeSegment(@Nullable TrackingToken token,
                                                         String processorName,
                                                         Segment segment,
                                                         ProcessingContext context) {
            return delegate.initializeSegment(token, processorName, segment, context);
        }

        @Override
        public CompletableFuture<Void> deleteToken(String processorName, int segmentId, ProcessingContext context) {
            return delegate.deleteToken(processorName, segmentId, context);
        }

        @Override
        public CompletableFuture<Segment> fetchSegment(String processorName,
                                                       int segmentId,
                                                       ProcessingContext context) {
            return delegate.fetchSegment(processorName, segmentId, context);
        }

        @Override
        public CompletableFuture<List<Segment>> fetchSegments(String processorName, ProcessingContext context) {
            return delegate.fetchSegments(processorName, context);
        }

        @Override
        public CompletableFuture<List<Segment>> fetchAvailableSegments(String processorName,
                                                                       ProcessingContext context) {
            return delegate.fetchAvailableSegments(processorName, context);
        }

        @Override
        public CompletableFuture<String> retrieveStorageIdentifier(ProcessingContext context) {
            return delegate.retrieveStorageIdentifier(context);
        }
    }
}
