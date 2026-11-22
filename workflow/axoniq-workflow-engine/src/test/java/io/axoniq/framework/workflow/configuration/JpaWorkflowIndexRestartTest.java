/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.configuration;

import io.axoniq.framework.workflow.configuration.AbstractEventSourcedEntityRepositoryTestBase.TestContext;
import io.axoniq.framework.workflow.dsl.api.EventConditions;
import io.axoniq.framework.workflow.dsl.api.PayloadMapping;
import io.axoniq.framework.workflow.dsl.api.PrimitiveMetadata;
import io.axoniq.framework.workflow.dsl.api.Timing;
import io.axoniq.framework.workflow.dsl.api.WaitForStepDefinition;
import io.axoniq.framework.workflow.dsl.api.WorkflowState;
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.execution.DefaultEventNameCustomizer;
import io.axoniq.framework.workflow.runtime.execution.RunningWorkflows;
import io.axoniq.framework.workflow.runtime.execution.WorkflowEngine;
import io.axoniq.framework.workflow.runtime.execution.WorkflowEventTagResolver;
import io.axoniq.framework.workflow.runtime.execution.WorkflowEventTags;
import io.axoniq.framework.workflow.index.api.WorkflowIndex;
import io.axoniq.framework.workflow.runtime.execution.WorkflowStore;
import io.axoniq.framework.workflow.runtime.execution.payload.GlobalOnlyPayloadReducer;
import io.axoniq.framework.workflow.runtime.execution.payload.LocalOnlyPayloadReducer;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import io.axoniq.framework.workflow.index.jpa.JpaWorkflowIndex;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.Persistence;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.test.appender.ListAppender;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.configuration.DecoratorDefinition;
import org.axonframework.eventsourcing.eventstore.AppendCondition;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.GenericTaggedEventMessage;
import org.axonframework.eventsourcing.eventstore.TaggedEventMessage;
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.inmemory.InMemoryTokenStore;
import org.axonframework.messaging.eventstreaming.StreamingCondition;
import org.axonframework.messaging.eventstreaming.Tag;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Restarts the default {@link WorkflowConfigurer} wiring with a {@link JpaWorkflowIndex} against an event store, a
 * token store and a database that all outlive the stopped configuration, the way their durable counterparts do.
 * <p>
 * The oracles are which workflows the restarted configuration runs, and whether any claim fell back to loading the
 * full running-workflow list instead of the stored index plus the lifecycle events from the token it resumes from.
 *
 * @author Allard Buijze
 */
class JpaWorkflowIndexRestartTest {

    private static final String MODULE = "jpa-index-restart";
    private static final int SEGMENTS = 4;
    private static final int WORKFLOWS = 40;
    // Generous, as a segment whose checkpoint failed is claimed again only after a back-off that grows on repeats.
    private static final Duration AWAIT = Duration.ofSeconds(60);
    private static final WaitForStepDefinition AWAIT_PAID = new WaitForStepDefinition(
            new PrimitiveMetadata("awaitPaid", DefaultEventNameCustomizer.Builder.defaults()),
            EventConditions.fromQualifiedName(new QualifiedName("paid")),
            new PayloadMapping(LocalOnlyPayloadReducer.INSTANCE, GlobalOnlyPayloadReducer.INSTANCE),
            new Timing(Duration.ofMinutes(5))
    );
    // Package-private classes of the engine, so referred to by the names of their loggers.
    private static final String CLAIM_LOADER = WorkflowEngine.class.getPackageName() + ".RunningWorkflowsClaimLoader";
    private static final String PROJECTION = WorkflowEngine.class.getPackageName() + ".RunningWorkflowsProjection";

    private final EventStorageEngine storageEngine = new InMemoryEventStorageEngine();
    private final TokenStore tokenStore = new InMemoryTokenStore();
    private final EntityManagerFactory entityManagerFactory = Persistence.createEntityManagerFactory(
            "workflowIndex", Map.of("jakarta.persistence.jdbc.url", "jdbc:hsqldb:mem:" + UUID.randomUUID())
    );
    private final JpaWorkflowIndex reader = JpaWorkflowIndex.builder()
                                                            .entityManagerFactory(entityManagerFactory)
                                                            .build();
    private final AtomicInteger fullLoads = new AtomicInteger();
    private final AtomicInteger initializedLoads = new AtomicInteger();
    private final AtomicBoolean useIndex = new AtomicBoolean(true);
    private final AtomicBoolean failIndexWrites = new AtomicBoolean();
    private final AtomicBoolean failIndexLoads = new AtomicBoolean();
    private final AtomicInteger failedWrites = new AtomicInteger();
    private final AtomicBoolean dropTokenStores = new AtomicBoolean();
    private final AtomicInteger droppedTokenStores = new AtomicInteger();
    private final Set<String> runningBodies = ConcurrentHashMap.newKeySet();
    // The event store the next started configuration uses: the shared one, unless a test runs a node on another.
    private EventStorageEngine storageEngineOfNextNode = storageEngine;
    private final Map<AxonConfiguration, DisconnectableStorageEngine> configurations = new LinkedHashMap<>();
    private final Map<AxonConfiguration, CrashableTokenStore> tokenStores = new LinkedHashMap<>();

    @AfterEach
    void tearDown() {
        List.copyOf(configurations.keySet()).forEach(this::stop);
        entityManagerFactory.close();
    }

    @Test
    void restoresTheRunningWorkflowsFromTheStoredIndexWithoutLoadingTheFullList() {
        // given - workflows that completed, and workflows still waiting for an event
        var first = start();
        var completed = startWorkflows(first, WORKFLOWS);
        awaitRunning(first, completed);
        publish(first, "paid");
        await().atMost(AWAIT).until(() -> engine(first).workflowExecutions().isEmpty());
        var waiting = startWorkflows(first, WORKFLOWS);
        awaitRunning(first, waiting);
        stop(first);
        fullLoads.set(0);
        runningBodies.clear();

        // when
        var restarted = start();

        // then - exactly the waiting workflows run again, and no claim loaded the full running-workflow list
        awaitRunning(restarted, waiting);
        assertThat(runningBodies).containsExactlyInAnyOrderElementsOf(waiting);
        assertThat(fullLoads).hasValue(0);
        publish(restarted, "paid");
        await().atMost(AWAIT).until(() -> engine(restarted).workflowExecutions().isEmpty());
    }

    @Test
    void restoresSegmentsThatNeverOwnedARunningWorkflowWithoutLoadingTheFullList() {
        // given - one running workflow, so the other segments never saw a change of their running workflows
        var first = start();
        var running = startWorkflows(first, 1);
        awaitRunning(first, running);
        publish(first, "unrelated");
        stop(first);
        fullLoads.set(0);
        initializedLoads.set(0);
        runningBodies.clear();

        // when
        var restarted = start();

        // then - every segment finds the index initialized, also those whose entries were always empty
        awaitRunning(restarted, running);
        await().atMost(AWAIT).until(() -> initializedLoads.get() == SEGMENTS);
        assertThat(fullLoads).hasValue(0);
    }

    @Test
    void restoresEveryWorkflowAfterCheckpointsFailedToWriteTheIndex() {
        // given - workflows whose start reached the index
        var first = start();
        var indexed = startWorkflows(first, WORKFLOWS);
        awaitRunning(first, indexed);
        // and - workflows started while the index cannot be written, so the checkpoints of their segments fail, and
        // the processor releases those segments and claims them again
        failIndexWrites.set(true);
        var notIndexed = startWorkflows(first, WORKFLOWS);
        var all = new HashSet<>(indexed);
        all.addAll(notIndexed);
        await().atMost(AWAIT).until(() -> failedWrites.get() > 0);
        failIndexWrites.set(false);

        // when - the index can be written again
        // then - the claims of those segments restore every workflow
        awaitRunning(first, all);
        assertThat(runningBodies).containsAll(all);

        // when - the application restarts
        stop(first);
        fullLoads.set(0);
        runningBodies.clear();
        var restarted = start();

        // then - the stored index plus the lifecycle events after the stored tokens restore every workflow
        awaitRunning(restarted, all);
        assertThat(runningBodies).containsExactlyInAnyOrderElementsOf(all);
        assertThat(fullLoads).hasValue(0);
    }

    @Test
    void restoresWorkflowsWhoseStartNeverReachedTheIndexAfterACrash() {
        // given - workflows whose start reached the index
        var first = start();
        var indexed = startWorkflows(first, WORKFLOWS);
        awaitRunning(first, indexed);
        // and - workflows started while the index cannot be written, after which the node stops without writing them
        failIndexWrites.set(true);
        var notIndexed = startWorkflows(first, WORKFLOWS);
        await().atMost(AWAIT).until(() -> failedWrites.get() > 0);
        stop(first);
        failIndexWrites.set(false);
        fullLoads.set(0);
        runningBodies.clear();
        var all = new HashSet<>(indexed);
        all.addAll(notIndexed);

        // when
        var restarted = start();

        // then - the stored index holds the first workflows, the lifecycle events after the stored tokens the others
        awaitRunning(restarted, all);
        assertThat(runningBodies).containsExactlyInAnyOrderElementsOf(all);
        assertThat(fullLoads).hasValue(0);
    }

    @Test
    void restoresTheRunningWorkflowsWhenTheIndexIsIntroducedAfterTheTokensAdvanced() {
        // given - an application that ran workflows without an index, so its tokens passed their starts
        useIndex.set(false);
        var withoutIndex = start();
        var running = startWorkflows(withoutIndex, WORKFLOWS);
        awaitRunning(withoutIndex, running);
        stop(withoutIndex);
        runningBodies.clear();

        // when - it restarts with an index that holds none of them
        useIndex.set(true);
        var withIndex = start();

        // then - the index is not initialized, so the claims read the running workflows from the event store
        awaitRunning(withIndex, running);
        assertThat(runningBodies).containsExactlyInAnyOrderElementsOf(running);
        assertThat(fullLoads).hasPositiveValue();

        // when - it restarts once more
        stop(withIndex);
        fullLoads.set(0);
        runningBodies.clear();
        var restarted = start();

        // then - the claims that initialized the index make every later claim use it
        awaitRunning(restarted, running);
        assertThat(runningBodies).containsExactlyInAnyOrderElementsOf(running);
        assertThat(fullLoads).hasValue(0);
    }

    @Test
    void doesNotRestoreAWorkflowThatEndedWhileCheckpointsFailedToWriteTheIndex() {
        // given - workflows whose start reached the index
        var first = start();
        var ended = startWorkflows(first, WORKFLOWS);
        awaitRunning(first, ended);
        await().atMost(AWAIT).until(() -> indexed().containsAll(ended));
        // and - the workflows end while the index cannot be written, so the checkpoints of their segments fail, and
        // the processor releases those segments and claims them again
        failIndexWrites.set(true);
        runningBodies.clear();
        int loadsBefore = initializedLoads.get();
        publish(first, "paid");
        await().atMost(AWAIT).until(() -> failedWrites.get() > 0
                && initializedLoads.get() > loadsBefore);

        // then - the claims find their end in the lifecycle events after the stored tokens, and restore none of them
        await().atMost(AWAIT).until(() -> engine(first).workflowExecutions().isEmpty());
        assertThat(runningBodies).isEmpty();

        // when - the index can be written again, and the application restarts
        failIndexWrites.set(false);
        await().atMost(AWAIT).until(() -> indexed().isEmpty());
        stop(first);
        fullLoads.set(0);
        initializedLoads.set(0);
        var restarted = start();

        // then - no claim restores them, nor loads the full running-workflow list
        await().atMost(AWAIT).until(() -> initializedLoads.get() == SEGMENTS);
        assertThat(engine(restarted).workflowExecutions()).isEmpty();
        assertThat(runningBodies).isEmpty();
        assertThat(fullLoads).hasValue(0);
    }

    @Test
    void restoresEveryWorkflowFromEntriesAheadOfTheStoredTokensAfterACrash() {
        // given - workflows whose start reached the index
        var first = start();
        var indexed = startWorkflows(first, WORKFLOWS);
        awaitRunning(first, indexed);
        await().atMost(AWAIT).until(() -> indexed().containsAll(indexed));
        // and - workflows whose start reaches the index while the stores of the processor's tokens are lost, as when
        // the node crashes right after each write of the index, so the entries are ahead of the stored tokens
        dropTokenStores.set(true);
        var ahead = startWorkflows(first, WORKFLOWS);
        var all = new HashSet<>(indexed);
        all.addAll(ahead);
        await().atMost(AWAIT).until(() -> droppedTokenStores.get() > 0 && indexed().equals(all));

        // when - the node crashes: nothing it does reaches the event store or the token store anymore
        crash(first);
        dropTokenStores.set(false);
        fullLoads.set(0);
        runningBodies.clear();
        var restarted = start();

        // then - the stored entries plus the lifecycle events after the stored tokens restore every workflow once
        awaitRunning(restarted, all);
        assertThat(runningBodies).containsExactlyInAnyOrderElementsOf(all);
        assertThat(fullLoads).hasValue(0);
    }

    @Test
    void restoresEveryWorkflowAfterTokensAdvancedPastSkippedEventsWithChangesOfTheClaimPending() {
        // given - a workflow whose start reached the index
        var first = start();
        var running = startWorkflows(first, 1);
        awaitRunning(first, running);
        await().atMost(AWAIT).until(() -> indexed().containsAll(running));
        stop(first);
        Segment owner = ownerOf(running.iterator().next());
        // and - a restart whose claims cannot load the index, so they load the full running-workflow list, and the
        // claim of the owning segment keeps the workflow as a change to write, although the index holds it already
        failIndexLoads.set(true);
        var second = start();
        awaitRunning(second, running);
        failIndexLoads.set(false);

        // when - workflows of the other segments start, so the owning segment only skips their events, and stores
        // the tokens past them without a checkpoint of its own
        var skipped = startWorkflowsNotOwnedBy(second, owner, 3);
        var all = new HashSet<>(running);
        all.addAll(skipped);
        awaitRunning(second, all);
        var head = storageEngine.latestToken().orTimeout(5, TimeUnit.SECONDS).join();
        await().atMost(AWAIT).until(() -> storedToken(owner).covers(head));
        // and - the application stops without writing the change of that claim
        await().atMost(AWAIT).until(() -> indexed().containsAll(skipped));
        failIndexWrites.set(true);
        stop(second);
        failIndexWrites.set(false);
        fullLoads.set(0);
        runningBodies.clear();

        // when
        var restarted = start();

        // then - the index already held the workflow, so the change was never needed
        awaitRunning(restarted, all);
        assertThat(runningBodies).containsExactlyInAnyOrderElementsOf(all);
        assertThat(fullLoads).hasValue(0);
    }

    @Test
    void holdsBackOnlyTheSegmentOwningAWorkflowWithAnInvalidIdUntilATerminalEventOfThatWorkflowIsStored() {
        // given - a workflow that an earlier version of the engine started with an id longer than 255 characters,
        // which the current engine refuses to start: it runs under a valid id in an event store of its own, whose
        // events are then copied, in order, to the shared event store under the invalid id
        var invalidId = "legacy-" + "x".repeat(300);
        var placeholderId = UUID.randomUUID().toString();
        var legacyStorageEngine = new InMemoryEventStorageEngine();
        useIndex.set(false);
        storageEngineOfNextNode = legacyStorageEngine;
        var legacy = start();
        publish(legacy, new GenericEventMessage(
                placeholderId, new MessageType("start"), Map.of("orderId", "order-1"), Map.of(), Instant.now()
        ));
        awaitRunning(legacy, Set.of(placeholderId));
        awaitTokensCoverTheHead(legacyStorageEngine, Segment.splitBalanced(Segment.ROOT_SEGMENT, SEGMENTS - 1));
        stop(legacy);
        storageEngineOfNextNode = storageEngine;
        copyRenamingWorkflowId(legacyStorageEngine, placeholderId, invalidId);
        runningBodies.clear();
        Segment owner = ownerOf(invalidId);
        List<Segment> others = Segment.splitBalanced(Segment.ROOT_SEGMENT, SEGMENTS - 1)
                                      .stream()
                                      .filter(segment -> segment.getSegmentId() != owner.getSegmentId())
                                      .toList();
        var logs = new ListAppender("JpaWorkflowIndexRestartTest");
        logs.start();
        List<Logger> loggers = List.of((Logger) LogManager.getLogger(CLAIM_LOADER),
                                       (Logger) LogManager.getLogger(PROJECTION));
        loggers.forEach(logger -> logger.addAppender(logs));
        try {
            // when - the application restarts with the JPA index, and workflows start in every segment
            useIndex.set(true);
            fullLoads.set(0);
            var restarted = start();
            awaitRunning(restarted, Set.of(invalidId));
            var heldBack = startWorkflowsOwnedBy(restarted, owner, 1);
            var elsewhere = new HashSet<String>();
            others.forEach(segment -> elsewhere.addAll(startWorkflowsOwnedBy(restarted, segment, 1)));

            // then - the other segments handle their workflows and store their tokens past them
            awaitTokensCoverTheHead(storageEngine, others);
            assertThat(runningBodies).containsAll(elsewhere);
            // and - the owning segment runs its workflow, but every checkpoint of it fails with an error naming the
            // id, so its token stays before the start it handles, while the processor claims it again, and no claim
            // initializes the index, so each of them reads the full running-workflow list
            await().atMost(AWAIT).until(() -> runningBodies.containsAll(heldBack)
                    && errorsNaming(logs, PROJECTION, invalidId) >= 2
                    && fullLoads.get() > SEGMENTS
                    && errorsNaming(logs, CLAIM_LOADER, invalidId) > SEGMENTS);
            assertThat(storedToken(owner).covers(latestToken(storageEngine))).isFalse();
            assertThat(reader.load(MODULE, Segment.ROOT_SEGMENT).orTimeout(5, TimeUnit.SECONDS).join()).isEmpty();
            assertThat(initializedLoads).hasValue(0);

            // when - a terminal lifecycle event of the workflow with the invalid id is stored: a CANCELLED event,
            // published directly rather than through the engine's cancel path
            publish(restarted, new GenericEventMessage(
                    UUID.randomUUID().toString(),
                    new MessageType("legacyWorkflowCancelled"),
                    Map.of(),
                    MetadataUtils.create(invalidId, WorkflowStatus.CANCELLED),
                    Instant.now()
            ));

            // then - the segment recovers: it stores its token past every event again, and a claim initializes the
            // index
            awaitTokensCoverTheHead(storageEngine, List.of(owner));
            await().atMost(AWAIT).until(() -> reader.load(MODULE, Segment.ROOT_SEGMENT)
                                                     .orTimeout(5, TimeUnit.SECONDS)
                                                     .join()
                                                     .isPresent());
            var running = new HashSet<>(heldBack);
            running.addAll(elsewhere);
            await().atMost(AWAIT).until(() -> indexed().equals(running));
        } finally {
            loggers.forEach(logger -> logger.removeAppender(logs));
            logs.stop();
        }
    }

    private AxonConfiguration start() {
        var module = WorkflowModule.configure(MODULE, TestContext.class)
                                   .definition(d -> d
                                           .declarative(c -> ctx -> {
                                               runningBodies.add(ctx.workflowId());
                                               ctx.awaitEvent(AWAIT_PAID);
                                           })
                                           .workflowName(MODULE)
                                           .on(c -> EventConditions.fromQualifiedName(new QualifiedName("start")))
                                           .notCustomized()
                                   )
                                   .processorConfiguration(pc -> pc.initialSegmentCount(SEGMENTS))
                                   .contextFactory(c -> TestContext::new);
        var nodeStorageEngine = new DisconnectableStorageEngine(storageEngineOfNextNode);
        var nodeTokenStore = new CrashableTokenStore(tokenStore);
        var configuration = WorkflowConfigurer.create()
                .registerWorkflowIndex(cfg -> useIndex.get()
                        ? new FailingWritesWorkflowIndex(JpaWorkflowIndex.builder()
                                                                         .entityManagerFactory(entityManagerFactory)
                                                                         .build())
                        : WorkflowIndex.none())
                .componentRegistry(cr -> cr
                        .registerComponent(EventStorageEngine.class, cfg -> nodeStorageEngine)
                        .registerComponent(TokenStore.class, cfg -> nodeTokenStore)
                        .registerDecorator(DecoratorDefinition.forType(WorkflowStore.class)
                                                              .with((cfg, name, delegate) -> new CountingWorkflowStore(
                                                                      delegate
                                                              )))
                        .registerModule(module)
                )
                .build();
        configurations.put(configuration, nodeStorageEngine);
        tokenStores.put(configuration, nodeTokenStore);
        configuration.start();
        return configuration;
    }

    private void stop(AxonConfiguration configuration) {
        configuration.shutdown();
        configurations.remove(configuration).disconnect();
        tokenStores.remove(configuration).crash();
    }

    /**
     * Cuts the given node off from the event store and the token store before stopping it, so nothing it does on
     * shutdown reaches them, as after a crash. Its writes to the index still land, like writes in flight at a crash.
     */
    private void crash(AxonConfiguration configuration) {
        tokenStores.get(configuration).crash();
        configurations.get(configuration).disconnect();
        stop(configuration);
    }

    private Set<String> indexed() {
        return reader.load(MODULE, Segment.ROOT_SEGMENT)
                     .orTimeout(5, TimeUnit.SECONDS)
                     .join()
                     .orElse(Set.of());
    }

    private TrackingToken storedToken(Segment segment) {
        return tokenStore.fetchToken(MODULE, segment.getSegmentId(), new StubProcessingContext())
                         .orTimeout(5, TimeUnit.SECONDS)
                         .join();
    }

    private static TrackingToken latestToken(EventStorageEngine engine) {
        return engine.latestToken().orTimeout(5, TimeUnit.SECONDS).join();
    }

    private void awaitTokensCoverTheHead(EventStorageEngine engine, List<Segment> segments) {
        var head = latestToken(engine);
        await().atMost(AWAIT).until(() -> segments.stream().allMatch(segment -> storedToken(segment).covers(head)));
    }

    /**
     * Appends every event of the given {@code source} to the shared event store, in order, so the positions the
     * stored tokens refer to stay the same, with the workflow id {@code from} replaced by {@code to} in the metadata
     * and the tags of the workflow events.
     */
    private void copyRenamingWorkflowId(EventStorageEngine source, String from, String to) {
        var tagResolver = new WorkflowEventTagResolver();
        var head = latestToken(source);
        List<TaggedEventMessage<?>> copies = new ArrayList<>();
        MessageStream<EventMessage> stream = source.stream(
                StreamingCondition.startingFrom(source.firstToken().orTimeout(5, TimeUnit.SECONDS).join())
        );
        try {
            TrackingToken position = null;
            while (position == null || !position.covers(head)) {
                var entry = stream.next().orElseThrow();
                position = TrackingToken.fromContext(entry).orElseThrow();
                EventMessage event = entry.message();
                if (from.equals(event.metadata().get(MetadataUtils.METADATA_KEY_WORKFLOW_ID))) {
                    event = event.andMetadata(Map.of(MetadataUtils.METADATA_KEY_WORKFLOW_ID, to));
                }
                Set<Tag> tags = new HashSet<>(tagResolver.resolve(event));
                Tag.fromContext(entry).orElse(Set.of())
                   .stream()
                   .filter(tag -> !tag.key().equals(WorkflowEventTags.TAG_WORKFLOW_ID))
                   .forEach(tags::add);
                copies.add(new GenericTaggedEventMessage<>(event, tags));
            }
        } finally {
            stream.close();
        }
        storageEngine.appendEvents(AppendCondition.none(), new StubProcessingContext(), copies)
                     .thenCompose(EventStorageEngine.AppendTransaction::commit)
                     .orTimeout(5, TimeUnit.SECONDS)
                     .join();
    }

    /**
     * Counts the errors the given logger logged that name the given invalid workflow id, abbreviated.
     */
    private static long errorsNaming(ListAppender logs, String loggerName, String invalidId) {
        var abbreviated = "'" + invalidId.substring(0, 64) + "...'";
        return logs.getEvents()
                   .stream()
                   .filter(event -> event.getLevel() == Level.ERROR
                           && event.getLoggerName().equals(loggerName)
                           && event.getMessage().getFormattedMessage().contains(abbreviated))
                   .count();
    }

    private static Segment ownerOf(String workflowId) {
        return Segment.splitBalanced(Segment.ROOT_SEGMENT, SEGMENTS - 1)
                      .stream()
                      .filter(segment -> segment.matches(workflowId))
                      .findFirst()
                      .orElseThrow();
    }

    /**
     * Starts the given number of workflows whose ids the given {@code segment} owns, and returns their ids.
     */
    private static Set<String> startWorkflowsOwnedBy(AxonConfiguration configuration, Segment segment, int count) {
        Set<String> ids = new HashSet<>();
        while (ids.size() < count) {
            String id = UUID.randomUUID().toString();
            if (segment.matches(id)) {
                publish(configuration, new GenericEventMessage(
                        id, new MessageType("start"), Map.of("orderId", "order-1"), Map.of(), Instant.now()
                ));
                ids.add(id);
            }
        }
        return ids;
    }

    /**
     * Starts the given number of workflows whose ids the given {@code segment} does not own, and returns their ids.
     */
    private static Set<String> startWorkflowsNotOwnedBy(AxonConfiguration configuration, Segment segment, int count) {
        Set<String> ids = new HashSet<>();
        while (ids.size() < count) {
            String id = UUID.randomUUID().toString();
            if (!segment.matches(id)) {
                publish(configuration, new GenericEventMessage(
                        id, new MessageType("start"), Map.of("orderId", "order-1"), Map.of(), Instant.now()
                ));
                ids.add(id);
            }
        }
        return ids;
    }

    /**
     * Starts the given number of workflows and returns their ids. The default workflow id is the identifier of the
     * start event.
     */
    private static Set<String> startWorkflows(AxonConfiguration configuration, int count) {
        Set<String> ids = new HashSet<>();
        IntStream.range(0, count).forEach(index -> ids.add(publish(configuration, "start")));
        return ids;
    }

    private void awaitRunning(AxonConfiguration configuration, Set<String> workflowIds) {
        await().atMost(AWAIT)
               .until(() -> runningBodies.containsAll(workflowIds)
                       && engine(configuration).workflowExecutions().size() == workflowIds.size());
    }

    private static String publish(AxonConfiguration configuration, String eventName) {
        return publish(configuration, new GenericEventMessage(new MessageType(eventName), Map.of("orderId", "order-1")));
    }

    private static String publish(AxonConfiguration configuration, GenericEventMessage event) {
        configuration.getComponent(UnitOfWorkFactory.class)
                     .create("publish-" + event.type().name())
                     .executeWithResult(ctx -> configuration.getComponent(EventStore.class)
                                                            .publish(ctx, event)
                                                            .thenApply(ignored -> event))
                     .orTimeout(5, TimeUnit.SECONDS)
                     .join();
        return event.identifier();
    }

    private static WorkflowEngine engine(AxonConfiguration configuration) {
        return configuration.getComponents(WorkflowEngine.class).get("WorkflowEngine[" + MODULE + "]");
    }

    /**
     * Counts how often a claim loads the full running-workflow list, the fallback without a stored index.
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
            return delegate.loadWorkflow(workflowId, processingContext);
        }

        @Override
        public CompletableFuture<Optional<WorkflowState>> findWorkflow(String workflowId,
                                                                       ProcessingContext processingContext) {
            return delegate.findWorkflow(workflowId, processingContext);
        }
    }

    /**
     * Fails every write while {@link #failIndexWrites} is set, so the checkpoints of the segments with changes fail,
     * and counts the failed writes and the loads that find the index initialized.
     */
    private final class FailingWritesWorkflowIndex implements WorkflowIndex {

        private final WorkflowIndex delegate;

        private FailingWritesWorkflowIndex(WorkflowIndex delegate) {
            this.delegate = delegate;
        }

        @Override
        public CompletableFuture<Optional<Set<String>>> load(String indexName, Segment segment) {
            if (failIndexLoads.get()) {
                return CompletableFuture.failedFuture(new IllegalStateException("database down"));
            }
            return delegate.load(indexName, segment).whenComplete((index, failure) -> {
                if (index != null && index.isPresent()) {
                    initializedLoads.incrementAndGet();
                }
            });
        }

        @Override
        public CompletableFuture<Void> store(String indexName, IndexDelta delta) {
            if (failIndexWrites.get()) {
                failedWrites.incrementAndGet();
                return CompletableFuture.failedFuture(new IllegalStateException("database down"));
            }
            return delegate.store(indexName, delta);
        }

        @Override
        public CompletableFuture<Void> initialize(String indexName, Set<String> runningWorkflowIds) {
            if (failIndexWrites.get()) {
                return CompletableFuture.failedFuture(new IllegalStateException("database down"));
            }
            return delegate.initialize(indexName, runningWorkflowIds);
        }
    }

    /**
     * A token store of one node: loses every store of a token while {@link #dropTokenStores} is set, and fails every
     * operation once the node {@link #crash() crashed}.
     */
    private final class CrashableTokenStore implements TokenStore {

        private final TokenStore delegate;
        private final AtomicBoolean crashed = new AtomicBoolean();

        private CrashableTokenStore(TokenStore delegate) {
            this.delegate = delegate;
        }

        void crash() {
            crashed.set(true);
        }

        private <T> CompletableFuture<T> unlessCrashed(Supplier<CompletableFuture<T>> operation) {
            return crashed.get()
                    ? CompletableFuture.failedFuture(new IllegalStateException("crashed"))
                    : operation.get();
        }

        @Override
        public CompletableFuture<List<Segment>> initializeTokenSegments(String processorName,
                                                                        int segmentCount,
                                                                        @Nullable TrackingToken initialToken,
                                                                        ProcessingContext context) {
            return unlessCrashed(() -> delegate.initializeTokenSegments(processorName,
                                                                        segmentCount,
                                                                        initialToken,
                                                                        context));
        }

        @Override
        public CompletableFuture<Void> storeToken(@Nullable TrackingToken token,
                                                  String processorName,
                                                  int segmentId,
                                                  ProcessingContext context) {
            if (dropTokenStores.get()) {
                droppedTokenStores.incrementAndGet();
                return CompletableFuture.completedFuture(null);
            }
            return unlessCrashed(() -> delegate.storeToken(token, processorName, segmentId, context));
        }

        @Override
        public CompletableFuture<TrackingToken> fetchToken(String processorName,
                                                           int segmentId,
                                                           ProcessingContext context) {
            return unlessCrashed(() -> delegate.fetchToken(processorName, segmentId, context));
        }

        @Override
        public CompletableFuture<TrackingToken> fetchToken(String processorName,
                                                           Segment segment,
                                                           ProcessingContext context) {
            return unlessCrashed(() -> delegate.fetchToken(processorName, segment, context));
        }

        @Override
        public CompletableFuture<Void> extendClaim(String processorName, int segmentId, ProcessingContext context) {
            return unlessCrashed(() -> delegate.extendClaim(processorName, segmentId, context));
        }

        @Override
        public CompletableFuture<Void> releaseClaim(String processorName, int segmentId, ProcessingContext context) {
            return unlessCrashed(() -> delegate.releaseClaim(processorName, segmentId, context));
        }

        @Override
        public CompletableFuture<Void> initializeSegment(@Nullable TrackingToken token,
                                                         String processorName,
                                                         Segment segment,
                                                         ProcessingContext context) {
            return unlessCrashed(() -> delegate.initializeSegment(token, processorName, segment, context));
        }

        @Override
        public CompletableFuture<Void> deleteToken(String processorName, int segmentId, ProcessingContext context) {
            return unlessCrashed(() -> delegate.deleteToken(processorName, segmentId, context));
        }

        @Override
        public CompletableFuture<Segment> fetchSegment(String processorName,
                                                       int segmentId,
                                                       ProcessingContext context) {
            return unlessCrashed(() -> delegate.fetchSegment(processorName, segmentId, context));
        }

        @Override
        public CompletableFuture<List<Segment>> fetchSegments(String processorName, ProcessingContext context) {
            return unlessCrashed(() -> delegate.fetchSegments(processorName, context));
        }

        @Override
        public CompletableFuture<List<Segment>> fetchAvailableSegments(String processorName,
                                                                       ProcessingContext context) {
            return unlessCrashed(() -> delegate.fetchAvailableSegments(processorName, context));
        }

        @Override
        public CompletableFuture<String> retrieveStorageIdentifier(ProcessingContext context) {
            return unlessCrashed(() -> delegate.retrieveStorageIdentifier(context));
        }
    }
}
