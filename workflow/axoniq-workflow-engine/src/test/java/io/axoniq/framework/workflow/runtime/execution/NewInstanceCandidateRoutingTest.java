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
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.dsl.api.EventCondition;
import io.axoniq.framework.workflow.dsl.api.StepStatus;
import io.axoniq.framework.workflow.dsl.api.WorkflowContext;
import io.axoniq.framework.workflow.dsl.api.WorkflowState;
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowConfiguration;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowContextFactory;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecutionFactory;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import org.axonframework.common.TypeReference;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.sequencing.SequencingPolicy;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * How many start candidates a business event has decides how it is routed, and only the one-candidate case is well
 * covered elsewhere. This covers the other counts, and the ways deriving the count can go wrong:
 * <ul>
 *     <li><b>several distinct candidates</b> - no single owning segment exists, so the event is broadcast and each
 *     candidate is started by its owner alone;</li>
 *     <li><b>several definitions naming the same id</b> - the candidates are collected into a {@link java.util.Set},
 *     which dedups them back to one, so routing stays targeted. Asserted on the routing decision itself: an assertion
 *     that only counted instances would pass under broadcast too;</li>
 *     <li><b>no candidate</b> - nothing starts, but an instance resident on any segment may be waiting for the event,
 *     so it is broadcast;</li>
 *     <li><b>a derivation that throws</b> - degraded to a broadcast rather than allowed to fail the work package: a
 *     sequencing policy that throws stalls the segment, which is worse than an extra delivery;</li>
 *     <li><b>a {@code workflowIdProvider} deriving no id</b> - a misconfiguration, so that definition alone is
 *     dropped from the candidate set and skipped by the start path instead of stalling the segment, see
 *     {@link #aDefinitionDerivingNoWorkflowIdIsSkippedInsteadOfFailingTheWorkPackage()};</li>
 *     <li><b>several registered versions</b> - only the highest contributes a candidate, so the count is per
 *     definition and not per registration.</li>
 * </ul>
 * Every segment is left in replay mode, which is the state a node claiming a segment starts in: the starts below are
 * therefore materialized without their bodies being started, and that is asserted too.
 * <p>
 * The registry is the real {@link SimpleWorkflowConfigurationRegistry} throughout, so the version filtering under test
 * is the one the engine runs rather than a stub of it.
 */
class NewInstanceCandidateRoutingTest {

    private static final int SEGMENT_COUNT = 8;
    private static final List<Segment> SEGMENTS = IntStream.range(0, SEGMENT_COUNT)
                                                           .mapToObj(id -> new Segment(id, SEGMENT_COUNT - 1))
                                                           .toList();
    private static final QualifiedName START_EVENT = new QualifiedName("FanOutRequested");
    private static final QualifiedName UNREGISTERED_EVENT = new QualifiedName("NobodyStartsOnThis");

    /**
     * Far ahead of any segment's position, so no segment reaches live mode and no body is started.
     */
    private static final long STARTUP_LATEST_POSITION = 1_000;
    /**
     * Workflow ids the engine created an instance for, one entry per start it actually performed.
     */
    private final List<String> started = new ArrayList<>();
    /**
     * The executions those starts produced, so it can be asserted that none of their bodies ran.
     */
    private final List<WorkflowExecution> startedExecutions = new ArrayList<>();
    private final Map<WorkflowContext, String> idOfContext = new IdentityHashMap<>();
    private SimpleWorkflowConfigurationRegistry registry;
    private WorkflowEngineSequencingPolicy routing;
    private WorkflowEngine engine;
    private InMemoryWorkflowExecutionRepository repository;
    private WorkflowEngineCheckpointingSupport checkpointingSupport;

    /**
     * Two ids owned by different segments, found by scanning rather than assumed: {@code String.hashCode} is specified
     * by the JLS, so this is deterministic, and a scenario that happened to pick two ids on the same segment would pass
     * while testing nothing.
     */
    private static List<String> twoIdsOnDifferentSegments() {
        var first = "impure-0";
        var second = IntStream.rangeClosed(1, 1_000)
                              .mapToObj(index -> "impure-" + index)
                              .filter(id -> owningSegment(id).getSegmentId() != owningSegment(first).getSegmentId())
                              .findFirst()
                              .orElseThrow(() -> new AssertionError("No second id landed on another segment"));
        return List.of(first, second);
    }

    // ---------------------------------------------------------------------------------------------------------
    // several candidates
    // ---------------------------------------------------------------------------------------------------------

    private static String idOnAnotherSegmentThan(String workflowId) {
        return IntStream.rangeClosed(1, 1_000)
                        .mapToObj(index -> "publisher-" + index)
                        .filter(id -> owningSegment(id).getSegmentId() != owningSegment(workflowId).getSegmentId())
                        .findFirst()
                        .orElseThrow(() -> new AssertionError("No id landed on another segment"));
    }

    // ---------------------------------------------------------------------------------------------------------
    // several definitions, one id
    // ---------------------------------------------------------------------------------------------------------

    /**
     * A business event published by the given workflow through the publish primitive: the start event's type, carrying
     * the publisher's step metadata.
     */
    private static EventMessage publishedEvent(String publisherId) {
        var eventMessage = businessEvent(START_EVENT);
        when(eventMessage.metadata()).thenReturn(
                MetadataUtils.create(publisherId, "notify", StepStatus.COMPLETED)
                             .and(MetadataUtils.METADATA_KEY_STEP_PRIMITIVE, MetadataUtils.STEP_PRIMITIVE_PUBLISH)
        );
        return eventMessage;
    }

    // ---------------------------------------------------------------------------------------------------------
    // no candidate
    // ---------------------------------------------------------------------------------------------------------

    private static BiPredicate<EventMessage, ProcessingContext> always() {
        return (event, pc) -> true;
    }

    // ---------------------------------------------------------------------------------------------------------
    // a derivation that throws
    // ---------------------------------------------------------------------------------------------------------

    /**
     * Built with {@code doReturn} rather than {@code when}: these mocks are created from inside a Mockito answer (the
     * engine asks the factories while a stub is being served), and a nested {@code when} corrupts the ongoing
     * stubbing.
     */
    private static WorkflowContext workflowContext() {
        return mock(WorkflowContext.class);
    }

    private static io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecutionOperations workflowExecutionOperations() {
        var workflowExecutionOperations = mock(io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecutionOperations.class);
        var bodyContext = mock(ProcessingContext.class);
        doReturn(bodyContext).when(workflowExecutionOperations).processingContext();
        doAnswer(invocation -> {
            invocation.<Consumer<ProcessingContext>>getArgument(0).accept(bodyContext);
            return bodyContext;
        }).when(bodyContext).whenComplete(any());
        return workflowExecutionOperations;
    }

    private static WorkflowExecution execution(String workflowId) {
        var execution = mock(WorkflowExecutionFixture.CancellationCapableExecution.class);
        var state = mock(WorkflowState.class);
        doReturn(WorkflowStatus.STARTED).when(state).workflowStatus();
        doReturn(workflowId).when(execution).workflowId();
        doReturn(state).when(execution).state();
        doReturn(workflowExecutionOperations()).when(execution).workflowExecutionOperations();
        doReturn(mock(WorkflowCancellation.class)).when(execution).workflowCancellation();
        return execution;
    }

    // ---------------------------------------------------------------------------------------------------------
    // a workflowIdProvider that derives no id
    // ---------------------------------------------------------------------------------------------------------

    private static EventMessage startEvent() {
        return businessEvent(START_EVENT);
    }

    private static EventMessage unregisteredEvent() {
        return businessEvent(UNREGISTERED_EVENT);
    }

    private static EventMessage businessEvent(QualifiedName name) {
        var eventMessage = mock(EventMessage.class);
        when(eventMessage.metadata()).thenReturn(Metadata.emptyInstance());
        when(eventMessage.type()).thenReturn(new MessageType(name));
        when(eventMessage.payloadAs(any(TypeReference.class))).thenReturn(Map.of("id", "1"));
        return eventMessage;
    }

    // ---------------------------------------------------------------------------------------------------------
    // a derivation that does not agree with itself
    // ---------------------------------------------------------------------------------------------------------

    private static TrackingToken token(long position) {
        return new GlobalSequenceTrackingToken(position);
    }

    private static Segment owningSegment(String workflowId) {
        return SEGMENTS.stream()
                       .filter(segment -> WorkflowSegmentOwnership.ownedBy(segment, workflowId))
                       .findFirst()
                       .orElseThrow();
    }

    @BeforeEach
    void setUp() {
        registry = new SimpleWorkflowConfigurationRegistry();
        routing = new WorkflowEngineSequencingPolicy(registry);
        repository = new InMemoryWorkflowExecutionRepository();
        engine = new WorkflowEngine(registry,
                                    repository,
                                    mock(WorkflowCancellationService.class),
                                    mock(WorkflowStore.class),
                                    mock(UnitOfWorkFactory.class));
        checkpointingSupport = new WorkflowEngineCheckpointingSupport(engine);
        engine.setCheckpointingSupport(checkpointingSupport);
    }

    @Test
    void twoDefinitionsNamingTwoIdsAreBroadcastAndEachStartsOnItsOwnSegmentOnly() {
        register("Alpha", "1.0.0", always(), event -> "alpha-1");
        register("Beta", "1.0.0", always(), event -> "beta-1");
        var alphaOwner = owningSegment("alpha-1");
        var betaOwner = owningSegment("beta-1");

        assertThat(alphaOwner.getSegmentId())
                .as("the two candidates must land on different segments, or nothing here is cross-segment")
                .isNotEqualTo(betaOwner.getSegmentId());
        assertThat(routing.sequenceIdentifierFor(startEvent(), context(alphaOwner)))
                .as("two distinct candidates have no single owning segment, so the event must reach all of them")
                .contains(SequencingPolicy.BROADCAST);

        // The broadcast is offered to every segment; each is expected to start only what it owns.
        SEGMENTS.forEach(segment -> engine.handle(startEvent(), context(segment)));

        assertThat(started)
                .as("each candidate must be started exactly once over all %d segments, by its owner alone",
                    SEGMENT_COUNT)
                .containsExactlyInAnyOrder("alpha-1", "beta-1");
    }

    // ---------------------------------------------------------------------------------------------------------
    // several registered versions
    // ---------------------------------------------------------------------------------------------------------

    @Test
    void twoDefinitionsNamingTheSameIdStayTargetedRatherThanBroadcast() {
        register("Alpha", "1.0.0", always(), event -> "shared-1");
        register("Beta", "1.0.0", always(), event -> "shared-1");

        var decision = routing.sequenceIdentifierFor(startEvent(), context(owningSegment("shared-1")));

        assertThat(decision)
                .as("""
                            Routing decision for an event whose two definitions both name 'shared-1': %s. Expected the id \
                            itself. The candidates are collected into a Set, so the duplicate collapses and a single owning \
                            segment does exist. Broadcasting instead would still be correct in outcome, and an assertion that \
                            only counted instances would not notice, but it would cost a delivery on every one of the %d \
                            segments for an event that has one owner.""", decision, SEGMENT_COUNT)
                .contains("shared-1");
        assertThat(decision).isNotEqualTo(java.util.Optional.of(SequencingPolicy.BROADCAST));
    }

    @Test
    void anEventNoDefinitionStartsOnIsBroadcastAndStillWakesAWaitingInstance() {
        register("Alpha", "1.0.0", always(), event -> "alpha-1");
        var waitingId = "alpha-1";
        var waiting = execution(waitingId);
        repository.save(waitingId, () -> waiting);

        assertThat(routing.sequenceIdentifierFor(unregisteredEvent(), context(owningSegment(waitingId))))
                .as("no definition starts on this event, but an instance on any segment may be waiting for it")
                .contains(SequencingPolicy.BROADCAST);

        SEGMENTS.forEach(segment -> engine.handle(unregisteredEvent(), context(segment)));

        assertThat(started).as("an event with no start candidate must not create an instance").isEmpty();
        verify(waiting, times(1)).onEvent(any(), any());
    }

    // ---------------------------------------------------------------------------------------------------------
    // events published by a workflow through the publish primitive
    // ---------------------------------------------------------------------------------------------------------

    @Test
    void aStartConditionThatThrowsIsBroadcastInsteadOfFailingTheWorkPackage() {
        register("Alpha", "1.0.0", (event, pc) -> {
            throw new IllegalStateException("payload not converted yet");
        }, event -> "alpha-1");

        assertThatCode(() -> routing.sequenceIdentifierFor(startEvent(), context(SEGMENTS.getFirst())))
                .as("a sequencing policy that throws fails the work package and stalls that segment for good")
                .doesNotThrowAnyException();
        assertThat(routing.sequenceIdentifierFor(startEvent(), context(SEGMENTS.getFirst())))
                .contains(SequencingPolicy.BROADCAST);
    }

    @Test
    void aWorkflowIdProviderThatThrowsIsBroadcastInsteadOfFailingTheWorkPackage() {
        register("Alpha", "1.0.0", always(), event -> {
            throw new IllegalStateException("payload not converted yet");
        });

        assertThatCode(() -> routing.sequenceIdentifierFor(startEvent(), context(SEGMENTS.getFirst())))
                .doesNotThrowAnyException();
        assertThat(routing.sequenceIdentifierFor(startEvent(), context(SEGMENTS.getFirst())))
                .contains(SequencingPolicy.BROADCAST);
    }

    @Test
    void oneThrowingDefinitionDiscardsTheCandidatesOfItsHealthySiblingsToo() {
        register("Alpha", "1.0.0", always(), event -> "alpha-1");
        register("Beta", "1.0.0", always(), event -> {
            throw new IllegalStateException("payload not converted yet");
        });

        assertThat(routing.sequenceIdentifierFor(startEvent(), context(SEGMENTS.getFirst())))
                .as("""
                            The catch surrounds the whole stream, so one definition that cannot derive its id discards the \
                            candidates of every other definition on the same event and degrades it to a broadcast. Correct - \
                            every ownership guard narrows it back to exactly-once - and cheap only while such events are \
                            rare.""")
                .contains(SequencingPolicy.BROADCAST);
    }

    /**
     * An {@code idProperty} naming a field the event does not have makes {@code PayloadPropertyWorkflowIdProvider}
     * return {@code null}, on this event and on every later one of the same type. That is a misconfiguration, and the
     * start path must not carry it into the ownership guard: the guard derives a segment key from the id, so the work
     * package would fail and stall the segment, losing every instance that segment owns rather than only the one
     * misconfigured definition.
     * <p>
     * The provider is only known at runtime - an arbitrary function over the event, resolving property names against
     * the converted payload - so registration cannot reject it. Instead the start path reports the definition and skips
     * it: no instance, no stall, and an attributable error.
     */
    @Test
    void aDefinitionDerivingNoWorkflowIdIsSkippedInsteadOfFailingTheWorkPackage() {
        // The real provider, misconfigured exactly as a typo in idProperty would: the event carries 'id', not
        // 'orderId'. Nothing about the missing id below is arranged by the test.
        register("Alpha", "1.0.0", always(), new PayloadPropertyWorkflowIdProvider("orderId")::apply);
        var segment = SEGMENTS.getFirst();

        assertThat(new PayloadPropertyWorkflowIdProvider("orderId").apply(startEvent()))
                .as("precondition: an idProperty the event lacks yields a null id")
                .isNull();

        assertThat(routing.sequenceIdentifierFor(startEvent(), context(segment)))
                .as("a definition that derives no id contributes no candidate, and an event without a single "
                            + "candidate is broadcast")
                .contains(SequencingPolicy.BROADCAST);

        assertThatCode(() -> engine.handle(startEvent(), context(segment)))
                .as("""
                            A definition whose idProperty names a field the event lacks derives no workflow id. Handing that \
                            to the ownership guard would compute the segment key of nothing, fail the work package and stall \
                            the segment for every instance it owns. The start path must reject the definition instead.""")
                .doesNotThrowAnyException();

        assertThat(started).as("there is no id to start under, so no instance is created").isEmpty();
    }

    @Test
    void aDefinitionDerivingNoIdKeepsTheCandidateOfItsHealthySibling() {
        register("Alpha", "1.0.0", always(), event -> "alpha-1");
        register("Beta", "1.0.0", always(), new PayloadPropertyWorkflowIdProvider("orderId")::apply);

        assertThat(routing.sequenceIdentifierFor(startEvent(), context(SEGMENTS.getFirst())))
                .as("""
                            A definition that derives no id is dropped from the candidate set rather than collapsing it: the \
                            collector's own rejection of nulls would discard 'alpha-1' too and broadcast an event that has \
                            exactly one owner, on every event of this type. Contrast \
                            oneThrowingDefinitionDiscardsTheCandidatesOfItsHealthySiblingsToo, where the derivation throws \
                            and the whole stream is still lost.""")
                .contains("alpha-1");

        SEGMENTS.forEach(candidate -> engine.handle(startEvent(), context(candidate)));

        assertThat(started)
                .as("the healthy definition still starts exactly once, and the misconfigured one not at all")
                .containsExactly("alpha-1");
    }

    @Test
    void theOwnershipGuardRejectsAMissingWorkflowIdWithADiagnosticInsteadOfANullDereference() {
        String missingId = null;

        assertThatThrownBy(() -> WorkflowSegmentOwnership.ownedBy(SEGMENTS.getFirst(), missingId))
                .as("""
                            Defence in depth behind the start path's own rejection: no caller should reach the guard without \
                            an id, and one that does must be told what is wrong rather than dereference nothing while \
                            deriving the segment key.""")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("without a workflow id")
                .hasMessageContaining("idProperty");
        assertThatCode(() -> WorkflowSegmentOwnership.ownedBy(null, missingId))
                .as("unsegmented there is no ownership to decide, so the guard still short-circuits; the stall this "
                            + "protects against is segment-specific")
                .doesNotThrowAnyException();
    }

    /**
     * The start condition and the {@code workflowIdProvider} of a definition are evaluated twice per event: once here
     * while the event is sequenced, and again inside the engine's start path while it is handled. Nothing requires
     * either to be a pure function of the event, and nothing compares the two answers.
     * <p>
     * A provider whose answer changes between the two therefore aims the event at the segment owning the first id and
     * offers it, on that segment alone, an instance the segment does not own. The ownership guard rejects it, correctly
     * from its own point of view, and the event is gone: no instance, no failed step, no error, and only a
     * {@code DEBUG} line from the guard. A provider reading a clock, an RNG, a database sequence or falling back to
     * {@code UUID.randomUUID()} for an event without an id is enough; the counter below only makes the disagreement
     * observable.
     */
    @Test
    void anImpureWorkflowIdProviderRoutesToOneSegmentAndIsRejectedByIt_asExpectedGap() {
        var ids = twoIdsOnDifferentSegments();
        var derived = new ArrayList<String>();
        register("Alpha", "1.0.0", always(), event -> {
            var id = ids.get(Math.min(derived.size(), ids.size() - 1));
            derived.add(id);
            return id;
        });

        // Sequencing derives the first id, so the processor delivers this event to the segment owning that id only.
        var routed = routing.sequenceIdentifierFor(startEvent(), context(SEGMENTS.getFirst())).orElseThrow();
        assertThat(routed).as("the routing decision is taken on the first answer the provider gives")
                          .isEqualTo(ids.getFirst());
        var deliveredTo = owningSegment((String) routed);

        engine.handle(startEvent(), context(deliveredTo));

        assertThat(derived).as("the derivation ran twice for one event, and disagreed with itself")
                           .containsExactly(ids.getFirst(), ids.getLast());
        assertThat(owningSegment(ids.getLast()).getSegmentId())
                .as("precondition: the second answer is owned by another segment, which is the whole hazard")
                .isNotEqualTo(deliveredTo.getSegmentId());
        assertThat(started)
                .as("""
                            Instances created by the delivery: %s. The event was routed by '%s' to segment %d and the start \
                            path then asked for '%s', which segment %d does not own, so the ownership guard skipped it. No \
                            other segment is offered this event, so the start is lost outright - the outcome a purity \
                            requirement stated nowhere buys.""",
                    started, ids.getFirst(), deliveredTo.getSegmentId(), ids.getLast(),
                    deliveredTo.getSegmentId())
                .isEmpty();
    }

    /**
     * The same disagreement in the start condition rather than the id: true while the event is sequenced, false while
     * it is handled. The event is aimed at the candidate's owner and then no definition claims it there, so it is lost
     * in the same silence.
     */
    @Test
    void aStartConditionThatStopsMatchingAfterSequencingLosesTheEvent_asExpectedGap() {
        var evaluations = new AtomicInteger();
        register("Alpha", "1.0.0", (event, pc) -> evaluations.getAndIncrement() == 0, event -> "alpha-1");

        var routed = routing.sequenceIdentifierFor(startEvent(), context(SEGMENTS.getFirst())).orElseThrow();
        assertThat(routed).isEqualTo("alpha-1");

        engine.handle(startEvent(), context(owningSegment("alpha-1")));

        assertThat(evaluations).as("the start condition ran twice for one event: once to route it, once to act on it")
                               .hasValue(2);
        assertThat(started).as("a start condition that narrows between the two evaluations loses the event, because "
                                       + "the routing has already narrowed the delivery to one segment")
                           .isEmpty();
    }

    /**
     * The precise negative of the two above, and the reason this is a purity requirement rather than an ordering bug: a
     * disagreement in the other direction is harmless. A start condition that does not match while the event is
     * sequenced contributes no candidate, so the event is broadcast to every segment, and the definition that has
     * changed its mind by the time the event is handled still finds its owner among them. Only a derivation that
     * <em>narrows</em> after sequencing loses work.
     */
    @Test
    void aStartConditionThatOnlyStartsMatchingAfterSequencingStillStarts() {
        var evaluations = new AtomicInteger();
        register("Alpha", "1.0.0", (event, pc) -> evaluations.getAndIncrement() > 0, event -> "alpha-1");

        assertThat(routing.sequenceIdentifierFor(startEvent(), context(SEGMENTS.getFirst())))
                .as("no candidate at sequencing time, so the event goes to every segment")
                .contains(SequencingPolicy.BROADCAST);

        SEGMENTS.forEach(segment -> engine.handle(startEvent(), context(segment)));

        assertThat(started).as("the broadcast reaches the owner, which is why widening after sequencing is safe")
                           .containsExactly("alpha-1");
    }

    @Test
    void onlyTheHighestRegisteredVersionOfADefinitionContributesACandidate() {
        register("Alpha", "1.0.0", always(), event -> "alpha-v1");
        register("Alpha", "2.0.0", always(), event -> "alpha-v2");

        assertThat(routing.sequenceIdentifierFor(startEvent(), context(SEGMENTS.getFirst())))
                .as("""
                            Routing must see the single candidate the start path sees. Counting the superseded v1 as a second \
                            candidate would broadcast an event that has exactly one owner, on every event of this type.""")
                .contains("alpha-v2");
    }

    @Test
    void versionsBelowTheHighestNeitherRouteNorStart() {
        register("Alpha", "1.0.0", always(), event -> "alpha-v1");
        register("Beta", "1.0.0", always(), event -> "beta-v1");
        register("Alpha", "2.0.0", always(), event -> "alpha-v2");
        register("Beta", "2.0.0", always(), event -> "beta-v2");

        assertThat(routing.sequenceIdentifierFor(startEvent(), context(SEGMENTS.getFirst())))
                .as("two definitions at the highest version are two candidates")
                .contains(SequencingPolicy.BROADCAST);

        SEGMENTS.forEach(segment -> engine.handle(startEvent(), context(segment)));

        assertThat(started)
                .as("only the highest version of each definition starts, and each of those exactly once")
                .containsExactlyInAnyOrder("alpha-v2", "beta-v2");
    }

    @Test
    void aPublishedEventWithOneStartCandidateOnAnotherSegmentIsBroadcastSoThePublisherStillObservesIt() {
        register("Alpha", "1.0.0", always(), event -> "alpha-1");
        var publisherId = idOnAnotherSegmentThan("alpha-1");
        var publisher = execution(publisherId);
        repository.save(publisherId, () -> publisher);

        assertThat(routing.sequenceIdentifierFor(publishedEvent(publisherId), context(owningSegment(publisherId))))
                .as("candidate routing would deliver the event to alpha-1's segment only and leave the publisher on "
                            + "another segment waiting forever for its own step")
                .contains(SequencingPolicy.BROADCAST);

        SEGMENTS.forEach(segment -> engine.handle(publishedEvent(publisherId), context(segment)));

        assertThat(started).as("the published event starts the candidate exactly once").containsExactly("alpha-1");
        verify(publisher, times(1)).onEvent(any(), any());
    }

    /**
     * Registers a workflow definition on {@link #START_EVENT}. Its body is never run: these scenarios end at the moment
     * the instance is created, and creation is recorded in {@link #started}.
     */
    @SuppressWarnings("unchecked")
    private void register(String workflowName,
                          String version,
                          BiPredicate<EventMessage, ProcessingContext> startCondition,
                          Function<EventMessage, String> idProvider) {
        WorkflowConfiguration<WorkflowContext> configuration = mock(WorkflowConfiguration.class);
        WorkflowContextFactory<WorkflowContext> contextFactory = mock(WorkflowContextFactory.class);
        WorkflowExecutionFactory executionFactory = mock(WorkflowExecutionFactory.class);
        when(configuration.workflowName()).thenReturn(workflowName);
        when(configuration.workflowVersion()).thenReturn(version);
        when(configuration.workflowIdProvider()).thenReturn(idProvider::apply);
        when(configuration.workflowContextFactory()).thenReturn(contextFactory);
        when(configuration.workflowExecutionFactory()).thenReturn(executionFactory);
        // The engine builds the context once per start it actually performs, and hands it the id it starts under.
        when(contextFactory.createContext(anyMap(), any(), any(), eq(configuration)))
                .thenAnswer(invocation -> {
                    String workflowId = invocation.getArgument(1);
                    var workflowContext = workflowContext();
                    idOfContext.put(workflowContext, workflowId);
                    started.add(workflowId);
                    return workflowContext;
                });
        when(executionFactory.create(any())).thenAnswer(invocation -> {
            var created = execution(idOfContext.get(invocation.<WorkflowContext>getArgument(0)));
            startedExecutions.add(created);
            return created;
        });

        registry.register(new EventCondition() {
            @Override
            public BiPredicate<EventMessage, ProcessingContext> predicate() {
                return startCondition;
            }

            @Override
            public QualifiedName qualifiedName() {
                return START_EVENT;
            }
        }, configuration);
    }

    private ProcessingContext context(Segment segment) {
        var context = new StubProcessingContext();
        context.putResource(Segment.RESOURCE_KEY, segment);
        context.putResource(TrackingToken.RESOURCE_KEY, token(1));
        return context;
    }
}
