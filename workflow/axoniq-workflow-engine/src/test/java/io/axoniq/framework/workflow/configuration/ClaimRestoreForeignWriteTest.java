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
package io.axoniq.framework.workflow.configuration;

import io.axoniq.framework.workflow.runtime.api.execution.context.EventConditions;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.framework.workflow.runtime.execution.DefaultEventNameCustomizer;
import io.axoniq.framework.workflow.runtime.execution.EventSourcedWorkflowState;
import io.axoniq.framework.workflow.runtime.execution.WorkflowEngine;
import io.axoniq.framework.workflow.runtime.execution.WorkflowEventTags;
import io.axoniq.framework.workflow.runtime.util.EventMessageUtils;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.eventsourcing.eventstore.AppendCondition;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.SourcingCondition;
import org.axonframework.eventsourcing.eventstore.TaggedEventMessage;
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventstreaming.StreamingCondition;
import org.axonframework.messaging.eventstreaming.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Restores one instance while a previous owner's write for that same instance lands <em>inside</em> the instance's own
 * sourcing read, after the read has taken its snapshot.
 * <p>
 * {@code ClaimRestoreSelfFenceTest} covers the write that lands <em>before</em> the instance is read (the claim sources
 * it, so the restored execution may append) and the write that lands <em>after</em> the whole restore (the restored
 * execution is fenced). This covers the window between the two: the read is already in flight when the write lands.
 * The restored execution cannot have sourced that write, so it must not be allowed to append over it.
 * <p>
 * The second arm is the other direction of the same claim: with nothing written during the read, the restored
 * execution must never be rejected by its own history.
 */
class ClaimRestoreForeignWriteTest extends AbstractEventSourcedEntityRepositoryTestBase {

    private static final String MODULE = "claim-restore-foreign-write";
    private static final MessageType DEFINITION_ID =
            new MessageType(new QualifiedName(MODULE), MessageType.DEFAULT_VERSION);

    private static final Duration BUDGET = Duration.ofSeconds(5);

    private final DefaultEventNameCustomizer customizer = DefaultEventNameCustomizer.Builder.defaults();

    @Test
    void aWriteLandingInsideTheRestoreReadFencesTheRestoredExecution() {
        var storageEngine = new ForeignWritingStorageEngine(new InMemoryEventStorageEngine());
        configuration = configurationWith(storageEngine);
        configuration.start();

        var instanceA = workflowContext("wf-a", MessageType.DEFAULT_VERSION);
        var instanceB = workflowContext("wf-b", MessageType.DEFAULT_VERSION);
        publish(EventMessageUtils.startedWorkflow(instanceA, MODULE, DEFINITION_ID, customizer));
        publish(EventMessageUtils.startedWorkflow(instanceB, MODULE, DEFINITION_ID, customizer));

        // The previous owner records a step while this claim is reading 'wf-a', after the read took its snapshot.
        storageEngine.writeDuringSourcingOf("wf-a", () -> publish(step(instanceA, "approveOrder")));

        restoreSegment();

        assertThat(storageEngine.wroteDuringSourcing())
                .as("the previous owner's write must have landed during the read, or this test proves nothing")
                .isTrue();
        assertThat(terminatedWithin("wf-b", BUDGET))
                .as("no previous owner wrote for 'wf-b', so the same claim runs it to a terminal record")
                .isTrue();
        assertThat(terminatedWithin("wf-a", BUDGET))
                .as("'wf-a' did not source that write, so its own appends are rejected and it records nothing further")
                .isFalse();
    }

    @Test
    void aRestoredExecutionWithNoForeignWriteIsNeverRejectedByItsOwnHistory() {
        configuration = configurationWith(new ForeignWritingStorageEngine(new InMemoryEventStorageEngine()));
        configuration.start();

        var instanceA = workflowContext("wf-a", MessageType.DEFAULT_VERSION);
        var instanceB = workflowContext("wf-b", MessageType.DEFAULT_VERSION);
        publish(EventMessageUtils.startedWorkflow(instanceA, MODULE, DEFINITION_ID, customizer));
        publish(EventMessageUtils.startedWorkflow(instanceB, MODULE, DEFINITION_ID, customizer));
        publish(step(instanceA, "approveOrder"));

        restoreSegment();

        assertThat(terminatedWithin("wf-a", BUDGET))
                .as("nothing was written during the read, so the restored execution appends")
                .isTrue();
        assertThat(terminatedWithin("wf-b", BUDGET))
                .as("a sibling of the same claim is not fenced by another instance's events")
                .isTrue();
    }

    /**
     * Returns whether the given instance recorded a terminal workflow event within the given {@code budget}.
     */
    private boolean terminatedWithin(String workflowId, Duration budget) {
        var deadline = System.nanoTime() + budget.toNanos();
        while (System.nanoTime() < deadline) {
            if (isTerminal(workflowId)) {
                return true;
            }
            Thread.onSpinWait();
        }
        return isTerminal(workflowId);
    }

    private boolean isTerminal(String workflowId) {
        return configuration.getComponent(UnitOfWorkFactory.class)
                            .create("read-" + workflowId)
                            .executeWithResult(ctx -> repository(EventSourcedWorkflowState.class)
                                    .loadOrCreate(workflowId, ctx)
                                    .thenApply(managed -> managed.entity().workflowStatus().isTerminal()))
                            .join();
    }

    /**
     * Claims the whole key space and restores every instance it owns, sourcing on the claim's own context the way the
     * processor's claim callback does.
     */
    private void restoreSegment() {
        var workflowEngine = configuration.getComponent(WorkflowEngine.class);
        configuration.getComponent(UnitOfWorkFactory.class)
                     .create("segment-claim")
                     .executeWithResult(claim -> workflowEngine
                             .restoreWorkflowsFor(Segment.ROOT_SEGMENT, null, claim, claim)
                             .thenApply(ignored -> null))
                     .join();
    }

    private EventMessage step(WorkflowContext context, String stepName) {
        return EventMessageUtils.completedStep(context, stepName, Map.of("approved", true), null, customizer);
    }

    private AxonConfiguration configurationWith(ForeignWritingStorageEngine storageEngine) {
        var module = WorkflowModule.defaults(MODULE, TestContext.class)
                                   .workflowContextFactory(c -> TestContext::new)
                                   .definition(d -> d
                                           .declarative(c -> ctx -> {
                                           })
                                           .workflowName(MODULE)
                                           .on(c -> EventConditions.fromQualifiedName(new QualifiedName("start")))
                                           .notCustomized()
                                   );
        var configurer = WorkflowConfigurer.create();
        configurer.componentRegistry(cr -> cr
                .registerComponent(EventStorageEngine.class, cfg -> storageEngine)
                .registerModule(module));
        return configurer.build();
    }

    /**
     * Runs a registered action <em>after</em> the read of the instance it names has taken its snapshot, so a write of a
     * previous owner lands inside that instance's own sourcing read.
     */
    private static final class ForeignWritingStorageEngine implements EventStorageEngine {

        private final EventStorageEngine delegate;
        private final AtomicBoolean wrote = new AtomicBoolean();
        private volatile String workflowId;
        private volatile Runnable write;

        private ForeignWritingStorageEngine(EventStorageEngine delegate) {
            this.delegate = delegate;
        }

        private void writeDuringSourcingOf(String workflowId, Runnable write) {
            this.workflowId = workflowId;
            this.write = write;
        }

        private boolean wroteDuringSourcing() {
            return wrote.get();
        }

                @Override
        public MessageStream<EventMessage> source(SourcingCondition condition,
                                                  ProcessingContext processingContext) {
            var stream = delegate.source(condition, processingContext);
            var pending = write;
            if (pending != null && sourcesArmedInstance(condition) && wrote.compareAndSet(false, true)) {
                pending.run();
            }
            return stream;
        }

        private boolean sourcesArmedInstance(SourcingCondition condition) {
            var armed = workflowId;
            return armed != null && condition.criteria()
                                             .flatten()
                                             .stream()
                                             .anyMatch(criterion -> criterion.tags().contains(
                                                     Tag.of(WorkflowEventTags.TAG_WORKFLOW_ID, armed)));
        }

                @Override
        public CompletableFuture<AppendTransaction<?>> appendEvents(AppendCondition condition,
                                                                    ProcessingContext processingContext,
                                                                    List<TaggedEventMessage<?>> events) {
            return delegate.appendEvents(condition, processingContext, events);
        }

                @Override
        public MessageStream<EventMessage> stream(StreamingCondition condition) {
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
}
