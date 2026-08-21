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
package io.axoniq.workflow.configuration;

import io.axoniq.workflow.runtime.api.execution.context.EventConditions;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.workflow.runtime.execution.DefaultEventNameCustomizer;
import io.axoniq.workflow.runtime.execution.EventSourcedWorkflowState;
import io.axoniq.workflow.runtime.execution.WorkflowAppendConditions;
import io.axoniq.workflow.runtime.execution.WorkflowEngine;
import io.axoniq.workflow.runtime.execution.WorkflowEventTags;
import io.axoniq.workflow.runtime.util.EventMessageUtils;
import jakarta.annotation.Nonnull;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.eventsourcing.eventstore.AppendCondition;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.SourcingCondition;
import org.axonframework.eventsourcing.eventstore.TaggedEventMessage;
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventstreaming.StreamingCondition;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventstreaming.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Restores two workflow instances of one segment while a previous owner appends for one of them, and checks that both
 * restored instances still record their own progress.
 * <p>
 * The position a restored instance conditions its appends from has to be its own. A position shared by every instance
 * of the claim is the lowest of its reads, so it can sit before an event the claim itself sourced; the restored
 * execution is then rejected by its own history, stops, and the instance makes no progress on any node.
 * <p>
 * The oracle is the durable log: a restored instance that can append records a terminal event, one that fences itself
 * records nothing further.
 */
class ClaimRestoreSelfFenceTest extends AbstractEventSourcedEntityRepositoryTestBase {

    private static final String MODULE = "claim-restore-self-fence";
    private static final MessageType DEFINITION_ID =
            new MessageType(new QualifiedName(MODULE), MessageType.DEFAULT_VERSION);
    private static final Executor DIRECT = Runnable::run;

    private final DefaultEventNameCustomizer customizer = DefaultEventNameCustomizer.Builder.defaults();

    @Test
    void aRestoredInstanceAppendsAfterAPreviousOwnerWroteDuringTheClaimsSourcing() {
        var storageEngine = new PreviousOwnerWritingStorageEngine(new InMemoryEventStorageEngine());
        configuration = configurationWith(storageEngine);
        configuration.start();

        var instanceA = workflowContext("wf-a", MessageType.DEFAULT_VERSION);
        var instanceB = workflowContext("wf-b", MessageType.DEFAULT_VERSION);
        publish(EventMessageUtils.startedWorkflow(instanceA, MODULE, DEFINITION_ID, customizer));
        publish(EventMessageUtils.startedWorkflow(instanceB, MODULE, DEFINITION_ID, customizer));

        // The previous owner of 'wf-a' records a step just before this claim reads that instance, so the claim reads
        // it, while the marker it shares across instances still sits at an earlier read.
        storageEngine.writeBefore("wf-a", () -> publish(step(instanceA, "approveOrder")));

        restoreSegment();

        assertThat(storageEngine.wroteDuringSourcing())
                .as("the previous owner's write must have landed during the claim, or this test proves nothing")
                .isTrue();

        assertThat(terminatedWithin("wf-b", Duration.ofSeconds(5)))
                .as("no previous owner wrote for 'wf-b', so it runs to a terminal record either way")
                .isTrue();
        assertThat(terminatedWithin("wf-a", Duration.ofSeconds(5)))
                .as("'wf-a' sourced that write while restoring, so its own appends are no conflict")
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

    private WorkflowExecution runningExecution(String workflowId) {
        return configuration.getComponent(WorkflowEngine.class)
                            .workflowExecutions()
                            .stream()
                            .filter(execution -> execution.workflowId().equals(workflowId))
                            .findFirst()
                            .orElseThrow(() -> new AssertionError("Workflow '" + workflowId + "' was not restored"));
    }

    private Throwable appendFailure(WorkflowExecution execution, EventMessage event) {
        var append = WorkflowAppendConditions.append(configuration.getComponent(EventStore.class),
                                                    configuration.getComponent(UnitOfWorkFactory.class),
                                                    DIRECT,
                                                    Context.empty(),
                                                    event,
                                                    execution);
        try {
            append.join();
            return null;
        } catch (Throwable failure) {
            return failure;
        }
    }

    private EventMessage step(WorkflowContext context, String stepName) {
        return EventMessageUtils.completedStep(context, stepName, Map.of("approved", true), null, customizer);
    }

    private AxonConfiguration configurationWith(PreviousOwnerWritingStorageEngine storageEngine) {
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
     * Runs a registered action right before the instance it names is sourced, so a write of a previous owner lands
     * between two reads of one claim.
     */
    private static final class PreviousOwnerWritingStorageEngine implements EventStorageEngine {

        private final EventStorageEngine delegate;
        private final AtomicBoolean wrote = new AtomicBoolean();
        private volatile String workflowId;
        private volatile Runnable write;

        private PreviousOwnerWritingStorageEngine(EventStorageEngine delegate) {
            this.delegate = delegate;
        }

        private void writeBefore(String workflowId, Runnable write) {
            this.workflowId = workflowId;
            this.write = write;
        }

        private boolean wroteDuringSourcing() {
            return wrote.get();
        }

        @Nonnull
        @Override
        public MessageStream<EventMessage> source(@Nonnull SourcingCondition condition,
                                                  ProcessingContext processingContext) {
            var pending = write;
            if (pending != null && sourcesArmedInstance(condition) && wrote.compareAndSet(false, true)) {
                pending.run();
            }
            return delegate.source(condition, processingContext);
        }

        private boolean sourcesArmedInstance(SourcingCondition condition) {
            var armed = workflowId;
            return armed != null && condition.criteria()
                                             .flatten()
                                             .stream()
                                             .anyMatch(criterion -> criterion.tags().contains(
                                                     Tag.of(WorkflowEventTags.TAG_WORKFLOW_ID, armed)));
        }

        @Nonnull
        @Override
        public CompletableFuture<AppendTransaction<?>> appendEvents(@Nonnull AppendCondition condition,
                                                                    ProcessingContext processingContext,
                                                                    @Nonnull List<TaggedEventMessage<?>> events) {
            return delegate.appendEvents(condition, processingContext, events);
        }

        @Nonnull
        @Override
        public MessageStream<EventMessage> stream(@Nonnull StreamingCondition condition) {
            return delegate.stream(condition);
        }

        @Nonnull
        @Override
        public CompletableFuture<TrackingToken> firstToken() {
            return delegate.firstToken();
        }

        @Nonnull
        @Override
        public CompletableFuture<TrackingToken> latestToken() {
            return delegate.latestToken();
        }

        @Nonnull
        @Override
        public CompletableFuture<TrackingToken> tokenAt(@Nonnull Instant at) {
            return delegate.tokenAt(at);
        }

        @Override
        public void describeTo(@Nonnull ComponentDescriptor descriptor) {
            descriptor.describeWrapperOf(delegate);
        }
    }
}
