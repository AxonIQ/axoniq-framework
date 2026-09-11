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

import io.axoniq.framework.axonserver.connector.configuration.AxonServerConfigurationEnhancer;
import io.axoniq.framework.messaging.multitenancy.MultiTenancyUtils;
import io.axoniq.framework.workflow.configuration.WorkflowModule;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContextFactory;
import io.axoniq.framework.workflow.history.inmemory.InMemoryWorkflowHistoryRepository;
import io.axoniq.framework.workflow.history.inmemory.MutableWorkflowHistoryRepository;
import io.axoniq.framework.workflow.runtime.api.annotation.Workflow;
import io.axoniq.framework.workflow.runtime.api.execution.context.Version;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.framework.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.execution.WorkflowEventTagResolver;
import io.axoniq.framework.workflow.runtime.execution.payload.CombineGlobalAndLocalPayloadReducer;
import io.axoniq.framework.workflow.runtime.test.fixture.WorkflowTestDriver;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.eventsourcing.eventstore.AppendCondition;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.GenericTaggedEventMessage;
import org.axonframework.eventsourcing.eventstore.SourcingCondition;
import org.axonframework.eventsourcing.eventstore.TerminalEventMessage;
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.VersionedType;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.inmemory.InMemoryTokenStore;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.axonframework.messaging.eventstreaming.Tag;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static io.axoniq.framework.workflow.runtime.execution.WorkflowEventTags.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Verifies that append conditions stop a second writer of one workflow instance.
 * <p>
 * Two engines share one event store and each has its own token store, so both claim every segment, both receive every
 * event, and neither is told to release anything. That is the situation a node is in after it loses a segment claim
 * without noticing: it keeps running instances another node has taken over, and only the event store can tell the two
 * apart.
 *
 * @author Stefan Dragisic
 */
class ConcurrentWriterFencingTest {

    private static final VersionedType WORKFLOW_DEFINITION_ID =
            VersionedType.of(new QualifiedName("FencedWorkflow"), Version.DEFAULT_VERSION);

    private final EventStorageEngine eventStorageEngine = new InMemoryEventStorageEngine();
    private final JacksonConverter eventConverter = new JacksonConverter();
    private final List<String> bodyRuns = new CopyOnWriteArrayList<>();
    private final AtomicInteger stepActionRuns = new AtomicInteger();

    @Test
    void twoEnginesRunningOneInstanceRecordEachFactOnce() {
        var nodeA = startNode();
        var nodeB = startNode();
        try {
            nodeA.publishEvent(new StartFencedWorkflow("order-1"));

            // Both nodes spawn the instance; whichever loses a race is stopped, so neither keeps it running.
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                assertThat(bodyRuns).isNotEmpty();
                assertThat(eventsNamed("order-1", "FencedWorkflowCompleted")).hasSize(1);
            });
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(runningWorkflowIds(nodeA)).isEmpty()
            );
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(runningWorkflowIds(nodeB)).isEmpty()
            );

            // One started event, every fact of the instance recorded exactly once, and the step action ran once even
            // though both nodes executed the body.
            assertThat(eventsNamed("order-1", "FencedWorkflowStarted")).hasSize(1);
            assertThat(eventsWithTags(workflowTag("order-1")))
                    .extracting(event -> event.type().qualifiedName().toString())
                    .doesNotHaveDuplicates();
            assertThat(stepActionRuns).hasValue(1);
        } finally {
            nodeA.shutdown();
            nodeB.shutdown();
        }
    }

    @Test
    void startingAnInstanceWhoseIdentifierWasAlreadyUsedIsRejected() {
        seedTerminatedWorkflow("order-2");

        var node = startNode();
        try {
            node.publishEvent(new StartFencedWorkflow("order-2"));

            await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                    assertThat(runningWorkflowIds(node)).isEmpty()
            );

            // The seeded history is untouched, and the rejected start means the body never ran.
            assertThat(eventsWithTags(workflowTag("order-2"), lifecycleTag())).hasSize(2);
            assertThat(bodyRuns).isEmpty();
            assertThat(stepActionRuns).hasValue(0);
        } finally {
            node.shutdown();
        }
    }

    private void seedTerminatedWorkflow(String workflowId) {
        appendDirectly(new GenericEventMessage(
                new MessageType("FencedWorkflowStarted"),
                Map.of("id", workflowId),
                MetadataUtils.create(workflowId, WorkflowStatus.STARTED, WORKFLOW_DEFINITION_ID)
                             .and(MetadataUtils.METADATA_KEY_MODIFY_PAYLOAD, CombineGlobalAndLocalPayloadReducer.NAME)
        ).withConverter(eventConverter));
        appendDirectly(new GenericEventMessage(
                new MessageType("FencedWorkflowCompleted"),
                Map.of(),
                MetadataUtils.create(workflowId, WorkflowStatus.COMPLETED, WORKFLOW_DEFINITION_ID)
        ).withConverter(eventConverter));
    }

    private void appendDirectly(EventMessage eventMessage) {
        var tags = new LinkedHashSet<>(new WorkflowEventTagResolver().resolve(eventMessage));
        eventStorageEngine.appendEvents(AppendCondition.none(),
                                        null,
                                        List.of(new GenericTaggedEventMessage<>(eventMessage, Set.copyOf(tags))))
                          .join()
                          .commit()
                          .join();
    }

    private List<EventMessage> eventsWithTags(Tag... tags) {
        MessageStream<EventMessage> stream = eventStorageEngine.source(
                SourcingCondition.conditionFor(EventCriteria.havingTags(tags))
        );
        try {
            return stream.reduce(new ArrayList<EventMessage>(), (events, entry) -> {
                if (!(entry.message() instanceof TerminalEventMessage)) {
                    events.add(entry.message());
                }
                return events;
            }).join();
        } finally {
            stream.close();
        }
    }

    private List<EventMessage> eventsNamed(String workflowId, String localName) {
        return eventsWithTags(workflowTag(workflowId)).stream()
                                                      .filter(event -> event.type().qualifiedName().localName()
                                                                            .equals(localName))
                                                      .toList();
    }

    private static Tag workflowTag(String workflowId) {
        return Tag.of(TAG_WORKFLOW_ID, workflowId);
    }

    private static Tag lifecycleTag() {
        return Tag.of(TAG_WORKFLOW_EVENT_TYPE, TAG_VALUE_EVENT_TYPE_LIFECYCLE);
    }

    /**
     * Starts an engine sharing the event store with every other node of this test, but claiming segments through a
     * token store of its own, so it never learns that another node runs the same instances.
     */
    private WorkflowTestDriver startNode() {
        var workflow = new FencedWorkflow(bodyRuns, stepActionRuns);
        var module = WorkflowModule.defaults("concurrent-writer-fencing", SimpleWorkflowContext.class)
                                   .workflowContextFactory(c -> new SimpleWorkflowContextFactory())
                                   .definition(d -> d.autodetected(c -> workflow));

        return WorkflowTestDriver.live(module, configurer -> configurer
                .componentRegistry(MultiTenancyUtils::disable)
                .componentRegistry(r -> r.disableEnhancer(AxonServerConfigurationEnhancer.class))
                .componentRegistry(cr -> cr
                        .registerComponent(EventStorageEngine.class, cfg -> eventStorageEngine)
                        .registerComponent(MutableWorkflowHistoryRepository.class,
                                           cfg -> new InMemoryWorkflowHistoryRepository())
                        .registerComponent(TokenStore.class, cfg -> new InMemoryTokenStore())));
    }

    /**
     * Returns the ids of the instances the given node is running.
     */
    private static List<String> runningWorkflowIds(WorkflowTestDriver node) {
        return node.workflowTestServices()
                   .workflowEngine()
                   .workflowExecutions()
                   .stream()
                   .map(WorkflowExecution::workflowId)
                   .sorted()
                   .toList();
    }

    public record StartFencedWorkflow(String id) {

    }

    public static final class FencedWorkflow {

        private final List<String> bodyRuns;
        private final AtomicInteger stepActionRuns;

        public FencedWorkflow(List<String> bodyRuns, AtomicInteger stepActionRuns) {
            this.bodyRuns = bodyRuns;
            this.stepActionRuns = stepActionRuns;
        }

        @Workflow(
                workflowName = "FencedWorkflow",
                idProperty = "id",
                startOnEventClass = StartFencedWorkflow.class
        )
        public void execute(SimpleWorkflowContext ctx) {
            bodyRuns.add(ctx.workflowId());
            ctx.awaitExecute("ship", Boolean.class, () -> {
                stepActionRuns.incrementAndGet();
                return true;
            });
        }
    }
}
