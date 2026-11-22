/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.integrationtests.workflow;

import io.axoniq.framework.axonserver.connector.configuration.AxonServerConfigurationEnhancer;
import io.axoniq.framework.workflow.annotation.Workflow;
import io.axoniq.framework.workflow.configuration.WorkflowModule;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.framework.workflow.runtime.execution.WorkflowEngine;
import io.axoniq.framework.workflow.runtime.test.fixture.WorkflowTestDriver;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.SourcingCondition;
import org.axonframework.eventsourcing.eventstore.TerminalEventMessage;
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.annotation.Event;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.inmemory.InMemoryTokenStore;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.axonframework.messaging.eventstreaming.Tag;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.axoniq.framework.workflow.dsl.base.BaseWorkflowContext.equalsTo;
import static io.axoniq.framework.workflow.runtime.association.Associations.associate;
import static io.axoniq.framework.workflow.runtime.association.PayloadPropertyValueRetriever.payloadProperty;
import static io.axoniq.framework.workflow.runtime.execution.WorkflowEventTags.TAG_WORKFLOW_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Verifies that append conditions stop the second of two nodes that both wake the same hibernated workflow.
 * <p>
 * Two engines share one event store and each has its own token store, so both claim every segment and both receive
 * every event, the situation of a node that lost a segment claim without noticing. Both restore the waiting workflow,
 * both hibernate it, and both wake it when its event arrives. Each wake sources the workflow on its own, so only the
 * event store tells the two apart: the first append wins, and the other node's execution is stopped.
 *
 * @author Allard Buijze
 */
class HibernatedWorkflowStaleNodeWakeTest {

    private static final String WORKFLOW_NAME = "StaleWakeWorkflow";

    private final EventStorageEngine eventStorageEngine = new InMemoryEventStorageEngine();
    private final AtomicInteger bodyRuns = new AtomicInteger();

    @Test
    void twoNodesWakingOneHibernatedWorkflowRecordEachFactOnce() {
        var nodeA = startNode();
        WorkflowTestDriver nodeB = null;
        try {
            // given: the workflow waits for its payment, and a second node restored it too
            nodeA.publishEvent(new StartStaleWakeWorkflow("order-1"));
            await().atMost(Duration.ofSeconds(10))
                   .until(() -> eventsNamed("order-1", "AwaitPaymentStarted").size() == 1);
            nodeB = startNode();
            var b = nodeB;
            await().atMost(Duration.ofSeconds(10)).until(() -> runningWorkflowIds(b).contains("order-1")
                    || hibernatedWorkflowIds(b).contains("order-1"));
            // and both hibernated it
            await().atMost(Duration.ofSeconds(10)).until(() -> hibernatedWorkflowIds(nodeA).contains("order-1")
                    && hibernatedWorkflowIds(b).contains("order-1"));

            // when
            nodeA.publishEvent(new StalePaymentReceived("order-1"));

            // then: the workflow completed once, each fact is recorded once, and neither node keeps it
            await().atMost(Duration.ofSeconds(10))
                   .untilAsserted(() -> assertThat(eventsNamed("order-1", WORKFLOW_NAME + "Completed")).hasSize(1));
            assertThat(eventsWithTag("order-1")).extracting(event -> event.type().qualifiedName().toString())
                                                .doesNotHaveDuplicates();
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                assertThat(runningWorkflowIds(nodeA)).isEmpty();
                assertThat(runningWorkflowIds(b)).isEmpty();
                assertThat(hibernatedWorkflowIds(nodeA)).isEmpty();
                assertThat(hibernatedWorkflowIds(b)).isEmpty();
            });
            assertThat(bodyRuns.get()).isGreaterThanOrEqualTo(2);
        } finally {
            nodeA.shutdown();
            if (nodeB != null) {
                nodeB.shutdown();
            }
        }
    }

    private static WorkflowEngine engine(WorkflowTestDriver node) {
        return node.workflowTestServices().workflowEngine();
    }

    private static List<String> runningWorkflowIds(WorkflowTestDriver node) {
        return engine(node).workflowExecutions().stream().map(WorkflowExecution::workflowId).toList();
    }

    private static List<String> hibernatedWorkflowIds(WorkflowTestDriver node) {
        return List.copyOf(engine(node).hibernatedWorkflowIds());
    }

    private List<EventMessage> eventsWithTag(String workflowId) {
        MessageStream<EventMessage> stream = eventStorageEngine.source(
                SourcingCondition.conditionFor(EventCriteria.havingTags(Tag.of(TAG_WORKFLOW_ID, workflowId)))
        );
        try {
            return stream.reduce(new ArrayList<EventMessage>(), (events, entry) -> {
                if (!(entry.message() instanceof TerminalEventMessage)) {
                    events.add(entry.message());
                }
                return events;
            }).orTimeout(5, TimeUnit.SECONDS).join();
        } finally {
            stream.close();
        }
    }

    private List<EventMessage> eventsNamed(String workflowId, String localName) {
        return eventsWithTag(workflowId).stream()
                                        .filter(event -> event.type().qualifiedName().localName().equals(localName))
                                        .toList();
    }

    /**
     * Starts an engine with hibernation, sharing the event store with every other node of this test, but claiming
     * segments through a token store of its own, so it never learns that another node runs the same workflow.
     */
    private WorkflowTestDriver startNode() {
        var workflow = new StaleWakeWorkflow(bodyRuns);
        var module = WorkflowModule.defaults("stale-node-wake", SimpleWorkflowContext.class)
                                   .definition(d -> d.autodetected(c -> workflow));
        return WorkflowTestDriver.live(module, configurer -> configurer
                .hibernation(hibernation -> hibernation.enabled(true).idleDelay(Duration.ofMillis(200)))
                .componentRegistry(r -> r.disableEnhancer(AxonServerConfigurationEnhancer.class))
                .componentRegistry(cr -> cr
                        .registerComponent(EventStorageEngine.class, cfg -> eventStorageEngine)
                        .registerComponent(TokenStore.class, cfg -> new InMemoryTokenStore())));
    }

    public record StartStaleWakeWorkflow(String id) {

    }

    @Event(name = "StalePaymentReceived")
    public record StalePaymentReceived(String id) {

    }

    public static final class StaleWakeWorkflow {

        private final AtomicInteger bodyRuns;

        public StaleWakeWorkflow(AtomicInteger bodyRuns) {
            this.bodyRuns = bodyRuns;
        }

        @Workflow(
                workflowName = WORKFLOW_NAME,
                idProperty = "id",
                startOnEventClass = StartStaleWakeWorkflow.class
        )
        public void execute(SimpleWorkflowContext ctx) {
            bodyRuns.incrementAndGet();
            var id = String.valueOf(ctx.workflowPayload().get("id"));
            ctx.awaitEvent("awaitPayment",
                           StalePaymentReceived.class,
                           associate(payloadProperty("id"), equalsTo(id)),
                           step -> step.timeout(Duration.ofMinutes(5)));
        }
    }
}
