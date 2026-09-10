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
package io.axoniq.framework.workflow.simulation.scenarios;

import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import io.axoniq.framework.workflow.simulation.harness.EngineInstance;
import io.axoniq.framework.workflow.simulation.harness.Polling;
import io.axoniq.framework.workflow.simulation.harness.SimulationWorld;
import io.axoniq.framework.workflow.simulation.workflow.CountingEffects;
import io.axoniq.framework.workflow.simulation.workflow.PublishChainWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.PublishChainRequestedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.PublishReplyEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.PublishRequestEvent;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.SimpleEventHandlingComponent;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.configuration.EventProcessorModule;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.eventstreaming.StreamableEventSource;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;

import static io.axoniq.framework.workflow.simulation.scenarios.PublishOracles.isTerminal;

/**
 * A published event is a regular Axon Framework event: a plain {@code EventHandlingComponent} on its own pooled
 * streaming processor, registered next to the workflow engine through the harness's component-registry seam, receives
 * the requester's published request and the responder's published reply exactly once each, with the business payload
 * and the publisher's {@code workflowId} in the metadata.
 * <p>
 * ORACLE: the handler's received messages, by distinct identifier, per event type; payload equality; metadata. WORKLOAD:
 * the three chain definitions plus one plain handler, one order id, no faults. EVIDENCE: the handler is subscribed on
 * exactly the two business types and records what it is handed. AMBIGUITY: a handler that received nothing within the
 * deadline fails the run. BUDGET: one seed, ~10 s.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class PublishToEventHandlerScenario {

    private static final Duration DEADLINE = Duration.ofSeconds(15);

    private PublishToEventHandlerScenario() {
    }

    /**
     * What the plain handler received.
     *
     * @param requestsReceived   distinct request events handled (expected 1).
     * @param repliesReceived    distinct reply events handled (expected 1).
     * @param requestPayloadOk   the request payload equals the published business event.
     * @param replyPayloadOk     the reply payload equals the published business event.
     * @param requestFromRequester the request carries the requester's {@code workflowId} metadata.
     * @param replyFromResponder   the reply carries the responder's {@code workflowId} metadata.
     */
    public record Outcome(int requestsReceived, int repliesReceived, boolean requestPayloadOk, boolean replyPayloadOk,
                          boolean requestFromRequester, boolean replyFromResponder) {
    }

    /**
     * Runs the chain with a plain event handler listening.
     *
     * @param seed    world seed.
     * @param orderId order id shared by the chain.
     * @return the outcome.
     */
    public static Outcome run(long seed, String orderId) {
        var effects = new CountingEffects();
        var received = new CopyOnWriteArrayList<EventMessage>();
        var requestType = new QualifiedName(PublishRequestEvent.class);
        var replyType = new QualifiedName(PublishReplyEvent.class);
        var handler = SimpleEventHandlingComponent.create("publishObserver")
                                                  .subscribe(requestType, (event, context) -> {
                                                      received.add(event);
                                                      return MessageStream.empty();
                                                  })
                                                  .subscribe(replyType, (event, context) -> {
                                                      received.add(event);
                                                      return MessageStream.empty();
                                                  });
        // The same wiring an application's own processor gets: the configured event store as source and the configured
        // token store; nothing workflow-specific.
        var observerProcessor = EventProcessorModule
                .pooledStreaming("publish-observer")
                .eventHandlingComponents(components -> components.declarative("publishObserverComponent", cfg -> handler))
                .customized((cfg, processor) -> processor
                        .eventSource(cfg.getComponent(StreamableEventSource.class))
                        .tokenStore(cfg.getComponent(TokenStore.class))
                        .unitOfWorkFactory(cfg.getComponent(UnitOfWorkFactory.class)))
                .build();
        try (var world = SimulationWorld.withExtraRegistrations(seed, EngineInstance.publishChainWorkflow(effects),
                                                                 registry -> registry.registerModule(observerProcessor))) {
            String requester = PublishChainWorkflow.REQUESTER_ID_PREFIX + orderId;
            String responder = PublishChainWorkflow.RESPONDER_ID_PREFIX + orderId;
            world.engine().publish(new PublishChainRequestedEvent(orderId));
            Polling.awaitOrFail(DEADLINE, "the chain to complete with a plain handler registered next to the engine",
                                () -> isTerminal(world.committedLog(), requester));
            Polling.awaitOrFail(DEADLINE, "the plain handler to receive both published events (received so far: "
                                        + received.stream().map(e -> e.type().qualifiedName().toString()).toList() + ")",
                                () -> received.stream().filter(e -> requestType.equals(e.type().qualifiedName())).count() >= 1
                                        && received.stream().filter(e -> replyType.equals(e.type().qualifiedName())).count() >= 1);
            List<EventMessage> requests = distinct(received, requestType);
            List<EventMessage> replies = distinct(received, replyType);
            return new Outcome(
                    requests.size(),
                    replies.size(),
                    requests.stream().allMatch(e -> new PublishRequestEvent(orderId).equals(e.payloadAs(PublishRequestEvent.class))),
                    replies.stream().allMatch(e -> new PublishReplyEvent(orderId).equals(e.payloadAs(PublishReplyEvent.class))),
                    requests.stream().allMatch(e -> requester.equals(MetadataUtils.getWorkflowId(e.metadata()))),
                    replies.stream().allMatch(e -> responder.equals(MetadataUtils.getWorkflowId(e.metadata()))));
        }
    }

    private static List<EventMessage> distinct(List<EventMessage> received, QualifiedName type) {
        return received.stream()
                       .filter(e -> type.equals(e.type().qualifiedName()))
                       .filter(e -> Objects.nonNull(e.identifier()))
                       .collect(java.util.stream.Collectors.toMap(EventMessage::identifier, e -> e, (a, b) -> a,
                                                                  java.util.LinkedHashMap::new))
                       .values().stream().toList();
    }
}
