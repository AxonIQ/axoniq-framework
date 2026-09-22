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
package io.axoniq.framework.workflow.runtime.test.utils;

import io.axoniq.framework.workflow.runtime.test.configuration.WorkflowTestEventPublicationEnhancer;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventSink;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Test-oriented event publishing adapter for workflow tests.
 *
 * <p>This component is used by the workflow test infrastructure to publish events into the configured
 * {@link EventSink} using the same messaging abstractions as the runtime. Tests can pass either a prebuilt
 * {@link EventMessage} or a plain event payload. When a plain payload is provided, this publisher creates a
 * {@link GenericEventMessage}, resolves its message type, assigns an id, stamps it with the configured clock, and
 * applies the configured {@link EventConverter} before publication.</p>
 *
 * <p>It is typically registered by test configuration helpers such as
 * {@link WorkflowTestEventPublicationEnhancer} and used indirectly
 * through higher-level APIs like {@link io.axoniq.framework.workflow.runtime.test.fixture.WorkflowTestDriver} and
 * {@link io.axoniq.framework.workflow.runtime.test.fixture.WorkflowTestFixture}. This keeps fixture tests close to real runtime
 * event publication while still allowing deterministic control over timestamps and generated identifiers.</p>
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
@Internal
public class TestEventPublisher {

    private static final Logger logger = LoggerFactory.getLogger(TestEventPublisher.class);

    private final EventSink eventSink;
    private final MessageTypeResolver messageTypeResolver;
    private final EventConverter converter;
    private final Clock clock;
    private final IdGenerator idGenerator;

    /**
     * Constructs a new {@link TestEventPublisher}
     *
     * @param eventSink           event sink to publish to
     * @param messageTypeResolver message type resolver
     * @param converter           event converter
     * @param clock               clock to use for event timestamps
     * @param idGenerator         id generator to use for event ids
     */
    public TestEventPublisher(EventSink eventSink,
                              MessageTypeResolver messageTypeResolver,
                              EventConverter converter,
                              Clock clock,
                              IdGenerator idGenerator
    ) {
        this.eventSink = eventSink;
        this.messageTypeResolver = messageTypeResolver;
        this.converter = converter;
        this.clock = clock;
        this.idGenerator = idGenerator;
    }


    /**
     * Publishes the event.
     *
     * @param event event message or payload to publish
     * @return a {@link CompletableFuture} that completes when the event has been published
     */
    public CompletableFuture<Void> publish(Object event) {
        var eventMessage = (event instanceof EventMessage) ? (EventMessage) event : new GenericEventMessage(
                idGenerator.next(),
                messageTypeResolver.resolveOrThrow(event),
                event,
                Map.of(),
                this.clock.instant()
        ).withConverter(converter);
        try {
            logger.info("Publishing Event: {}, {}",
                        eventMessage.type(),
                        eventMessage.payloadAs(Map.class)
            );
        } catch (Exception e) {
            logger.info("Publishing Event: {}",
                        eventMessage.type()
            );
        }
        return eventSink.publish(null, eventMessage);
    }
}
