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

package io.axoniq.framework.integrationtests.axonserverconnector;

import io.axoniq.axonserver.connector.event.PersistentStreamProperties;
import io.axoniq.framework.axonserver.connector.event.PersistentStreamEventSource;
import io.axoniq.framework.axonserver.connector.event.PersistentStreamEventSourceFactory;
import io.axoniq.framework.axonserver.connector.event.PersistentStreamScheduledExecutorBuilder;
import io.axoniq.framework.axonserver.connector.event.PersistentStreamSequencingPolicy;
import io.axoniq.framework.integrationtests.testsuite.infrastructure.AxonServerTestInfrastructure;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurer;
import org.axonframework.integrationtests.testsuite.infrastructure.TestInfrastructure;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.sequencing.SequentialPolicy;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventHandlingComponent;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.SimpleEventHandlingComponent;
import org.axonframework.messaging.eventhandling.annotation.Event;
import org.axonframework.messaging.eventhandling.configuration.EventProcessorModule;
import org.axonframework.messaging.eventhandling.gateway.EventAppender;
import org.axonframework.messaging.eventhandling.processing.subscribing.SubscribingEventProcessorModule;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.*;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration test verifying that {@link PersistentStreamEventSource} receives events published to Axon Server
 * via a persistent stream, matching the declarative configuration documented in the connector reference guide.
 * <p>
 * Each test uses a unique stream name (UUID suffix) to prevent cross-test contamination when the shared Axon Server
 * container is reused across test runs in the same JVM.
 * <p>
 * Run with:
 * <pre>{@code
 * ./mvnw -Pintegration-test verify -pl integrationtests -Dit.test=PersistentStreamAxonServerIT
 * }</pre>
 *
 * @author Jakob Hatzl
 * @since 5.2.0
 */
class PersistentStreamAxonServerIT {

    private static final TestInfrastructure INFRASTRUCTURE = new AxonServerTestInfrastructure();

    private AxonConfiguration configuration;
    private String streamName;
    private CopyOnWriteArrayList<EventMessage> received = new CopyOnWriteArrayList<>();

    @BeforeEach
    void setUp() {
        INFRASTRUCTURE.start();
        INFRASTRUCTURE.purgeData();
        streamName = "stream-" + UUID.randomUUID();
        received = new CopyOnWriteArrayList<>();
        configuration = buildConfiguration();
    }

    @AfterEach
    void tearDown() {
        if (configuration != null) {
            configuration.shutdown();
            configuration = null;
        }
        INFRASTRUCTURE.stop();
    }

    @Nested
    class EventDelivery {

        @Test
        void subscriberReceivesSinglePublishedEvent() {
            // when
            publishEvents(new StreamTestEvent("event-1"));

            // then
            await().atMost(15, TimeUnit.SECONDS)
                   .untilAsserted(() -> assertThat(received).hasSize(1));
        }

        @Test
        void subscriberReceivesMultiplePublishedEvents() {
            // when
            publishEvents(
                    new StreamTestEvent("event-1"),
                    new StreamTestEvent("event-2"),
                    new StreamTestEvent("event-3")
            );

            // then
            await().atMost(15, TimeUnit.SECONDS)
                   .untilAsserted(() -> assertThat(received).hasSize(3));
        }

    }

    @Nested
    class AckTokenPersistence {

        @Test
        void resubscribingWithSameStreamNameReceivesOnlyNewEvents() {
            // given
            String streamName = "restart-test-" + UUID.randomUUID();

            // Phase 1: subscribe and receive initial events with the first configuration
            publishEvents(new StreamTestEvent("initial-1"), new StreamTestEvent("initial-2"));
            await().atMost(15, TimeUnit.SECONDS)
                   .untilAsserted(() -> assertThat(received).hasSize(2));

            // when - shut down the entire configuration (simulates application restart) and clear received events
            configuration.shutdown();
            configuration = null;
            received.clear();

            // start a fresh second configuration (new connection, new beans)
            configuration = buildConfiguration();

            // publish two new events via the second configuration
            publishEvents(new StreamTestEvent("after-restart-1"), new StreamTestEvent("after-restart-2"));

            // then - only the two post-restart events arrive; initial events are not re-delivered
            await().atMost(15, TimeUnit.SECONDS)
                   .untilAsserted(() -> assertThat(received).hasSize(2));
        }
    }

    // ----- helpers -------------------------------------------------------
    private @NonNull AxonConfiguration buildConfiguration() {
        var module = buildProcessorModule();
        return EventSourcingConfigurer.create()
                                      .componentRegistry(INFRASTRUCTURE::configureInfrastructure)
                                      .messaging(msg -> msg.eventProcessing(
                                                         ep ->
                                                                 ep.subscribing(
                                                                         subscribing -> subscribing.processor(module))
                                                 )
                                      )
                                      .start();
    }

    private @NonNull SubscribingEventProcessorModule buildProcessorModule() {
        return EventProcessorModule.subscribing("persistent-stream-test-module")
                                   .eventHandlingComponents(components -> components.declarative(
                                           "persistent-streams-handling-component",
                                           cfg -> buildPersistentStreamsTestComponent()
                                   )).customized((c, subscribing) -> subscribing.eventSource(
                        buildPersistentStreamSource(streamName, c)
                ));
    }

    /**
     * {@link SimpleEventHandlingComponent} to test persistent stream connection
     *
     * @return the {@link SimpleEventHandlingComponent}
     */
    private EventHandlingComponent buildPersistentStreamsTestComponent() {
        SimpleEventHandlingComponent handlingComponent =
                SimpleEventHandlingComponent.create("persistent-streams-test-handling", SequentialPolicy.INSTANCE);
        handlingComponent.subscribe(

                new QualifiedName("test", "StreamTestEvent"),
                (eventMsg, ctx) -> {
                    received.add(eventMsg);
                    return MessageStream.empty();
                }
        );
        return handlingComponent;
    }

    /**
     * Constructs a {@link PersistentStreamEventSource} for the given {@code streamName} using the same pattern as
     * the declarative configuration documented in the connector reference guide.
     *
     * @param streamName the unique persistent stream identifier in Axon Server
     * @param c the running application configuration
     * @return a ready-to-subscribe {@link PersistentStreamEventSource}
     */
    private PersistentStreamEventSource buildPersistentStreamSource(String streamName, Configuration c) {
        int segmentCount = 1;
        int batchSize = 100;

        PersistentStreamProperties properties = new PersistentStreamProperties(
                streamName,
                segmentCount,
                PersistentStreamSequencingPolicy.SEQUENTIAL_POLICY,
                Collections.emptyList(),
                "HEAD",
                null  // no server-side filter
        );

        return PersistentStreamEventSourceFactory.defaultFactory()
                                                 .build(streamName,
                                                        properties,
                                                        PersistentStreamScheduledExecutorBuilder.defaultFactory().build(
                                                                segmentCount,
                                                                streamName),
                                                        batchSize,
                                                        c);
    }

    /**
     * Publishes the given {@code events} to Axon Server via {@link EventAppender} inside a {@link
     * org.axonframework.messaging.core.unitofwork.UnitOfWork}. The call blocks until Axon Server acknowledges the
     * append (or times out after 10 seconds).
     *
     * @param events the event payloads to publish
     */
    private void publishEvents(Object... events) {
        UnitOfWorkFactory unitOfWorkFactory = configuration.getComponent(UnitOfWorkFactory.class);
        var uow = unitOfWorkFactory.create();
        uow.runOnInvocation(ctx -> EventAppender.forContext(ctx).append(events));
        uow.execute().orTimeout(10, TimeUnit.SECONDS).join();
    }

    /**
     * Simple event payload used in the persistent stream tests.
     *
     * @param id a human-readable event identifier
     */
    @Event(namespace = "test",
            name = "StreamTestEvent",
            version = "1.0.0")
    record StreamTestEvent(String id) {

    }
}
