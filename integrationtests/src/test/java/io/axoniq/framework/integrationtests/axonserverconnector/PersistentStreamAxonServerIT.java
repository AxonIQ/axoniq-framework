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
import io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration;
import io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager;
import io.axoniq.framework.axonserver.connector.event.PersistentStreamEventSource;
import io.axoniq.framework.axonserver.connector.event.PersistentStreamSequencingPolicy;
import io.axoniq.framework.integrationtests.testsuite.infrastructure.AxonServerTestInfrastructure;
import org.axonframework.common.Registration;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurer;
import org.axonframework.integrationtests.testsuite.infrastructure.TestInfrastructure;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.axonframework.messaging.eventhandling.gateway.EventAppender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
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

    @BeforeEach
    void setUp() {
        INFRASTRUCTURE.start();
        // EventSourcingConfigurer wires the AxonServerEventStorageEngine as EventSink so that
        // events published via EventAppender land in Axon Server's event store and can be
        // picked up by the persistent stream.
        configuration = EventSourcingConfigurer.create()
                                               .componentRegistry(INFRASTRUCTURE::configureInfrastructure)
                                               .start();
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
            // given
            String streamName = "single-event-" + UUID.randomUUID();
            PersistentStreamEventSource source = buildPersistentStreamSource(streamName);

            CopyOnWriteArrayList<EventMessage> received = new CopyOnWriteArrayList<>();
            Registration subscription = source.subscribe((events, ctx) -> {
                received.addAll(events);
                return CompletableFuture.completedFuture(null);
            });

            // when
            publishEvents(new StreamTestEvent("event-1"));

            // then
            await().atMost(15, TimeUnit.SECONDS)
                   .untilAsserted(() -> assertThat(received).hasSize(1));

            subscription.cancel();
        }

        @Test
        void subscriberReceivesMultiplePublishedEvents() {
            // given
            String streamName = "multi-event-" + UUID.randomUUID();
            PersistentStreamEventSource source = buildPersistentStreamSource(streamName);

            CopyOnWriteArrayList<EventMessage> received = new CopyOnWriteArrayList<>();
            Registration subscription = source.subscribe((events, ctx) -> {
                received.addAll(events);
                return CompletableFuture.completedFuture(null);
            });

            // when
            publishEvents(
                    new StreamTestEvent("event-1"),
                    new StreamTestEvent("event-2"),
                    new StreamTestEvent("event-3")
            );

            // then
            await().atMost(15, TimeUnit.SECONDS)
                   .untilAsserted(() -> assertThat(received).hasSize(3));

            subscription.cancel();
        }
    }

    @Nested
    class AckTokenPersistence {

        @Test
        void resubscribingWithSameStreamNameReceivesOnlyNewEvents() {
            // given
            String streamName = "ack-test-" + UUID.randomUUID();

            // Phase 1: subscribe and receive initial events
            PersistentStreamEventSource source = buildPersistentStreamSource(streamName);
            CopyOnWriteArrayList<EventMessage> phase1 = new CopyOnWriteArrayList<>();
            Registration subscription = source.subscribe((events, ctx) -> {
                phase1.addAll(events);
                return CompletableFuture.completedFuture(null);
            });

            publishEvents(new StreamTestEvent("initial-1"), new StreamTestEvent("initial-2"));
            await().atMost(15, TimeUnit.SECONDS)
                   .untilAsserted(() -> assertThat(phase1).hasSize(2));

            // when - cancel subscription; Axon Server persists the ack token
            subscription.cancel();

            // publish two more events while no subscriber is active
            publishEvents(new StreamTestEvent("new-1"), new StreamTestEvent("new-2"));

            // Phase 2: re-subscribe with the same stream name
            PersistentStreamEventSource source2 = buildPersistentStreamSource(streamName);
            CopyOnWriteArrayList<EventMessage> phase2 = new CopyOnWriteArrayList<>();
            Registration subscription2 = source2.subscribe((events, ctx) -> {
                phase2.addAll(events);
                return CompletableFuture.completedFuture(null);
            });

            // then - only the two new events arrive; initial events are not re-delivered
            await().atMost(15, TimeUnit.SECONDS)
                   .untilAsserted(() -> assertThat(phase2).hasSize(2));

            subscription2.cancel();
        }

        @Test
        void ackTokenSurvivesFullConfigurationRestart() {
            // given
            String streamName = "restart-test-" + UUID.randomUUID();

            // Phase 1: subscribe and receive initial events with the first configuration
            PersistentStreamEventSource source = buildPersistentStreamSource(streamName);
            CopyOnWriteArrayList<EventMessage> phase1 = new CopyOnWriteArrayList<>();
            Registration subscription = source.subscribe((events, ctx) -> {
                phase1.addAll(events);
                return CompletableFuture.completedFuture(null);
            });

            publishEvents(new StreamTestEvent("initial-1"), new StreamTestEvent("initial-2"));
            await().atMost(15, TimeUnit.SECONDS)
                   .untilAsserted(() -> assertThat(phase1).hasSize(2));

            subscription.cancel();

            // when - shut down the entire configuration (simulates application restart)
            configuration.shutdown();
            configuration = null;

            // start a fresh second configuration (new connection, new beans)
            configuration = EventSourcingConfigurer.create()
                                                   .componentRegistry(INFRASTRUCTURE::configureInfrastructure)
                                                   .start();

            // publish two new events via the second configuration
            publishEvents(new StreamTestEvent("after-restart-1"), new StreamTestEvent("after-restart-2"));

            // Phase 2: subscribe with the same stream name on the new configuration
            PersistentStreamEventSource source2 = buildPersistentStreamSource(streamName);
            CopyOnWriteArrayList<EventMessage> phase2 = new CopyOnWriteArrayList<>();
            Registration subscription2 = source2.subscribe((events, ctx) -> {
                phase2.addAll(events);
                return CompletableFuture.completedFuture(null);
            });

            // then - only the two post-restart events arrive; initial events are not re-delivered
            await().atMost(15, TimeUnit.SECONDS)
                   .untilAsserted(() -> assertThat(phase2).hasSize(2));

            subscription2.cancel();
        }
    }

    // ----- helpers -------------------------------------------------------

    /**
     * Constructs a {@link PersistentStreamEventSource} for the given {@code streamName} using the same pattern as
     * the declarative configuration documented in the connector reference guide.
     *
     * @param streamName the unique persistent stream identifier in Axon Server
     * @return a ready-to-subscribe {@link PersistentStreamEventSource}
     */
    private PersistentStreamEventSource buildPersistentStreamSource(String streamName) {
        AxonServerConnectionManager connectionManager =
                configuration.getComponent(AxonServerConnectionManager.class);
        AxonServerConfiguration serverConfig =
                configuration.getComponent(AxonServerConfiguration.class);
        EventConverter converter = configuration.getComponent(EventConverter.class);
        UnitOfWorkFactory unitOfWorkFactory = configuration.getComponent(UnitOfWorkFactory.class);

        PersistentStreamProperties properties = new PersistentStreamProperties(
                streamName,
                1,
                PersistentStreamSequencingPolicy.SEQUENTIAL_PER_AGGREGATE_POLICY,
                Collections.emptyList(),
                "HEAD",
                null
        );

        return new PersistentStreamEventSource(
                streamName,
                connectionManager,
                serverConfig,
                converter,
                properties,
                Executors.newScheduledThreadPool(1),
                unitOfWorkFactory,
                100
        );
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
    record StreamTestEvent(String id) {

    }
}
