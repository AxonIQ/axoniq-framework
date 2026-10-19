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

import io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration;
import io.axoniq.framework.testcontainer.AxonServerContainer;
import io.axoniq.framework.testcontainer.AxonServerContainerUtils;
import io.axoniq.framework.testcontainer.SharedAxonServerContainer;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.conversion.Converter;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.eventsourcing.annotation.EventTag;
import org.axonframework.eventsourcing.configuration.EventSourcedEntityModule;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurer;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.snapshot.api.SnapshotPolicy;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.conversion.DelegatingEventConverter;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.axonframework.messaging.eventstreaming.Tag;
import org.axonframework.modelling.EntityEvolver;
import org.axonframework.modelling.StateManager;
import org.axonframework.modelling.repository.Repository;
import org.junit.jupiter.api.*;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Reproduces an event being skipped when an entity is sourced from a snapshot in Axon Server.
 * <p>
 * A load that triggers a snapshot stores it at the consistency marker of that load. Axon Server's consistency marker
 * is the sequence the next event will get, while a snapshotted source only returns events with a sequence greater
 * than the snapshot's. The event appended right after the snapshotting load, typically by the command that loaded the
 * entity, lands exactly on the snapshot's sequence and is left out of every later load.
 */
class AxonServerSnapshotPositionIT {

    private static final String CONTEXT = "axon-server-snapshot-position-it";
    private static final AxonServerContainer CONTAINER = SharedAxonServerContainer.INSTANCE;
    private static final QualifiedName ACCOUNT = new QualifiedName(Account.class);

    private final String accountId = UUID.randomUUID().toString();

    private AxonConfiguration configuration;
    private StateManager stateManager;
    private EventStore eventStore;
    private UnitOfWorkFactory unitOfWorkFactory;
    private SnapshotStore snapshotStore;

    @BeforeAll
    static void startContainer() throws IOException {
        SharedAxonServerContainer.ensureStarted();
        try {
            AxonServerContainerUtils.deleteContext(CONTAINER.getHost(), CONTAINER.getHttpPort(), CONTEXT);
        } catch (IOException ignored) {
            // Context didn't exist yet.
        }
        AxonServerContainerUtils.createContext(CONTAINER.getHost(),
                                               CONTAINER.getHttpPort(),
                                               CONTEXT,
                                               AxonServerContainerUtils.DCB_CONTEXT);
    }

    @BeforeEach
    void setUp() {
        DelegatingEventConverter eventConverter =
                new DelegatingEventConverter(new JacksonConverter(JsonMapper.builder().build()));

        configuration = EventSourcingConfigurer.create()
                .componentRegistry(cr -> cr.registerComponent(Converter.class, c -> eventConverter))
                .componentRegistry(cr -> cr.registerComponent(
                        AxonServerConfiguration.class,
                        c -> AxonServerConfiguration.builder()
                                                    .componentName("AxonServerSnapshotPositionIT")
                                                    .servers(CONTAINER.getAxonServerAddress())
                                                    .context(CONTEXT)
                                                    .build()
                ))
                .registerEntity(
                        EventSourcedEntityModule.declarative(String.class, Account.class)
                                .messagingModel((c, model) -> model.entityEvolver(new AccountEvolver()).build())
                                .entityFactory(c -> (id, msg, ctx) -> {
                                    AccountCreated created = msg.payloadAs(AccountCreated.class, eventConverter);
                                    return new Account(created.id(), 0);
                                })
                                .criteriaResolver(c -> (id, ctx) -> EventCriteria.havingTags(Tag.of("account", id)))
                                .snapshotPolicy(SnapshotPolicy.afterEvents(5))
                                .build()
                )
                .build();
        configuration.start();

        stateManager = configuration.getComponent(StateManager.class);
        eventStore = configuration.getComponent(EventStore.class);
        unitOfWorkFactory = configuration.getComponent(UnitOfWorkFactory.class);
        snapshotStore = configuration.getComponent(SnapshotStore.class);
    }

    @AfterEach
    void tearDown() {
        configuration.shutdown();
    }

    /**
     * What a command handler does: load the entity, which snapshots it, and append an event in the same unit of work.
     */
    @Test
    void eventAppendedByTheSnapshottingUnitOfWorkIsSourcedAfterwards() {
        publish(new AccountCreated(accountId), new Deposited(accountId, 1), new Deposited(accountId, 1),
                new Deposited(accountId, 1), new Deposited(accountId, 1), new Deposited(accountId, 1));

        unitOfWorkFactory.create().executeWithResult(pc -> {
            Repository<String, Account> repository = stateManager.repository(Account.class, String.class);
            return repository.load(accountId, pc).thenAccept(account -> {
                assertThat(account.entity().balance()).isEqualTo(5);
                eventStore.transaction(pc).appendEvent(event(new Deposited(accountId, 100)));
            });
        }).join();
        awaitSnapshot();

        assertThat(loadAccount().balance()).isEqualTo(105);
    }

    /**
     * The same, with the event appended by a later unit of work, as long as nothing else was appended in between.
     */
    @Test
    void eventAppendedRightAfterTheSnapshottingLoadIsSourcedAfterwards() {
        publish(new AccountCreated(accountId), new Deposited(accountId, 1), new Deposited(accountId, 1),
                new Deposited(accountId, 1), new Deposited(accountId, 1), new Deposited(accountId, 1));

        assertThat(loadAccount().balance()).isEqualTo(5);
        awaitSnapshot();
        publish(new Deposited(accountId, 100));

        assertThat(loadAccount().balance()).isEqualTo(105);
    }

    private void awaitSnapshot() {
        await().atMost(Duration.ofSeconds(5))
               .untilAsserted(() -> assertThat(snapshotStore.load(ACCOUNT, accountId, null)
                                                            .get(1, TimeUnit.SECONDS)).isNotNull());
    }

    private void publish(Object... payloads) {
        eventStore.publish(null, Arrays.stream(payloads).map(AxonServerSnapshotPositionIT::event).toList()).join();
    }

    private static EventMessage event(Object payload) {
        return new GenericEventMessage(new MessageType(payload.getClass()), payload);
    }

    private Account loadAccount() {
        Repository<String, Account> repository = stateManager.repository(Account.class, String.class);
        return unitOfWorkFactory.create()
                                .executeWithResult(pc -> repository.load(accountId, pc))
                                .join()
                                .entity();
    }

    record AccountCreated(@EventTag(key = "account") String id) {}

    record Deposited(@EventTag(key = "account") String id, long amount) {}

    record Account(String id, long balance) {}

    private static class AccountEvolver implements EntityEvolver<Account> {

        @Override
        public Account evolve(Account account, EventMessage event, ProcessingContext context) {
            EventConverter converter = context.component(EventConverter.class);
            if (event.type().qualifiedName().equals(new QualifiedName(AccountCreated.class))) {
                return new Account(event.payloadAs(AccountCreated.class, converter).id(), 0);
            }
            Deposited deposited = event.payloadAs(Deposited.class, converter);
            return new Account(account.id(), account.balance() + deposited.amount());
        }
    }
}
