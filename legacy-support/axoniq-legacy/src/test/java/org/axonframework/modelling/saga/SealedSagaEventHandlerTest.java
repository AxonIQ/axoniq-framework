/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package org.axonframework.modelling.saga;

import org.axonframework.common.FutureUtils;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.config.SagaConfigurer;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.annotation.UnsupportedHandlerException;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventSink;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.axonframework.modelling.saga.repository.SagaStore;
import org.axonframework.modelling.saga.repository.inmemory.InMemorySagaStore;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.axonframework.messaging.eventhandling.EventTestUtils.asEventMessage;

/**
 * Shows that a {@link SagaEventHandler @SagaEventHandler} whose parameter is a sealed type handles every permitted
 * subtype, each published under its own name, in one Saga per association value. The Saga model names each handler by
 * the payload type of its member, and the framework gives a sealed-type handler one member per subtype. A family with a
 * {@code non-sealed} branch cannot be listed, so the application refuses to start.
 */
class SealedSagaEventHandlerTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    private final InMemorySagaStore sagaStore = new InMemorySagaStore();

    private @Nullable AxonConfiguration configuration;

    @BeforeEach
    void setUp() {
        AccountSaga.HANDLED.clear();
    }

    @AfterEach
    void tearDown() {
        if (configuration != null) {
            configuration.shutdown();
        }
    }

    @Nested
    class ClosedFamily {

        @Test
        void everySubtypeReachesTheOneSagaOfItsAssociation() {
            // given
            configuration = configurer(AccountSaga.class).start();

            // when
            publish(asEventMessage(new AccountOpened("account-1")));
            publish(asEventMessage(new MoneyDeposited("account-1")));

            // then
            assertThat(AccountSaga.HANDLED).containsExactly(new AccountOpened("account-1"),
                                                            new MoneyDeposited("account-1"));
            assertThat(sagaStore.findSagas(AccountSaga.class, new AssociationValue("accountId", "account-1")))
                    .hasSize(1);
        }

        @Test
        void everySubtypeReachesTheSagaFromASerializedPayload() {
            // given events as an event store delivers them: a byte[] payload under the subtype's own name
            configuration = configurer(AccountSaga.class).start();
            EventConverter converter = configuration.getComponent(EventConverter.class);

            // when
            publish(serialized(new AccountOpened("account-2"), converter));
            publish(serialized(new MoneyDeposited("account-2"), converter));

            // then
            assertThat(AccountSaga.HANDLED).containsExactly(new AccountOpened("account-2"),
                                                            new MoneyDeposited("account-2"));
            assertThat(sagaStore.findSagas(AccountSaga.class, new AssociationValue("accountId", "account-2")))
                    .hasSize(1);
        }
    }

    @Nested
    class FamilyWithANonSealedBranch {

        @Test
        void theApplicationFailsToStartNamingTheBranch() {
            // given
            MessagingConfigurer configurer = configurer(OpenFamilySaga.class);

            // when / then
            assertThatThrownBy(configurer::start)
                    .rootCause()
                    .isInstanceOf(UnsupportedHandlerException.class)
                    .hasMessageContaining(OpenFamilyEvent.class.getName())
                    .hasMessageContaining("[" + OpenBranch.class.getName() + "]")
                    .hasMessageContaining("is neither sealed nor final");
        }
    }

    private MessagingConfigurer configurer(Class<?> sagaType) {
        return MessagingConfigurer
                .create()
                .componentRegistry(cr -> cr.registerComponent(SagaStore.class, c -> sagaStore))
                .eventProcessing(processing -> processing.subscribing(
                        subscribing -> subscribing.defaultProcessor(
                                "saga-processor",
                                components -> components.declarative(sagaType.getSimpleName(),
                                                                     SagaConfigurer.forType(sagaType)))
                ));
    }

    private static EventMessage serialized(AccountEvent event, EventConverter converter) {
        return new GenericEventMessage(new MessageType(event.getClass()), converter.convert(event, byte[].class));
    }

    private void publish(EventMessage event) {
        FutureUtils.joinAndUnwrap(
                configuration.getComponent(EventSink.class).publish(null, List.of(event)), TIMEOUT
        );
    }

    public sealed interface AccountEvent permits AccountOpened, MoneyDeposited {

        String accountId();
    }

    public record AccountOpened(String accountId) implements AccountEvent {

    }

    public record MoneyDeposited(String accountId) implements AccountEvent {

    }

    public sealed interface OpenFamilyEvent permits OpenFamilyOpened, OpenBranch {

        String accountId();
    }

    public record OpenFamilyOpened(String accountId) implements OpenFamilyEvent {

    }

    public non-sealed interface OpenBranch extends OpenFamilyEvent {

    }

    @SuppressWarnings({"unused", "deprecation", "removal"})
    public static class AccountSaga {

        static final List<AccountEvent> HANDLED = new CopyOnWriteArrayList<>();

        @StartSaga
        @SagaEventHandler(associationProperty = "accountId")
        public void on(AccountEvent event) {
            HANDLED.add(event);
        }
    }

    @SuppressWarnings({"unused", "deprecation", "removal"})
    public static class OpenFamilySaga {

        @StartSaga
        @SagaEventHandler(associationProperty = "accountId")
        public void on(OpenFamilyEvent event) {
        }
    }
}
