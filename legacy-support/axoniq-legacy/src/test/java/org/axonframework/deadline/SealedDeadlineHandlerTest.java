/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package org.axonframework.deadline;

import org.axonframework.common.FutureUtils;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.config.SagaConfigurer;
import org.axonframework.deadline.annotation.DeadlineHandler;
import org.axonframework.messaging.ScopeAwareProvider;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventSink;
import org.axonframework.modelling.saga.SagaEventHandler;
import org.axonframework.modelling.saga.StartSaga;
import org.axonframework.modelling.saga.repository.SagaStore;
import org.axonframework.modelling.saga.repository.inmemory.InMemorySagaStore;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.axonframework.messaging.eventhandling.EventTestUtils.asEventMessage;

/**
 * Shows that a {@link DeadlineHandler @DeadlineHandler} whose payload parameter is a sealed type receives the deadline
 * of every subtype. Deadline handlers are not subscribed by name: they match the deadline name and the Java type of
 * the payload, so the handler is not split per subtype, and a family with a {@code non-sealed} branch starts as well.
 */
class SealedDeadlineHandlerTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    private @Nullable AxonConfiguration configuration;

    @BeforeEach
    void setUp() {
        ReminderSaga.HANDLED.clear();
        OpenFamilyReminderSaga.HANDLED.clear();
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
        void theDeadlineHandlerReceivesTheDeadlineOfEverySubtype() {
            // given
            configuration = configurer(ReminderSaga.class).start();

            // when
            publish(new RemindersRequested("order-1"));

            // then
            await().atMost(TIMEOUT).untilAsserted(() -> assertThat(ReminderSaga.HANDLED).containsExactlyInAnyOrder(
                    new PaymentDue("order-1"), new PaymentOverdue("order-1")
            ));
        }
    }

    @Nested
    class FamilyWithANonSealedBranch {

        @Test
        void theApplicationStartsAndTheDeadlineHandlerReceivesTheDeadlineOfEverySubtype() {
            // given
            configuration = configurer(OpenFamilyReminderSaga.class).start();

            // when
            publish(new RemindersRequested("order-2"));

            // then
            await().atMost(TIMEOUT).untilAsserted(
                    () -> assertThat(OpenFamilyReminderSaga.HANDLED).containsExactlyInAnyOrder(
                            new OpenFamilyPaymentDue("order-2"), new OpenFamilyPaymentOverdue("order-2")
                    ));
        }
    }

    private static MessagingConfigurer configurer(Class<?> sagaType) {
        return MessagingConfigurer
                .create()
                .componentRegistry(cr -> cr.registerComponent(SagaStore.class, c -> new InMemorySagaStore())
                                           .registerComponent(
                                                   DeadlineManager.class,
                                                   c -> SimpleDeadlineManager
                                                           .builder()
                                                           .scopeAwareProvider(c.getComponent(ScopeAwareProvider.class))
                                                           .unitOfWorkFactory(c.getComponent(UnitOfWorkFactory.class))
                                                           .build()
                                           ))
                .eventProcessing(processing -> processing.subscribing(
                        subscribing -> subscribing.defaultProcessor(
                                "saga-processor",
                                components -> components.declarative(sagaType.getSimpleName(),
                                                                     SagaConfigurer.forType(sagaType)))
                ));
    }

    private void publish(Object event) {
        FutureUtils.joinAndUnwrap(
                configuration.getComponent(EventSink.class).publish(null, List.of(asEventMessage(event))), TIMEOUT
        );
    }

    public record RemindersRequested(String orderId) {

    }

    public sealed interface PaymentReminder permits PaymentDue, PaymentOverdue {

    }

    public record PaymentDue(String orderId) implements PaymentReminder {

    }

    public record PaymentOverdue(String orderId) implements PaymentReminder {

    }

    public sealed interface OpenFamilyReminder permits OpenFamilyPaymentDue, OpenFamilyBranch {

    }

    public record OpenFamilyPaymentDue(String orderId) implements OpenFamilyReminder {

    }

    public non-sealed interface OpenFamilyBranch extends OpenFamilyReminder {

    }

    public record OpenFamilyPaymentOverdue(String orderId) implements OpenFamilyBranch {

    }

    @SuppressWarnings({"unused", "deprecation", "removal"})
    public static class ReminderSaga {

        static final List<PaymentReminder> HANDLED = new CopyOnWriteArrayList<>();

        @StartSaga
        @SagaEventHandler(associationProperty = "orderId")
        public void on(RemindersRequested event, DeadlineManager deadlineManager) {
            deadlineManager.schedule(Duration.ZERO, "paymentDue", new PaymentDue(event.orderId()));
            deadlineManager.schedule(Duration.ZERO, "paymentOverdue", new PaymentOverdue(event.orderId()));
        }

        @DeadlineHandler
        public void on(PaymentReminder reminder) {
            HANDLED.add(reminder);
        }
    }

    @SuppressWarnings({"unused", "deprecation", "removal"})
    public static class OpenFamilyReminderSaga {

        static final List<OpenFamilyReminder> HANDLED = new CopyOnWriteArrayList<>();

        @StartSaga
        @SagaEventHandler(associationProperty = "orderId")
        public void on(RemindersRequested event, DeadlineManager deadlineManager) {
            deadlineManager.schedule(Duration.ZERO, "paymentDue", new OpenFamilyPaymentDue(event.orderId()));
            deadlineManager.schedule(Duration.ZERO, "paymentOverdue", new OpenFamilyPaymentOverdue(event.orderId()));
        }

        @DeadlineHandler
        public void on(OpenFamilyReminder reminder) {
            HANDLED.add(reminder);
        }
    }
}
