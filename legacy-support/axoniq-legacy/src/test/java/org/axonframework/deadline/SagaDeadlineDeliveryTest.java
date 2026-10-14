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

package org.axonframework.deadline;

import org.axonframework.common.FutureUtils;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.config.SagaConfigurer;
import org.axonframework.deadline.annotation.DeadlineHandler;
import org.axonframework.messaging.ScopeAwareProvider;
import org.axonframework.messaging.ScopeDescriptor;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.annotation.MetadataValue;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventSink;
import org.axonframework.modelling.saga.AssociationValue;
import org.axonframework.modelling.saga.EndSaga;
import org.axonframework.modelling.saga.SagaEventHandler;
import org.axonframework.modelling.saga.SagaScopeDescriptor;
import org.axonframework.modelling.saga.StartSaga;
import org.axonframework.modelling.saga.repository.SagaStore;
import org.axonframework.modelling.saga.repository.inmemory.InMemorySagaStore;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.axonframework.messaging.eventhandling.EventTestUtils.asEventMessage;

/**
 * Shows that a deadline a Saga schedules reaches that Saga's {@link DeadlineHandler @DeadlineHandler}, with no other
 * wiring than a {@link DeadlineManager} built on the configuration's {@link ScopeAwareProvider}: the Saga manager
 * registers itself with that provider.
 */
class SagaDeadlineDeliveryTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(5);
    private static final Duration REMINDER_DELAY = Duration.ofMillis(100);

    private final InMemorySagaStore sagaStore = new InMemorySagaStore();

    private @Nullable AxonConfiguration configuration;

    @BeforeEach
    void setUp() {
        PaymentSaga.REMINDERS.clear();
        PaymentSaga.DELIVERY_CHECKS.clear();
    }

    @AfterEach
    void tearDown() {
        if (configuration != null) {
            configuration.shutdown();
        }
    }

    @Nested
    class ToTheSagaThatScheduledIt {

        @Test
        void theDeadlineHandlerRunsOnTheSagaThatScheduledTheDeadline() {
            // given
            start();

            // when
            publish(new OrderPlaced("order-1", 1));
            publish(new OrderPlaced("order-2", 1));

            // then
            await().atMost(TIMEOUT).untilAsserted(() -> assertThat(PaymentSaga.REMINDERS).hasSize(2));
            assertThat(PaymentSaga.REMINDERS).containsExactlyInAnyOrder(
                    new Reminder("order-1", sagaScopeOf("order-1")),
                    new Reminder("order-2", sagaScopeOf("order-2"))
            );
        }

        @Test
        void theSagaKeepsTheStateItsDeadlineHandlerChanged() {
            // given
            start();

            // when
            publish(new OrderPlaced("order-1", 1));

            // then
            await().atMost(TIMEOUT).untilAsserted(() -> assertThat(PaymentSaga.REMINDERS).hasSize(1));
            await().atMost(TIMEOUT).untilAsserted(() -> assertThat(sagaOf("order-1").remindersSent).isEqualTo(1));
        }

        @Test
        void aDeadlineHandlerSchedulesTheNextDeadline() {
            // given
            start();

            // when
            publish(new OrderPlaced("order-1", 3));

            // then
            await().atMost(TIMEOUT).untilAsserted(() -> assertThat(PaymentSaga.REMINDERS).hasSize(3));
            assertThat(PaymentSaga.REMINDERS).extracting(Reminder::orderId).containsOnly("order-1");
        }

        @Test
        void aDeadlineHandlerCancelsAPendingDeadline() {
            // given: the first reminder is due well after the payment is confirmed
            start();
            publish(new OrderPlaced("order-1", 1, REMINDER_DELAY.multipliedBy(5)));

            // when
            publish(new PaymentReceived("order-1"));

            // then
            await().atMost(TIMEOUT).untilAsserted(() -> assertThat(sagaOf("order-1").paid).isTrue());
            await().during(REMINDER_DELAY.multipliedBy(8))
                   .atMost(TIMEOUT)
                   .untilAsserted(() -> assertThat(PaymentSaga.REMINDERS).isEmpty());
        }

        @Test
        void theDeadlineHandlerResolvesTheMetadataTheDeadlineWasScheduledWith() {
            // given
            start();
            publish(new OrderPlaced("order-1", 1));

            // when
            publish(new OrderShipped("order-1", "parcel-service"));

            // then
            await().atMost(TIMEOUT).untilAsserted(() -> assertThat(PaymentSaga.DELIVERY_CHECKS)
                    .containsExactly(new DeliveryCheck("order-1", "parcel-service")));
        }
    }

    @Nested
    class EndingTheSaga {

        @Test
        void aDeadlineHandlerEndsTheSaga() {
            // given
            start();
            publish(new OrderPlaced("order-1", 1));
            String sagaId = sagaIdOf("order-1");

            // when
            publish(new PaymentOverdue("order-1"));

            // then
            await().atMost(TIMEOUT).untilAsserted(() -> assertThat(sagaStore.loadSaga(PaymentSaga.class, sagaId))
                    .isNull());
        }

        @Test
        void aDeadlineOfAnEndedSagaIsNotDelivered() {
            // given
            start();
            publish(new OrderPlaced("order-1", 1));
            String sagaId = sagaIdOf("order-1");

            // when
            publish(new OrderCancelled("order-1"));

            // then
            assertThat(sagaStore.loadSaga(PaymentSaga.class, sagaId)).isNull();
            await().during(REMINDER_DELAY.multipliedBy(3))
                   .atMost(TIMEOUT)
                   .untilAsserted(() -> assertThat(PaymentSaga.REMINDERS).isEmpty());
        }
    }

    /**
     * A persistent deadline manager can fire an overdue deadline before the configuration has started the event
     * processors that build the Saga managers. The configuration's {@link ScopeAwareProvider} waits for them.
     */
    @Test
    void aDeadlineFiringBeforeTheApplicationStartedReachesTheSagaOnceItHas() {
        // given
        PaymentSaga saga = new PaymentSaga();
        saga.orderId = "order-1";
        saga.remindersToSend = 1;
        sagaStore.insertSaga(PaymentSaga.class, "saga-1", saga, Set.of(new AssociationValue("orderId", "order-1")));
        configuration = configurer().build();
        DeadlineManager deadlineManager = configuration.getComponent(DeadlineManager.class);
        SagaScopeDescriptor sagaScope = new SagaScopeDescriptor("PaymentSaga", "saga-1");
        deadlineManager.schedule(Duration.ZERO, "paymentReminder", "order-1", sagaScope);
        await().during(REMINDER_DELAY).atMost(TIMEOUT).until(PaymentSaga.REMINDERS::isEmpty);

        // when
        configuration.start();

        // then
        await().atMost(TIMEOUT)
               .untilAsserted(() -> assertThat(PaymentSaga.REMINDERS).containsExactly(new Reminder("order-1",
                                                                                                    sagaScope)));
    }

    private void start() {
        configuration = configurer().start();
    }

    private MessagingConfigurer configurer() {
        return MessagingConfigurer
                .create()
                .componentRegistry(cr -> cr.registerComponent(SagaStore.class, c -> sagaStore)
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
                                components -> components.declarative("PaymentSaga",
                                                                     SagaConfigurer.forType(PaymentSaga.class)))
                ));
    }

    private void publish(Object event) {
        EventMessage eventMessage = asEventMessage(event);
        FutureUtils.joinAndUnwrap(
                configuration.getComponent(EventSink.class).publish(null, List.of(eventMessage)), TIMEOUT
        );
    }

    private String sagaIdOf(String orderId) {
        Set<String> sagaIds = sagaStore.findSagas(PaymentSaga.class, new AssociationValue("orderId", orderId));
        assertThat(sagaIds).hasSize(1);
        return sagaIds.iterator().next();
    }

    private SagaScopeDescriptor sagaScopeOf(String orderId) {
        return new SagaScopeDescriptor("PaymentSaga", sagaIdOf(orderId));
    }

    private PaymentSaga sagaOf(String orderId) {
        SagaStore.Entry<PaymentSaga> entry = sagaStore.loadSaga(PaymentSaga.class, sagaIdOf(orderId));
        assertThat(entry).isNotNull();
        return entry.saga();
    }

    public record OrderPlaced(String orderId, int reminders, Duration firstReminderAfter) {

        public OrderPlaced(String orderId, int reminders) {
            this(orderId, reminders, REMINDER_DELAY);
        }
    }

    public record PaymentOverdue(String orderId) {

    }

    public record OrderCancelled(String orderId) {

    }

    public record PaymentReceived(String orderId) {

    }

    public record OrderShipped(String orderId, String carrier) {

    }

    public record DeliveryCheck(String orderId, String carrier) {

    }

    public record Reminder(String orderId, ScopeDescriptor scope) {

    }

    @SuppressWarnings({"unused", "deprecation", "removal"})
    public static class PaymentSaga {

        static final List<Reminder> REMINDERS = new CopyOnWriteArrayList<>();
        static final List<DeliveryCheck> DELIVERY_CHECKS = new CopyOnWriteArrayList<>();

        private String orderId;
        private int remindersToSend;
        private int remindersSent;
        private boolean paid;

        @StartSaga
        @SagaEventHandler(associationProperty = "orderId")
        public void on(OrderPlaced event, DeadlineManager deadlineManager) {
            orderId = event.orderId();
            remindersToSend = event.reminders();
            deadlineManager.schedule(event.firstReminderAfter(), "paymentReminder", orderId);
        }

        @DeadlineHandler(deadlineName = "paymentReminder")
        public void onReminder(String orderId, ScopeDescriptor scope, DeadlineManager deadlineManager) {
            REMINDERS.add(new Reminder(orderId, scope));
            remindersSent++;
            if (remindersSent < remindersToSend) {
                deadlineManager.schedule(REMINDER_DELAY, "paymentReminder", orderId);
            }
        }

        @SagaEventHandler(associationProperty = "orderId")
        public void on(PaymentReceived event, DeadlineManager deadlineManager) {
            deadlineManager.schedule(Duration.ZERO, "paymentConfirmed", event.orderId());
        }

        @DeadlineHandler(deadlineName = "paymentConfirmed")
        public void onPaymentConfirmed(String orderId, DeadlineManager deadlineManager) {
            paid = true;
            deadlineManager.cancelAllWithinScope("paymentReminder");
        }

        @SagaEventHandler(associationProperty = "orderId")
        public void on(OrderShipped event, DeadlineManager deadlineManager) {
            deadlineManager.schedule(Duration.ZERO, "deliveryCheck", new GenericMessage(
                    new MessageType(String.class), orderId, Map.of("carrier", event.carrier())
            ));
        }

        @DeadlineHandler(deadlineName = "deliveryCheck")
        public void onDeliveryCheck(String orderId, @MetadataValue("carrier") String carrier) {
            DELIVERY_CHECKS.add(new DeliveryCheck(orderId, carrier));
        }

        @SagaEventHandler(associationProperty = "orderId")
        public void on(PaymentOverdue event, DeadlineManager deadlineManager) {
            deadlineManager.schedule(Duration.ZERO, "paymentOverdue");
        }

        @EndSaga
        @DeadlineHandler(deadlineName = "paymentOverdue")
        public void onOverdue() {
            // the Saga ends with its overdue payment
        }

        @EndSaga
        @SagaEventHandler(associationProperty = "orderId")
        public void on(OrderCancelled event) {
            // the Saga ends without a reminder
        }
    }
}
