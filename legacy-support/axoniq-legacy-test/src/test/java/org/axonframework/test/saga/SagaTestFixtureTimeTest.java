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

package org.axonframework.test.saga;

import org.axonframework.deadline.DeadlineManager;
import org.axonframework.deadline.annotation.DeadlineHandler;
import org.axonframework.messaging.commandhandling.gateway.CommandDispatcher;
import org.axonframework.modelling.saga.SagaEventHandler;
import org.axonframework.modelling.saga.StartSaga;
import org.axonframework.test.FixtureExecutionException;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * How the fixture's time moves, and how the deadlines a Saga scheduled fire when it does.
 * <p>
 * The fixture's time is the time of its {@link org.axonframework.test.deadline.StubDeadlineManager}: moving it fires
 * every deadline due by then, in the Saga that scheduled it.
 *
 * @author Mateusz Nowak
 */
@SuppressWarnings("removal")
class SagaTestFixtureTimeTest {

    private static final Instant START = Instant.parse("2026-10-10T10:00:00Z");
    private static final Duration REMINDER_DELAY = Duration.ofMinutes(30);
    private static final String ORDER_ID = "order-1";

    private final SagaTestFixture<ReminderSaga> fixture = new SagaTestFixture<>(ReminderSaga.class);

    @AfterEach
    void tearDown() {
        fixture.stop();
    }

    @Nested
    class CurrentTime {

        @Test
        void deadlinesAreScheduledRelativeToTheGivenCurrentTime() {
            // given / when / then
            fixture.givenCurrentTime(START)
                   .whenPublishingA(new OrderPlaced(ORDER_ID))
                   .expectScheduledDeadline(START.plus(REMINDER_DELAY), new Reminder(ORDER_ID));
        }

        @Test
        void currentTimeFollowsTheTimeTheFixtureWasMovedTo() {
            // given
            fixture.givenCurrentTime(START);

            // when
            fixture.whenTimeElapses(Duration.ofMinutes(5));

            // then
            assertThat(fixture.currentTime()).isEqualTo(START.plus(Duration.ofMinutes(5)));
        }
    }

    @Nested
    class InTheWhenPhase {

        @Test
        void elapsingTimeFiresTheDueDeadlinesAndRecordsWhatTheirHandlersDispatched() {
            // given / when / then
            fixture.givenAPublished(new OrderPlaced(ORDER_ID))
                   .whenTimeElapses(REMINDER_DELAY)
                   .expectActiveSagas(1)
                   .expectDispatchedCommands(new SendReminder(ORDER_ID))
                   .expectTriggeredDeadlinesWithName("remind")
                   .expectNoScheduledDeadlines()
                   .expectSuccessfulHandlerExecution();
        }

        @Test
        void advancingTimeFiresTheDueDeadlinesAndRecordsWhatTheirHandlersDispatched() {
            // given / when / then
            fixture.givenCurrentTime(START)
                   .andThenAPublished(new OrderPlaced(ORDER_ID))
                   .whenTimeAdvancesTo(START.plus(REMINDER_DELAY))
                   .expectDispatchedCommands(new SendReminder(ORDER_ID))
                   .expectTriggeredDeadlines(new Reminder(ORDER_ID));
        }

        @Test
        void aDeadlineThatIsNotDueYetDoesNotFire() {
            // given / when / then
            fixture.givenAPublished(new OrderPlaced(ORDER_ID))
                   .whenTimeElapses(REMINDER_DELAY.minusSeconds(1))
                   .expectNoDispatchedCommands()
                   .expectTriggeredDeadlines()
                   .expectScheduledDeadlineWithName(Duration.ofSeconds(1), "remind");
        }

        @Test
        void aCancelledDeadlineDoesNotFire() {
            // given / when / then
            fixture.givenAPublished(new OrderPlaced(ORDER_ID))
                   .andThenAPublished(new OrderShipped(ORDER_ID))
                   .whenTimeElapses(REMINDER_DELAY)
                   .expectNoDispatchedCommands()
                   .expectTriggeredDeadlines();
        }

        @Test
        void aFailingDeadlineHandlerFailsTheWhenPhaseWithAFixtureExecutionException() {
            // given
            fixture.givenAPublished(new OrderPlaced(ORDER_ID))
                   .andThenAPublished(new ReminderBroken(ORDER_ID));

            // when / then
            assertThatThrownBy(() -> fixture.whenTimeElapses(REMINDER_DELAY))
                    .isInstanceOf(FixtureExecutionException.class)
                    .hasMessage("Exception occurred while trying to advance time and handle scheduled events")
                    .rootCause()
                    .hasMessage("The reminder could not be sent");
        }
    }

    @Nested
    class InTheGivenPhase {

        @Test
        void elapsingTimeFiresTheDueDeadlinesWithoutRecordingWhatTheirHandlersDispatched() {
            // given / when / then
            fixture.givenAPublished(new OrderPlaced(ORDER_ID))
                   .andThenTimeElapses(REMINDER_DELAY)
                   .whenPublishingA(new OrderShipped(ORDER_ID))
                   .expectNoDispatchedCommands()
                   .expectNoScheduledDeadlines();
        }

        @Test
        void advancingTimeFiresTheDueDeadlinesWithoutRecordingWhatTheirHandlersDispatched() {
            // given / when / then
            fixture.givenCurrentTime(START)
                   .andThenAPublished(new OrderPlaced(ORDER_ID))
                   .andThenTimeAdvancesTo(START.plus(REMINDER_DELAY))
                   .whenPublishingA(new OrderShipped(ORDER_ID))
                   .expectNoDispatchedCommands()
                   .expectNoScheduledDeadlines();
        }

        @Test
        void theDeadlinesTriggeredBeforeTheWhenPhaseStillCountAsTriggered() {
            // Axon Framework 4 behaved this way too: the triggered deadlines are never reset between the phases, so
            // a deadline fired in the given phase satisfies a triggered-deadline assertion on the when phase.
            fixture.givenAPublished(new OrderPlaced(ORDER_ID))
                   .andThenTimeElapses(REMINDER_DELAY)
                   .whenPublishingA(new OrderShipped(ORDER_ID))
                   .expectTriggeredDeadlinesWithName("remind");
        }
    }

    @Test
    void aDeadlineScheduledByAHandlerThatFailsAfterwardsStaysScheduled() {
        // Axon Framework 4 behaved this way too: its stub recorded a deadline the moment it was scheduled, rather than
        // when the handler scheduling it completed.
        fixture.givenNoPriorActivity()
               .whenPublishingA(new OrderPlacedThenFailed(ORDER_ID))
               .expectScheduledDeadlineWithName(REMINDER_DELAY, "remind");
    }

    public record OrderPlaced(String orderId) {

    }

    public record OrderPlacedThenFailed(String orderId) {

    }

    public record OrderShipped(String orderId) {

    }

    public record ReminderBroken(String orderId) {

    }

    public record Reminder(String orderId) {

    }

    public record SendReminder(String orderId) {

    }

    public static class ReminderSaga {

        private boolean broken;

        @StartSaga
        @SagaEventHandler(associationProperty = "orderId")
        public void on(OrderPlaced event, DeadlineManager deadlineManager) {
            deadlineManager.schedule(REMINDER_DELAY, "remind", new Reminder(event.orderId()));
        }

        @StartSaga
        @SagaEventHandler(associationProperty = "orderId")
        public void on(OrderPlacedThenFailed event, DeadlineManager deadlineManager) {
            deadlineManager.schedule(REMINDER_DELAY, "remind", new Reminder(event.orderId()));
            throw new IllegalStateException("Failed after scheduling the reminder");
        }

        @SagaEventHandler(associationProperty = "orderId")
        public void on(OrderShipped event, DeadlineManager deadlineManager) {
            deadlineManager.cancelAllWithinScope("remind");
        }

        @SagaEventHandler(associationProperty = "orderId")
        public void on(ReminderBroken event) {
            broken = true;
        }

        @DeadlineHandler(deadlineName = "remind")
        public void remind(Reminder reminder, CommandDispatcher dispatcher) {
            if (broken) {
                throw new IllegalStateException("The reminder could not be sent");
            }
            dispatcher.send(new SendReminder(reminder.orderId()));
        }
    }
}
