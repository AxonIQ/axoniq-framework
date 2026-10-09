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
import org.axonframework.messaging.eventhandling.annotation.Timestamp;
import org.axonframework.modelling.saga.EndSaga;
import org.axonframework.modelling.saga.SagaEventHandler;
import org.axonframework.modelling.saga.StartSaga;
import org.axonframework.test.AxonAssertionError;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.axonframework.test.matchers.Matchers.*;

/**
 * Test class intended to validate all methods in regards to scheduling and validating deadlines.
 * <p>
 * Moved from Axon Framework 4 with four changes. The Axon Framework 4 scenarios also asserted
 * {@code expectNoScheduledEvents()}, which is dropped because the event scheduler is not ported into
 * {@code axoniq-legacy}. The interceptors are written against the Axon Framework 5 interceptor signatures. The failure
 * checks use AssertJ rather than JUnit assertions, which this repository does not allow. And the fixture is stopped
 * after each test, as it runs a started configuration.
 *
 * @author Milan Savic
 * @author Steven van Beelen
 */
@SuppressWarnings("removal")
class FixtureDeadlinesTest {

    private static final String AGGREGATE_ID = "id";
    private static final TriggerSagaStartEvent START_SAGA_EVENT = new TriggerSagaStartEvent(AGGREGATE_ID);
    private static final int TRIGGER_DURATION_MINUTES = 10;
    private static final String DEADLINE_NAME = "deadlineName";
    private static final String DEADLINE_PAYLOAD = "deadlineDetails";
    private static final String NONE_OCCURRING_DEADLINE_PAYLOAD = "none-occurring-deadline";

    private SagaTestFixture<MySaga> fixture;

    @BeforeEach
    void setUp() {
        fixture = new SagaTestFixture<>(MySaga.class);
    }

    @AfterEach
    void tearDown() {
        fixture.stop();
    }

    @Test
    void expectScheduledDeadline() {
        fixture.givenNoPriorActivity()
               .whenAggregate(AGGREGATE_ID)
               .publishes(START_SAGA_EVENT)
               .expectActiveSagas(1)
               .expectScheduledDeadline(Duration.ofMinutes(TRIGGER_DURATION_MINUTES), DEADLINE_PAYLOAD);
    }

    @Test
    void expectScheduledDeadlineOfType() {
        fixture.givenNoPriorActivity()
               .whenAggregate(AGGREGATE_ID)
               .publishes(START_SAGA_EVENT)
               .expectActiveSagas(1)
               .expectScheduledDeadlineOfType(Duration.ofMinutes(TRIGGER_DURATION_MINUTES), String.class);
    }

    @Test
    void expectScheduledDeadlineWithName() {
        fixture.givenAggregate(AGGREGATE_ID)
               .published(START_SAGA_EVENT)
               .whenAggregate(AGGREGATE_ID)
               .publishes(new PayloadlessDeadlineShouldBeSetEvent(AGGREGATE_ID))
               .expectScheduledDeadlineWithName(Duration.ofMinutes(TRIGGER_DURATION_MINUTES), "payloadless-deadline");
    }

    @Test
    void expectNoScheduledDeadline() {
        fixture.givenAggregate(AGGREGATE_ID)
               .published(START_SAGA_EVENT)
               .whenPublishingA(new ResetTriggerEvent(AGGREGATE_ID))
               .expectActiveSagas(1)
               .expectNoScheduledDeadline(Duration.ofMinutes(TRIGGER_DURATION_MINUTES), DEADLINE_PAYLOAD);
    }

    @Test
    void expectNoScheduledDeadlineOfType() {
        fixture.givenAggregate(AGGREGATE_ID)
               .published(START_SAGA_EVENT)
               .whenPublishingA(new ResetTriggerEvent(AGGREGATE_ID))
               .expectActiveSagas(1)
               .expectNoScheduledDeadlineOfType(Duration.ofMinutes(TRIGGER_DURATION_MINUTES), String.class);
    }

    @Test
    void expectNoScheduledDeadlineWithName() {
        fixture.givenAggregate(AGGREGATE_ID)
               .published(START_SAGA_EVENT)
               .whenPublishingA(new ResetTriggerEvent(AGGREGATE_ID))
               .expectActiveSagas(1)
               .expectNoScheduledDeadlineWithName(Duration.ofMinutes(TRIGGER_DURATION_MINUTES), DEADLINE_NAME);
    }

    @Test
    void deadlineMetMatching() {
        //noinspection deprecation
        fixture.givenAggregate(AGGREGATE_ID)
               .published(START_SAGA_EVENT)
               .whenTimeElapses(Duration.ofMinutes(TRIGGER_DURATION_MINUTES + 1))
               .expectActiveSagas(1)
               .expectDeadlinesMetMatching(payloadsMatching(exactSequenceOf(deepEquals(DEADLINE_PAYLOAD))));
    }

    @Test
    void triggeredDeadlinesMatching() {
        fixture.givenAggregate(AGGREGATE_ID)
               .published(START_SAGA_EVENT)
               .whenTimeElapses(Duration.ofMinutes(TRIGGER_DURATION_MINUTES + 1))
               .expectActiveSagas(1)
               .expectTriggeredDeadlinesMatching(payloadsMatching(exactSequenceOf(deepEquals(DEADLINE_PAYLOAD))));
    }

    @Test
    void deadlineMet() {
        //noinspection deprecation
        fixture.givenAggregate(AGGREGATE_ID)
               .published(START_SAGA_EVENT)
               .whenTimeElapses(Duration.ofMinutes(TRIGGER_DURATION_MINUTES + 1))
               .expectActiveSagas(1)
               .expectDeadlinesMet(DEADLINE_PAYLOAD);
    }

    @Test
    void triggeredDeadlines() {
        fixture.givenAggregate(AGGREGATE_ID)
               .published(START_SAGA_EVENT)
               .whenTimeElapses(Duration.ofMinutes(TRIGGER_DURATION_MINUTES + 1))
               .expectActiveSagas(1)
               .expectTriggeredDeadlines(DEADLINE_PAYLOAD);
    }

    @Test
    void triggeredDeadlinesFailsForIncorrectDeadlines() {
        FixtureExecutionResult given = fixture.givenAggregate(AGGREGATE_ID)
                                              .published(START_SAGA_EVENT)
                                              .whenTimeElapses(Duration.ofMinutes(TRIGGER_DURATION_MINUTES + 1));

        assertThatThrownBy(() -> given.expectTriggeredDeadlines(NONE_OCCURRING_DEADLINE_PAYLOAD))
                .isInstanceOf(AxonAssertionError.class)
                .hasMessageContaining("Expected deadlines were not triggered at the given deadline manager.");
    }

    @Test
    void triggeredDeadlinesFailsForIncorrectNumberOfDeadlines() {
        FixtureExecutionResult given = fixture.givenAggregate(AGGREGATE_ID)
                                              .published(START_SAGA_EVENT)
                                              .whenTimeElapses(Duration.ofMinutes(TRIGGER_DURATION_MINUTES + 1));

        assertThatThrownBy(() -> given.expectTriggeredDeadlines(DEADLINE_PAYLOAD, NONE_OCCURRING_DEADLINE_PAYLOAD))
                .isInstanceOf(AxonAssertionError.class)
                .hasMessageContaining("Got wrong number of triggered deadlines.");
    }

    @Test
    void triggeredDeadlinesWithName() {
        fixture.givenAggregate(AGGREGATE_ID)
               .published(START_SAGA_EVENT)
               .whenTimeElapses(Duration.ofMinutes(TRIGGER_DURATION_MINUTES + 1))
               .expectActiveSagas(1)
               .expectTriggeredDeadlinesWithName(DEADLINE_NAME);
    }

    @Test
    void triggeredDeadlinesWithNameFailsForIncorrectDeadlines() {
        FixtureExecutionResult given = fixture.givenAggregate(AGGREGATE_ID)
                                              .published(START_SAGA_EVENT)
                                              .whenTimeElapses(Duration.ofMinutes(TRIGGER_DURATION_MINUTES + 1));
        assertThatThrownBy(() -> given.expectTriggeredDeadlinesWithName(NONE_OCCURRING_DEADLINE_PAYLOAD))
                .isInstanceOf(AxonAssertionError.class)
                .hasMessageContaining("Expected deadlines were not triggered at the given deadline manager.");
    }

    @Test
    void triggeredDeadlinesWithNameFailsForIncorrectNumberOfDeadlines() {
        FixtureExecutionResult given = fixture.givenAggregate(AGGREGATE_ID)
                                              .published(START_SAGA_EVENT)
                                              .whenTimeElapses(Duration.ofMinutes(TRIGGER_DURATION_MINUTES + 1));

        assertThatThrownBy(() -> given.expectTriggeredDeadlinesWithName(DEADLINE_NAME, NONE_OCCURRING_DEADLINE_PAYLOAD))
                .isInstanceOf(AxonAssertionError.class)
                .hasMessageContaining("Got wrong number of triggered deadlines.");
    }

    @Test
    void triggeredDeadlinesOfType() {
        fixture.givenAggregate(AGGREGATE_ID)
               .published(START_SAGA_EVENT)
               .whenTimeElapses(Duration.ofMinutes(TRIGGER_DURATION_MINUTES + 1))
               .expectActiveSagas(1)
               .expectTriggeredDeadlinesOfType(String.class);
    }

    @Test
    void triggeredDeadlinesOfTypeFailsForIncorrectDeadlines() {
        FixtureExecutionResult given = fixture.givenAggregate(AGGREGATE_ID)
                                              .published(START_SAGA_EVENT)
                                              .whenTimeElapses(Duration.ofMinutes(TRIGGER_DURATION_MINUTES + 1));

        assertThatThrownBy(() -> given.expectTriggeredDeadlinesOfType(Integer.class))
                .isInstanceOf(AxonAssertionError.class)
                .hasMessageContaining("Expected deadlines were not triggered at the given deadline manager.");
    }

    @Test
    void triggeredDeadlinesOfTypeFailsForIncorrectNumberOfDeadlines() {
        FixtureExecutionResult given = fixture.givenAggregate(AGGREGATE_ID)
                                              .published(START_SAGA_EVENT)
                                              .whenTimeElapses(Duration.ofMinutes(TRIGGER_DURATION_MINUTES + 1));

        assertThatThrownBy(() -> given.expectTriggeredDeadlinesOfType(String.class, String.class))
                .isInstanceOf(AxonAssertionError.class)
                .hasMessageContaining("Got wrong number of triggered deadlines.");
    }

    @Test
    void deadlineCancelled() {
        fixture.givenAggregate(AGGREGATE_ID)
               .published(START_SAGA_EVENT)
               .whenPublishingA(new ResetTriggerEvent(AGGREGATE_ID))
               .expectActiveSagas(1)
               .expectNoScheduledDeadlines();
    }

    @Test
    void deadlineWhichCancelsAll() {
        fixture.givenAggregate(AGGREGATE_ID)
               .published(START_SAGA_EVENT)
               .whenPublishingA(new ResetAllTriggeredEvent(AGGREGATE_ID))
               .expectActiveSagas(1)
               .expectNoScheduledDeadlines();
    }

    @Test
    void deadlineHandlerEndsSagaLifecycle() {
        fixture.givenAggregate(AGGREGATE_ID)
               .published(new TriggerSagaStartEvent(AGGREGATE_ID, "sagaEndingDeadline"))
               .whenTimeElapses(Duration.ofMinutes(TRIGGER_DURATION_MINUTES + 1))
               .expectActiveSagas(0);
    }

    private static class ResetAllTriggeredEvent {

        private final String identifier;

        private ResetAllTriggeredEvent(String identifier) {
            this.identifier = identifier;
        }

        public String getIdentifier() {
            return identifier;
        }
    }

    private static class PayloadlessDeadlineShouldBeSetEvent {

        private final String identifier;

        private PayloadlessDeadlineShouldBeSetEvent(String identifier) {
            this.identifier = identifier;
        }

        public String getIdentifier() {
            return identifier;
        }
    }

    @SuppressWarnings("unused")
    public static class MySaga {

        private String deadlineId;
        private String deadlineName;

        @StartSaga
        @SagaEventHandler(associationProperty = "identifier")
        public void on(TriggerSagaStartEvent event, @Timestamp Instant timestamp, DeadlineManager deadlineManager) {
            deadlineName = event.getDeadlineName();
            deadlineId = deadlineManager.schedule(
                    Duration.ofMinutes(TRIGGER_DURATION_MINUTES), deadlineName, DEADLINE_PAYLOAD
            );
        }

        @SagaEventHandler(associationProperty = "identifier")
        public void on(ResetTriggerEvent event, DeadlineManager deadlineManager) {
            deadlineManager.cancelSchedule(deadlineName, deadlineId);
        }

        @SagaEventHandler(associationProperty = "identifier")
        public void on(ResetAllTriggeredEvent event, DeadlineManager deadlineManager) {
            deadlineManager.cancelAll(deadlineName);
        }

        @SagaEventHandler(associationProperty = "identifier")
        public void on(PayloadlessDeadlineShouldBeSetEvent event, DeadlineManager deadlineManager) {
            deadlineManager.schedule(Duration.ofMinutes(TRIGGER_DURATION_MINUTES), "payloadless-deadline");
        }

        @DeadlineHandler
        public void handleDeadline(String deadlineInfo) {
            // Nothing to be done for test purposes, having this deadline handler invoked is sufficient
        }

        @DeadlineHandler(deadlineName = "payloadless-deadline")
        public void handle() {
            // Nothing to be done for test purposes, having this deadline handler invoked is sufficient
        }

        @EndSaga
        @DeadlineHandler(deadlineName = "sagaEndingDeadline")
        public void sagaEndingDeadline() {
        }
    }
}
