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

package org.axonframework.modelling.saga;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.test.appender.ListAppender;
import org.axonframework.deadline.DeadlineMessage;
import org.axonframework.deadline.GenericDeadlineMessage;
import org.axonframework.deadline.StoredDeadlineConverter;
import org.axonframework.deadline.UnknownDeadlinePayload;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.modelling.saga.repository.AnnotatedSagaRepository;
import org.axonframework.modelling.saga.repository.inmemory.InMemorySagaStore;
import org.junit.jupiter.api.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test class validating that a saga manager warns about a deadline whose stored payload type resolves to no class, so
 * that such a deadline is not lost unnoticed.
 *
 * @author Jakob Hatzl
 */
class AbstractSagaManagerUnknownDeadlinePayloadTest {

    private static final String DEADLINE_NAME = "paymentReminder";
    private static final SagaScopeDescriptor SCOPE = new SagaScopeDescriptor("ReminderSaga", "saga-1");

    private final AnnotatedSagaManager<ReminderSaga> testSubject =
            AnnotatedSagaManager.<ReminderSaga>builder()
                                .sagaRepository(AnnotatedSagaRepository.<ReminderSaga>builder()
                                                                       .sagaType(ReminderSaga.class)
                                                                       .sagaStore(new InMemorySagaStore())
                                                                       .build())
                                .sagaType(ReminderSaga.class)
                                .sagaFactory(ReminderSaga::new)
                                .build();

    private ListAppender appender;
    private Logger sagaManagerLogger;
    private boolean previousAdditive;

    @BeforeEach
    void attachAppender() {
        appender = new ListAppender("AnnotatedSagaManagerUnknownDeadlinePayloadTestAppender");
        appender.start();
        sagaManagerLogger = (Logger) LogManager.getLogger(AbstractSagaManager.class);
        previousAdditive = sagaManagerLogger.isAdditive();
        sagaManagerLogger.setAdditive(false);
        sagaManagerLogger.addAppender(appender);
    }

    @AfterEach
    void detachAppender() {
        sagaManagerLogger.removeAppender(appender);
        sagaManagerLogger.setAdditive(previousAdditive);
        appender.stop();
    }

    @Test
    void aDeadlineWithAnUnknownPayloadIsSentWithAWarningNamingTheDeadlineTheTypeAndTheScope() throws Exception {
        // given
        DeadlineMessage deadline = deadline(new UnknownDeadlinePayload("com.example.RemovedPayload", null, "{}"));

        // when
        testSubject.send(deadline, StubProcessingContext.forMessage(deadline), SCOPE);

        // then
        assertThat(appender.getEvents())
                .filteredOn(event -> event.getLevel() == Level.WARN)
                .singleElement()
                .satisfies(event -> assertThat(event.getMessage().getFormattedMessage())
                        .contains(DEADLINE_NAME, "com.example.RemovedPayload", SCOPE.scopeDescription()));
    }

    @Test
    void aDeadlineWithAKnownPayloadIsSentWithoutAWarning() throws Exception {
        // given
        DeadlineMessage deadline = deadline("payload");

        // when
        testSubject.send(deadline, StubProcessingContext.forMessage(deadline), SCOPE);

        // then
        assertThat(appender.getEvents()).noneMatch(event -> event.getLevel() == Level.WARN);
    }

    private static DeadlineMessage deadline(Object payload) {
        return new GenericDeadlineMessage(DEADLINE_NAME, StoredDeadlineConverter.messageTypeOf(payload), payload);
    }

    @SuppressWarnings("unused")
    public static class ReminderSaga {

    }
}
