/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */

package org.axonframework.test.saga;

import jakarta.inject.Inject;
import org.axonframework.messaging.commandhandling.gateway.CommandGateway;
import org.axonframework.messaging.eventhandling.EventBus;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.annotation.Timestamp;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.annotation.MetadataValue;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.modelling.saga.EndSaga;
import org.axonframework.modelling.saga.SagaEventHandler;
import org.axonframework.modelling.saga.SagaLifecycle;
import org.axonframework.modelling.saga.StartSaga;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Stub saga used to test various scenarios of the {@link FixtureConfiguration}.
 *
 * @author Allard Buijze
 */
@SuppressWarnings("unused")
public class StubSaga {

    private static final int TRIGGER_DURATION_MINUTES = 10;
//    @Inject
//    private transient EventScheduler scheduler;
    @Inject
    private NonTransientResource nonTransientResource;
    @Inject
    private transient CommandGateway commandGateway;

    private final List<Object> handledEvents = new ArrayList<>();

//    private ScheduleToken timer;

    @StartSaga
    @SagaEventHandler(associationProperty = "identifier")
    public void handleSagaStart(TriggerSagaStartEvent event,
                                TrackingToken trackingToken,
                                EventMessage message,
                                @MetadataValue("extraIdentifier") Object extraIdentifier) {
        assertNotNull(trackingToken);
        handledEvents.add(event);

        if (extraIdentifier != null) {
            associateWith("extraIdentifier", extraIdentifier.toString());
        }

//        timer = scheduler.schedule(
//                message.timestamp().plus(TRIGGER_DURATION_MINUTES, ChronoUnit.MINUTES),
//                new GenericEventMessage(
//                        new MessageType("event"), new TimerTriggeredEvent(event.getIdentifier())
//                )
//        );
    }

    @StartSaga(forceNew = true)
    @SagaEventHandler(associationProperty = "identifier")
    public void handleForcedSagaStart(ForceTriggerSagaStartEvent event, @Timestamp Instant timestamp) {
        handledEvents.add(event);
//        timer = scheduler.schedule(
//                timestamp.plus(TRIGGER_DURATION_MINUTES, ChronoUnit.MINUTES),
//                new GenericEventMessage(
//                        new MessageType("event"), new TimerTriggeredEvent(event.getIdentifier())
//                )
//        );
    }

    @SagaEventHandler(associationProperty = "identifier")
    public void handleEvent(TriggerExistingSagaEvent event, EventBus eventBus) {
        handledEvents.add(event);
        eventBus.publish(null, new GenericEventMessage(
                new MessageType("event"), new SagaWasTriggeredEvent(this)
        ));
    }

    @SagaEventHandler(associationProperty = "identifier")
    public void handle(ParameterResolvedEvent event, AtomicBoolean assertion, ProcessingContext context) {
        handledEvents.add(event);
        assertFalse(assertion.get());
        assertion.set(true);
        commandGateway.send(new ResolveParameterCommand(event.getIdentifier(), assertion), context);
    }

    @EndSaga
    @SagaEventHandler(associationProperty = "identifier")
    public void handleEndEvent(TriggerSagaEndEvent event) {
        handledEvents.add(event);
    }

    @SagaEventHandler(associationProperty = "identifier")
    public void handleFalseEvent(TriggerExceptionWhileHandlingEvent event) {
        handledEvents.add(event);
        throw new RuntimeException("This is a mock exception");
    }

    @SagaEventHandler(associationProperty = "identifier")
    public void handleTriggerEvent(TimerTriggeredEvent event, ProcessingContext context) {
        handledEvents.add(event);
        String result = commandGateway.send("Say hi!", String.class, context).join();
        if (result != null) {
            commandGateway.send(result, context);
        }
    }

    @SagaEventHandler(associationProperty = "identifier")
    public void handleResetTriggerEvent(ResetTriggerEvent event) {
        handledEvents.add(event);
//        scheduler.cancelSchedule(timer);
//        timer = scheduler.schedule(
//                Duration.ofMinutes(TRIGGER_DURATION_MINUTES),
//                new GenericEventMessage(
//                        new MessageType("event"), new TimerTriggeredEvent(event.getIdentifier())
//                )
//        );
    }

    @SagaEventHandler(associationProperty = "identifier", associationResolver = AssociationResolverStub.class)
    public void handleTriggerAssociationResolverSagaEvent(TriggerAssociationResolverSagaEvent event) {
        handledEvents.add(event);
    }

//    public EventScheduler getScheduler() {
//        return scheduler;
//    }

    public void associateWith(String key, String value) {
        SagaLifecycle.associateWith(key, value);
    }

    public void removeAssociationWith(String key, String value) {
        SagaLifecycle.removeAssociationWith(key, value);
    }

    public void end() {
        SagaLifecycle.end();
    }
}
