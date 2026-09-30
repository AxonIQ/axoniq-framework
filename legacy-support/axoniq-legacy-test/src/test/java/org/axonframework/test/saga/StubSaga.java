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

import org.axonframework.messaging.commandhandling.gateway.CommandGateway;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventSink;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.annotation.Timestamp;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.annotation.MetadataValue;
import org.axonframework.messaging.core.annotation.SourceId;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.modelling.saga.EndSaga;
import org.axonframework.modelling.saga.SagaEventHandler;
import org.axonframework.modelling.saga.SagaLifecycle;
import org.axonframework.modelling.saga.StartSaga;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Stub saga used to test various scenarios of the {@link FixtureConfiguration}.
 * <p>
 * Axon Framework 4 received its collaborators as {@code @Inject} annotated fields, filled in by a
 * {@code ResourceInjector}. Axon Framework 5 dropped field injection, so they arrive as handler method parameters
 * instead, which is why every handler needing one declares it.
 *
 * @author Allard Buijze
 */
@SuppressWarnings({"unused", "removal"})
public class StubSaga {

    private static final int TRIGGER_DURATION_MINUTES = 10;
//    TODO #5006
//    @Inject
//    private transient EventScheduler scheduler;

    private final List<Object> handledEvents = new ArrayList<>();

//    private ScheduleToken timer;

    @StartSaga
    @SagaEventHandler(associationProperty = "identifier")
    public void handleSagaStart(TriggerSagaStartEvent event,
                                SagaLifecycle lifecycle,
                                EventMessage message,
                                @MetadataValue("extraIdentifier") Object extraIdentifier) {
        handledEvents.add(event);

        if (extraIdentifier != null) {
            associateWith(lifecycle, "extraIdentifier", extraIdentifier.toString());
        }

//        TODO #5006
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
//        TODO #5006
//        timer = scheduler.schedule(
//                timestamp.plus(TRIGGER_DURATION_MINUTES, ChronoUnit.MINUTES),
//                new GenericEventMessage(
//                        new MessageType("event"), new TimerTriggeredEvent(event.getIdentifier())
//                )
//        );
    }

    @SagaEventHandler(associationProperty = "identifier")
    public void handleEvent(TriggerExistingSagaEvent event, EventSink eventSink, ProcessingContext context) {
        handledEvents.add(event);
        eventSink.publish(context, new GenericEventMessage(
                new MessageType("event"), new SagaWasTriggeredEvent(this)
        ));
    }

    @SagaEventHandler(associationProperty = "identifier")
    public void handle(ParameterResolvedEvent event,
                       AtomicBoolean assertion,
                       CommandGateway commandGateway,
                       ProcessingContext context) {
        handledEvents.add(event);
        assertThat(assertion.get()).isFalse();
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
    public void handleTriggerEvent(TimerTriggeredEvent event,
                                   CommandGateway commandGateway,
                                   ProcessingContext context) {
        handledEvents.add(event);
        String result = commandGateway.send("Say hi!", String.class, context).join();
        if (result != null) {
            commandGateway.send(result, context);
        }
    }

    @SagaEventHandler(associationProperty = "identifier")
    public void handleResetTriggerEvent(ResetTriggerEvent event) {
        handledEvents.add(event);
//        TODO #5006
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

//    TODO #5006
//    public EventScheduler getScheduler() {
//        return scheduler;
//    }

    public void associateWith(SagaLifecycle lifecycle, String key, String value) {
        lifecycle.associateWith(key, value);
    }

    public void removeAssociationWith(SagaLifecycle lifecycle, String key, String value) {
        lifecycle.removeAssociationWith(key, value);
    }

    public void end(SagaLifecycle lifecycle) {
        lifecycle.end();
    }
}
