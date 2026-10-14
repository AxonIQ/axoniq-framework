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

package org.axonframework.integrationtests.deadline;

import org.axonframework.common.AxonNonTransientException;
import org.axonframework.deadline.DeadlineManager;
import org.axonframework.eventsourcing.annotation.EventSourcedEntity;
import org.axonframework.eventsourcing.annotation.EventSourcingHandler;
import org.axonframework.eventsourcing.annotation.EventTag;
import org.axonframework.eventsourcing.annotation.reflection.EntityCreator;
import org.axonframework.messaging.commandhandling.annotation.CommandHandler;
import org.axonframework.messaging.core.annotation.MetadataValue;
import org.axonframework.messaging.eventhandling.gateway.EventAppender;
import org.axonframework.modelling.annotation.TargetEntityId;
import org.axonframework.modelling.command.AggregateScopeDescriptor;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An event-sourced entity migrated from an Axon Framework 4 aggregate, for the deadline manager test suite.
 * <p>
 * Its command handlers schedule and cancel deadlines for the {@link AggregateScopeDescriptor} of the entity, and a
 * command handler per deadline payload records the fired deadline as an event. A fired deadline reaches these handlers
 * as a command, as the deadline payload becomes the command payload. A deadline without a payload reaches the handler
 * named after the deadline.
 * <p>
 * The deadline payloads carry no {@link TargetEntityId}, as a migrated payload never identified its aggregate. The
 * entity is found through the identifier of the scope the deadline was scheduled for.
 */
@EventSourcedEntity(tagKey = "entityId")
public class DeadlineTestEntity {

    /**
     * The aggregate type of the {@link AggregateScopeDescriptor} the entity schedules its deadlines for.
     */
    public static final String TYPE = "DeadlineTestEntity";

    /**
     * The name of the deadline scheduled without a payload, which is the name of the command handler that receives
     * it.
     */
    public static final String PAYLOADLESS_DEADLINE_NAME = "payloadlessDeadline";

    /**
     * Counts the deliveries that failed in the {@link #on(FailingDeadlinePayload)} handler.
     */
    public static final AtomicInteger FAILED_DELIVERIES = new AtomicInteger();

    private String id;
    private final List<DeadlineOccurred> occurrences = new ArrayList<>();

    @EntityCreator
    DeadlineTestEntity() {
    }

    @CommandHandler
    static void handle(CreateEntity command, EventAppender appender) {
        appender.append(new EntityCreated(command.entityId()));
    }

    @CommandHandler
    String handle(ScheduleDeadline command, DeadlineManager deadlineManager) {
        return deadlineManager.schedule(Duration.ofMillis(command.triggerMillis()),
                                        command.deadlineName(),
                                        command.payload(),
                                        scope());
    }

    @CommandHandler
    void handle(ScheduleDeadlineThenFail command, DeadlineManager deadlineManager) {
        deadlineManager.schedule(Duration.ofMillis(command.triggerMillis()),
                                 command.deadlineName(),
                                 command.payload(),
                                 scope());
        throw new IllegalStateException("The command fails after it scheduled its deadline");
    }

    @CommandHandler
    void handle(ScheduleDeadlineThroughCollaboratorThenFail command, DeadlineCollaborator collaborator) {
        collaborator.schedule(Duration.ofMillis(command.triggerMillis()),
                              command.deadlineName(),
                              command.payload(),
                              scope());
        throw new IllegalStateException("The command fails after its collaborator scheduled its deadline");
    }

    @CommandHandler
    void handle(CancelDeadline command, DeadlineManager deadlineManager) {
        deadlineManager.cancelSchedule(command.deadlineName(), command.scheduleId());
    }

    @CommandHandler
    void handle(CancelAllDeadlines command, DeadlineManager deadlineManager) {
        deadlineManager.cancelAll(command.deadlineName());
    }

    @CommandHandler
    void handle(CancelAllDeadlinesWithinScope command, DeadlineManager deadlineManager) {
        deadlineManager.cancelAllWithinScope(command.deadlineName(), scope());
    }

    @CommandHandler
    Occurrences handle(ReadOccurrences command) {
        return new Occurrences(List.copyOf(occurrences));
    }

    @CommandHandler
    void on(DeadlinePayload payload, @MetadataValue("origin") @Nullable String origin, EventAppender appender) {
        appender.append(new DeadlineOccurred(id, "payload", payload.text(), origin));
    }

    @CommandHandler(commandName = PAYLOADLESS_DEADLINE_NAME)
    void on(String deadlineName, @MetadataValue("origin") @Nullable String origin, EventAppender appender) {
        appender.append(new DeadlineOccurred(id, "payloadless", deadlineName, origin));
    }

    @CommandHandler
    void on(FailingDeadlinePayload payload) {
        FAILED_DELIVERIES.incrementAndGet();
        throw new HandlingFailure();
    }

    @EventSourcingHandler
    void on(EntityCreated event) {
        this.id = event.entityId();
    }

    @EventSourcingHandler
    void on(DeadlineOccurred event) {
        occurrences.add(event);
    }

    private AggregateScopeDescriptor scope() {
        // Axon Framework 5 starts no aggregate scope around a command handler, so the scope is described explicitly.
        return new AggregateScopeDescriptor(TYPE, id);
    }

    public record CreateEntity(@TargetEntityId String entityId) {

    }

    public record ScheduleDeadline(@TargetEntityId String entityId,
                                   String deadlineName,
                                   @Nullable Object payload,
                                   long triggerMillis) {

    }

    public record ScheduleDeadlineThenFail(@TargetEntityId String entityId,
                                           String deadlineName,
                                           @Nullable Object payload,
                                           long triggerMillis) {

    }

    public record ScheduleDeadlineThroughCollaboratorThenFail(@TargetEntityId String entityId,
                                                              String deadlineName,
                                                              @Nullable Object payload,
                                                              long triggerMillis) {

    }

    public record CancelDeadline(@TargetEntityId String entityId, String deadlineName, String scheduleId) {

    }

    public record CancelAllDeadlines(@TargetEntityId String entityId, String deadlineName) {

    }

    public record CancelAllDeadlinesWithinScope(@TargetEntityId String entityId, String deadlineName) {

    }

    public record ReadOccurrences(@TargetEntityId String entityId) {

    }

    public record Occurrences(List<DeadlineOccurred> events) {

    }

    /**
     * A deadline payload without a {@link TargetEntityId}.
     *
     * @param text the text the fired deadline is recorded with
     */
    public record DeadlinePayload(String text) {

    }

    /**
     * A deadline payload whose handler always fails.
     *
     * @param text a text value
     */
    public record FailingDeadlinePayload(String text) {

    }

    public record EntityCreated(@EventTag String entityId) {

    }

    /**
     * A fired deadline, recorded by the entity.
     *
     * @param entityId the identifier of the entity the deadline fired for
     * @param kind     whether the deadline carried a payload
     * @param detail   the text of the payload, or the name of the deadline when it carried none
     * @param origin   the {@code origin} metadata value the deadline's command carried, if any
     */
    public record DeadlineOccurred(@EventTag String entityId,
                                   String kind,
                                   String detail,
                                   @Nullable String origin) {

    }

    /**
     * A component the entity delegates to, holding the configured {@link DeadlineManager} itself instead of receiving
     * it as a handler parameter.
     *
     * @param deadlineManager the configured deadline manager
     */
    public record DeadlineCollaborator(DeadlineManager deadlineManager) {

        void schedule(Duration triggerDuration,
                      String deadlineName,
                      @Nullable Object payload,
                      AggregateScopeDescriptor scope) {
            deadlineManager.schedule(triggerDuration, deadlineName, payload, scope);
        }
    }

    private static final class HandlingFailure extends AxonNonTransientException {

        private HandlingFailure() {
            super("The deadline handler fails");
        }
    }
}
