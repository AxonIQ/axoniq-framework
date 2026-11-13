/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package org.axonframework.deadline;

import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.eventsourcing.annotation.EventSourcedEntity;
import org.axonframework.eventsourcing.annotation.EventSourcingHandler;
import org.axonframework.eventsourcing.annotation.EventTag;
import org.axonframework.eventsourcing.annotation.reflection.EntityCreator;
import org.axonframework.eventsourcing.configuration.EventSourcedEntityModule;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurer;
import org.axonframework.messaging.commandhandling.annotation.CommandHandler;
import org.axonframework.messaging.commandhandling.gateway.CommandGateway;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.gateway.EventAppender;
import org.axonframework.modelling.annotation.TargetEntityId;
import org.axonframework.modelling.command.AggregateScopeDescriptor;
import org.junit.jupiter.api.*;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves that a fired deadline reaches a real entity's {@code @CommandHandler}, routed purely from the
 * {@link AggregateScopeDescriptor}'s identifier, not from the dispatched command's payload, which carries no
 * identifying field at all here, matching a realistic migrated Axon Framework 4 deadline payload.
 * <p>
 * The test entity below leaves {@code @EventSourcedEntity}'s {@code entityIdResolverDefinition} attribute at its
 * default: resolution works purely because {@link AggregateDeadlineEntityIdResolverConfigurationEnhancer} registers the
 * fallback automatically, merely by {@code axoniq-legacy} being on the test classpath.
 *
 * @author Steven van Beelen
 */
class AggregateDeadlineCommandTranslatorEntityRoutingTest {

    private AxonConfiguration configuration;
    private CommandGateway commandGateway;

    private AggregateDeadlineCommandTranslator testSubject;

    @BeforeEach
    void setUp() {
        EventSourcedEntityModule<String, DeadlineRoutingEntity> entityModule =
                EventSourcedEntityModule.autodetected(String.class, DeadlineRoutingEntity.class);
        EventSourcedEntityModule<String, OtherDeadlineRoutingEntity> otherEntityModule =
                EventSourcedEntityModule.autodetected(String.class, OtherDeadlineRoutingEntity.class);
        configuration = EventSourcingConfigurer.create()
                                               .registerEntity(entityModule)
                                               .registerEntity(otherEntityModule)
                                               .start();

        commandGateway = configuration.getComponent(CommandGateway.class);
        testSubject = new AggregateDeadlineCommandTranslator(commandGateway);
    }

    @AfterEach
    void tearDown() {
        configuration.shutdown();
    }

    @Test
    void firedDeadlinesSharingAPayloadTypeReachTheHandlersNamedAfterTheirDeadlines() {
        // given
        String entityId = "entity-3";
        commandGateway.sendAndWait(new CreateDeadlineRoutingEntity(entityId));
        AggregateScopeDescriptor scope = new AggregateScopeDescriptor("DeadlineRoutingEntity", entityId);
        DeadlineMessage reminder = new GenericDeadlineMessage(
                "reminderDue", new MessageType(SharedDeadlinePayload.class), new SharedDeadlinePayload("first")
        );
        DeadlineMessage escalation = new GenericDeadlineMessage(
                "escalationDue", new MessageType(SharedDeadlinePayload.class), new SharedDeadlinePayload("second")
        );

        // when
        fire(reminder, scope);
        String effectAfterReminder = commandGateway.sendAndWait(new ReadDeadlineEffect(entityId), String.class);
        fire(escalation, scope);
        String effectAfterEscalation = commandGateway.sendAndWait(new ReadDeadlineEffect(entityId), String.class);

        // then
        assertThat(effectAfterReminder).isEqualTo("reminder: first");
        assertThat(effectAfterEscalation).isEqualTo("escalation: second");
    }

    @Test
    void firedDeadlinesOfDifferentAggregateTypesSharingANameReachTheHandlersOfTheirOwnEntityType() {
        // given
        commandGateway.sendAndWait(new CreateDeadlineRoutingEntity("entity-4"));
        commandGateway.sendAndWait(new CreateOtherDeadlineRoutingEntity("other-entity-4"));
        DeadlineMessage reminder = new GenericDeadlineMessage(
                "reminderDue", new MessageType(SharedDeadlinePayload.class), new SharedDeadlinePayload("shared")
        );

        // when
        fire(reminder, new AggregateScopeDescriptor("DeadlineRoutingEntity", "entity-4"));
        fire(reminder, new AggregateScopeDescriptor("OtherDeadlineRoutingEntity", "other-entity-4"));

        // then
        assertThat(commandGateway.sendAndWait(new ReadDeadlineEffect("entity-4"), String.class))
                .isEqualTo("reminder: shared");
        assertThat(commandGateway.sendAndWait(new ReadOtherDeadlineEffect("other-entity-4"), String.class))
                .isEqualTo("other reminder: shared");
    }

    @Test
    void firedDeadlineReachesTheEntityNamedByTheAggregateScopeDescriptor() {
        // given
        String entityId = "entity-1";
        commandGateway.sendAndWait(new CreateDeadlineRoutingEntity(entityId));

        // the entity has no handler for "paymentDue", only one named after the payload type, as migrated with an
        // earlier version of the translator, so the deadline reaches it through the fallback
        ApplyDeadlineEffect command = new ApplyDeadlineEffect("triggered-by-deadline");
        DeadlineMessage deadline = new GenericDeadlineMessage(
                "paymentDue", new MessageType(ApplyDeadlineEffect.class), command
        );
        AggregateScopeDescriptor scope = new AggregateScopeDescriptor("DeadlineRoutingEntity", entityId);

        // when
        UnitOfWorkFactory unitOfWorkFactory = configuration.getComponent(UnitOfWorkFactory.class);
        CompletableFuture<Void> result = unitOfWorkFactory.create().executeWithResult(context -> {
            try {
                testSubject.send(deadline, context, scope);
                return CompletableFuture.completedFuture(null);
            } catch (Exception e) {
                return CompletableFuture.failedFuture(e);
            }
        });
        result.orTimeout(5, TimeUnit.SECONDS).join();

        // then
        String observedEffect = commandGateway.sendAndWait(new ReadDeadlineEffect(entityId), String.class);
        assertThat(observedEffect).isEqualTo("triggered-by-deadline");
    }

    @Test
    void firedPayloadLessDeadlineReachesTheEntityNamedByTheAggregateScopeDescriptor() {
        // given
        String entityId = "entity-2";
        commandGateway.sendAndWait(new CreateDeadlineRoutingEntity(entityId));

        // the entity has no handler for "DeadlineRoutingEntity.accountExpired", only one named after the deadline alone,
        // as migrated with an earlier version of the translator, so the deadline reaches it through the fallback
        DeadlineMessage deadline = new GenericDeadlineMessage("accountExpired", new MessageType(Void.class), null);
        AggregateScopeDescriptor scope = new AggregateScopeDescriptor("DeadlineRoutingEntity", entityId);

        // when
        UnitOfWorkFactory unitOfWorkFactory = configuration.getComponent(UnitOfWorkFactory.class);
        CompletableFuture<Void> result = unitOfWorkFactory.create().executeWithResult(context -> {
            try {
                testSubject.send(deadline, context, scope);
                return CompletableFuture.completedFuture(null);
            } catch (Exception e) {
                return CompletableFuture.failedFuture(e);
            }
        });
        result.orTimeout(5, TimeUnit.SECONDS).join();

        // then
        String observedEffect = commandGateway.sendAndWait(new ReadDeadlineEffect(entityId), String.class);
        assertThat(observedEffect).isEqualTo("no-payload-deadline-handled: accountExpired");
    }

    private void fire(DeadlineMessage deadline, AggregateScopeDescriptor scope) {
        UnitOfWorkFactory unitOfWorkFactory = configuration.getComponent(UnitOfWorkFactory.class);
        CompletableFuture<Void> result = unitOfWorkFactory.create().executeWithResult(context -> {
            try {
                testSubject.send(deadline, context, scope);
                return CompletableFuture.completedFuture(null);
            } catch (Exception e) {
                return CompletableFuture.failedFuture(e);
            }
        });
        result.orTimeout(5, TimeUnit.SECONDS).join();
    }

    @SuppressWarnings("unused")
    @EventSourcedEntity(tagKey = "entityId")
    public static class DeadlineRoutingEntity {

        private String id;
        private String effect;

        @EntityCreator
        DeadlineRoutingEntity() {
        }

        @CommandHandler
        static String handle(CreateDeadlineRoutingEntity command, EventAppender appender) {
            appender.append(new DeadlineRoutingEntityCreated(command.entityId()));
            return command.entityId();
        }

        @CommandHandler
        void handle(ApplyDeadlineEffect command, EventAppender appender) {
            appender.append(new DeadlineEffectApplied(this.id, command.effect()));
        }

        @CommandHandler(commandName = "DeadlineRoutingEntity.reminderDue")
        void handleReminder(SharedDeadlinePayload payload, EventAppender appender) {
            appender.append(new DeadlineEffectApplied(this.id, "reminder: " + payload.note()));
        }

        @CommandHandler(commandName = "DeadlineRoutingEntity.escalationDue")
        void handleEscalation(SharedDeadlinePayload payload, EventAppender appender) {
            appender.append(new DeadlineEffectApplied(this.id, "escalation: " + payload.note()));
        }

        @CommandHandler(commandName = "accountExpired")
        void handle(String command, EventAppender appender) {
            appender.append(new DeadlineEffectApplied(this.id, "no-payload-deadline-handled: " + command));
        }

        @CommandHandler
        String handle(ReadDeadlineEffect command) {
            return effect;
        }

        @EventSourcingHandler
        void on(DeadlineRoutingEntityCreated event) {
            this.id = event.entityId();
        }

        @EventSourcingHandler
        void on(DeadlineEffectApplied event) {
            this.effect = event.effect();
        }
    }

    /**
     * Declares a deadline with the same name as one of {@link DeadlineRoutingEntity}, which only the aggregate type in
     * the command name keeps apart.
     */
    @SuppressWarnings("unused")
    @EventSourcedEntity(tagKey = "otherEntityId")
    public static class OtherDeadlineRoutingEntity {

        private String id;
        private String effect;

        @EntityCreator
        OtherDeadlineRoutingEntity() {
        }

        @CommandHandler
        static String handle(CreateOtherDeadlineRoutingEntity command, EventAppender appender) {
            appender.append(new OtherDeadlineRoutingEntityCreated(command.otherEntityId()));
            return command.otherEntityId();
        }

        @CommandHandler(commandName = "OtherDeadlineRoutingEntity.reminderDue")
        void handleReminder(SharedDeadlinePayload payload, EventAppender appender) {
            appender.append(new OtherDeadlineEffectApplied(this.id, "other reminder: " + payload.note()));
        }

        @CommandHandler
        String handle(ReadOtherDeadlineEffect command) {
            return effect;
        }

        @EventSourcingHandler
        void on(OtherDeadlineRoutingEntityCreated event) {
            this.id = event.otherEntityId();
        }

        @EventSourcingHandler
        void on(OtherDeadlineEffectApplied event) {
            this.effect = event.effect();
        }
    }

    public record CreateDeadlineRoutingEntity(String entityId) {

    }

    public record CreateOtherDeadlineRoutingEntity(String otherEntityId) {

    }

    public record ReadOtherDeadlineEffect(@TargetEntityId String otherEntityId) {

    }

    public record OtherDeadlineRoutingEntityCreated(@EventTag String otherEntityId) {

    }

    public record OtherDeadlineEffectApplied(@EventTag String otherEntityId, String effect) {

    }

    /**
     * Carries no identifying field at all, matching a migrated Axon Framework 4 deadline payload that never needed to
     * identify the aggregate it was scheduled on.
     * <p>
     * The {@link AggregateDeadlineCommandTranslator} must route this purely from the {@link AggregateScopeDescriptor}'s
     * identifier.
     */
    public record ApplyDeadlineEffect(String effect) {

    }

    /**
     * The payload of two different deadlines, which reach different handlers as they are named after their deadlines.
     */
    public record SharedDeadlinePayload(String note) {

    }

    public record ReadDeadlineEffect(@TargetEntityId String entityId) {

    }

    public record DeadlineRoutingEntityCreated(@EventTag String entityId) {

    }

    public record DeadlineEffectApplied(@EventTag String entityId, String effect) {

    }
}
