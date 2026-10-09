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

import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.eventsourcing.annotation.EventSourcedEntity;
import org.axonframework.eventsourcing.annotation.EventSourcingHandler;
import org.axonframework.eventsourcing.annotation.EventTag;
import org.axonframework.eventsourcing.annotation.reflection.EntityCreator;
import org.axonframework.eventsourcing.annotation.reflection.InjectEntityId;
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

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves that a fired deadline reaches an entity whose identifier is not a {@link String}, for identifier types the
 * default converter cannot convert ({@link UUID} and a value object) as well as one it can ({@link Long}).
 *
 * @author Mateusz Nowak
 */
class AggregateDeadlineCommandTranslatorIdentifierTypeRoutingTest {

    private AxonConfiguration configuration;

    @AfterEach
    void tearDown() {
        configuration.shutdown();
    }

    @Test
    void firedDeadlineReachesUuidIdentifiedEntityWithNoArgumentCreator() {
        // given
        CommandGateway commandGateway = start(UUID.class, UuidIdentifiedEntity.class);
        UUID entityId = UUID.randomUUID();
        commandGateway.sendAndWait(new CreateUuidIdentifiedEntity(entityId));

        // when
        fireDeadline(commandGateway, entityId);

        // then
        String observedEffect = commandGateway.sendAndWait(new ReadUuidIdentifiedEntity(entityId), String.class);
        assertThat(observedEffect).isEqualTo("triggered-by-deadline");
    }

    @Test
    void firedDeadlineReachesValueObjectIdentifiedEntityWithNoArgumentCreator() {
        // given
        CommandGateway commandGateway = start(OrderId.class, OrderIdentifiedEntity.class);
        OrderId entityId = new OrderId("order-1");
        commandGateway.sendAndWait(new CreateOrderIdentifiedEntity(entityId));

        // when
        fireDeadline(commandGateway, entityId);

        // then
        String observedEffect = commandGateway.sendAndWait(new ReadOrderIdentifiedEntity(entityId), String.class);
        assertThat(observedEffect).isEqualTo("triggered-by-deadline");
    }

    @Test
    void firedDeadlineReachesLongIdentifiedEntityWhoseCreatorInjectsTheIdentifier() {
        // given
        CommandGateway commandGateway = start(Long.class, LongIdentifiedEntity.class);
        Long entityId = 42L;
        commandGateway.sendAndWait(new CreateLongIdentifiedEntity(entityId));

        // when
        fireDeadline(commandGateway, entityId);

        // then
        String observedEffect = commandGateway.sendAndWait(new ReadLongIdentifiedEntity(entityId), String.class);
        assertThat(observedEffect).isEqualTo("triggered-by-deadline for 42");
    }

    private <ID> CommandGateway start(Class<ID> idType, Class<?> entityType) {
        EventSourcedEntityModule<ID, ?> entityModule = EventSourcedEntityModule.autodetected(idType, entityType);
        configuration = EventSourcingConfigurer.create().registerEntity(entityModule).start();
        return configuration.getComponent(CommandGateway.class);
    }

    private void fireDeadline(CommandGateway commandGateway, Object entityId) {
        AggregateDeadlineCommandTranslator translator = new AggregateDeadlineCommandTranslator(commandGateway);
        ApplyDeadlineEffect payload = new ApplyDeadlineEffect("triggered-by-deadline");
        DeadlineMessage deadline =
                new GenericDeadlineMessage("paymentDue", new MessageType(ApplyDeadlineEffect.class), payload);
        AggregateScopeDescriptor scope = new AggregateScopeDescriptor("Entity", entityId);

        UnitOfWorkFactory unitOfWorkFactory = configuration.getComponent(UnitOfWorkFactory.class);
        unitOfWorkFactory.create().executeWithResult(context -> {
            try {
                translator.send(deadline, context, scope);
                return CompletableFuture.completedFuture(null);
            } catch (Exception e) {
                return CompletableFuture.failedFuture(e);
            }
        }).orTimeout(5, TimeUnit.SECONDS).join();
    }

    @SuppressWarnings("unused")
    @EventSourcedEntity(tagKey = "entityId")
    public static class UuidIdentifiedEntity {

        private UUID id;
        private String effect;

        @EntityCreator
        UuidIdentifiedEntity() {
        }

        @CommandHandler
        static UUID handle(CreateUuidIdentifiedEntity command, EventAppender appender) {
            appender.append(new UuidIdentifiedEntityCreated(command.entityId()));
            return command.entityId();
        }

        @CommandHandler
        void handle(ApplyDeadlineEffect command, EventAppender appender) {
            appender.append(new UuidIdentifiedEffectApplied(this.id, command.effect()));
        }

        @CommandHandler
        String handle(ReadUuidIdentifiedEntity command) {
            return effect;
        }

        @EventSourcingHandler
        void on(UuidIdentifiedEntityCreated event) {
            this.id = event.entityId();
        }

        @EventSourcingHandler
        void on(UuidIdentifiedEffectApplied event) {
            this.effect = event.effect();
        }
    }

    @SuppressWarnings("unused")
    @EventSourcedEntity(tagKey = "entityId")
    public static class OrderIdentifiedEntity {

        private OrderId id;
        private String effect;

        @EntityCreator
        OrderIdentifiedEntity() {
        }

        @CommandHandler
        static OrderId handle(CreateOrderIdentifiedEntity command, EventAppender appender) {
            appender.append(new OrderIdentifiedEntityCreated(command.entityId()));
            return command.entityId();
        }

        @CommandHandler
        void handle(ApplyDeadlineEffect command, EventAppender appender) {
            appender.append(new OrderIdentifiedEffectApplied(this.id, command.effect()));
        }

        @CommandHandler
        String handle(ReadOrderIdentifiedEntity command) {
            return effect;
        }

        @EventSourcingHandler
        void on(OrderIdentifiedEntityCreated event) {
            this.id = event.entityId();
        }

        @EventSourcingHandler
        void on(OrderIdentifiedEffectApplied event) {
            this.effect = event.effect();
        }
    }

    @SuppressWarnings("unused")
    @EventSourcedEntity(tagKey = "entityId")
    public static class LongIdentifiedEntity {

        private final Long id;
        private String effect;

        @EntityCreator
        LongIdentifiedEntity(@InjectEntityId Long id) {
            this.id = id;
        }

        @CommandHandler
        static Long handle(CreateLongIdentifiedEntity command, EventAppender appender) {
            appender.append(new LongIdentifiedEntityCreated(command.entityId()));
            return command.entityId();
        }

        @CommandHandler
        void handle(ApplyDeadlineEffect command, EventAppender appender) {
            appender.append(new LongIdentifiedEffectApplied(this.id, command.effect() + " for " + this.id));
        }

        @CommandHandler
        String handle(ReadLongIdentifiedEntity command) {
            return effect;
        }

        @EventSourcingHandler
        void on(LongIdentifiedEffectApplied event) {
            this.effect = event.effect();
        }
    }

    /**
     * Carries no identifying field, matching a migrated Axon Framework 4 deadline payload.
     */
    public record ApplyDeadlineEffect(String effect) {

    }

    public record CreateUuidIdentifiedEntity(@TargetEntityId UUID entityId) {

    }

    public record ReadUuidIdentifiedEntity(@TargetEntityId UUID entityId) {

    }

    public record UuidIdentifiedEntityCreated(@EventTag UUID entityId) {

    }

    public record UuidIdentifiedEffectApplied(@EventTag UUID entityId, String effect) {

    }

    public record OrderId(String value) {

    }

    public record CreateOrderIdentifiedEntity(@TargetEntityId OrderId entityId) {

    }

    public record ReadOrderIdentifiedEntity(@TargetEntityId OrderId entityId) {

    }

    public record OrderIdentifiedEntityCreated(@EventTag OrderId entityId) {

    }

    public record OrderIdentifiedEffectApplied(@EventTag OrderId entityId, String effect) {

    }

    public record CreateLongIdentifiedEntity(@TargetEntityId Long entityId) {

    }

    public record ReadLongIdentifiedEntity(@TargetEntityId Long entityId) {

    }

    public record LongIdentifiedEntityCreated(@EventTag Long entityId) {

    }

    public record LongIdentifiedEffectApplied(@EventTag Long entityId, String effect) {

    }
}
