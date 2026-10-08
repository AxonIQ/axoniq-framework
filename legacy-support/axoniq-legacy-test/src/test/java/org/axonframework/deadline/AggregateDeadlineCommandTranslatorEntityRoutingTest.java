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
        configuration = EventSourcingConfigurer.create().registerEntity(entityModule).start();

        commandGateway = configuration.getComponent(CommandGateway.class);
        testSubject = new AggregateDeadlineCommandTranslator(commandGateway);
    }

    @AfterEach
    void tearDown() {
        configuration.shutdown();
    }

    @Test
    void firedDeadlineReachesTheEntityNamedByTheAggregateScopeDescriptor() {
        // given
        String entityId = "entity-1";
        commandGateway.sendAndWait(new CreateDeadlineRoutingEntity(entityId));

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

    public record CreateDeadlineRoutingEntity(String entityId) {

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

    public record ReadDeadlineEffect(@TargetEntityId String entityId) {

    }

    public record DeadlineRoutingEntityCreated(@EventTag String entityId) {

    }

    public record DeadlineEffectApplied(@EventTag String entityId, String effect) {

    }
}
