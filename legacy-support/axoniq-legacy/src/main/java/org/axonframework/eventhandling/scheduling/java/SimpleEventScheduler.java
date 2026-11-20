/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package org.axonframework.eventhandling.scheduling.java;

import io.axoniq.framework.legacy.LegacySupportAxoniqAddon;
import io.axoniq.license.entitlement.EntitlementManager;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.common.AxonThreadFactory;
import org.axonframework.common.ClockUtils;
import org.axonframework.common.FutureUtils;
import org.axonframework.common.IdentifierFactory;
import org.axonframework.eventhandling.scheduling.EventScheduler;
import org.axonframework.eventhandling.scheduling.ScheduleToken;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.annotation.AnnotationMessageTypeResolver;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventSink;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.axonframework.common.BuilderUtils.assertNonNull;

/**
 * An {@link EventScheduler} implementation that uses Java's {@link ScheduledExecutorService} as scheduling and
 * triggering mechanism.
 * <p>
 * Note that this mechanism is non-persistent. Scheduled tasks will be lost when the JVM is shut down, unless special
 * measures have been taken to prevent that.
 * <p>
 * A fired event is published on the configured {@link EventSink} in a unit of work from the configured
 * {@link UnitOfWorkFactory}. The {@link MessageType} of a fired event is the one of the scheduled
 * {@link EventMessage}, or resolved from the scheduled payload through the configured {@link MessageTypeResolver}. A
 * failing publication is logged, as the event cannot be retried.
 * <p>
 * Register it as the {@link EventScheduler} component of the configuration. A Saga handler's {@code EventScheduler}
 * parameter then resolves to it, and the configuration shuts it down when the application stops:
 * <pre>{@code
 * MessagingConfigurer.create().componentRegistry(registry -> registry.registerComponent(
 *         EventScheduler.class,
 *         config -> SimpleEventScheduler.builder()
 *                                       .eventSink(config.getComponent(EventSink.class))
 *                                       .unitOfWorkFactory(config.getComponent(UnitOfWorkFactory.class))
 *                                       .messageTypeResolver(config.getComponent(MessageTypeResolver.class))
 *                                       .build()
 * ));
 * }</pre>
 *
 * @author Allard Buijze
 * @author Jakob Hatzl
 * @since 0.7
 */
public class SimpleEventScheduler implements EventScheduler {

    private static final Logger logger = LoggerFactory.getLogger(SimpleEventScheduler.class);
    private static final String THREAD_FACTORY_GROUP_NAME = "eventScheduler";

    private final ScheduledExecutorService scheduledExecutorService;
    private final EventSink eventSink;
    private final UnitOfWorkFactory unitOfWorkFactory;
    private final MessageTypeResolver messageTypeResolver;

    private final Map<String, Future<?>> tokens = new ConcurrentHashMap<>();

    /**
     * Instantiate a {@code SimpleEventScheduler} based on the fields contained in the {@link Builder}.
     * <p>
     * Will assert that the {@link EventSink} and the {@link UnitOfWorkFactory} are not {@code null}, and will throw an
     * {@link AxonConfigurationException} if either of them is {@code null}.
     *
     * @param builder the {@link Builder} used to instantiate a {@code SimpleEventScheduler} instance
     */
    protected SimpleEventScheduler(Builder builder) {
        builder.validate();
        this.scheduledExecutorService = builder.scheduledExecutorService;
        this.eventSink = builder.eventSink;
        this.unitOfWorkFactory = builder.unitOfWorkFactory;
        this.messageTypeResolver = builder.messageTypeResolver;
        EntitlementManager.INSTANCE.registerAddon(LegacySupportAxoniqAddon.class);
    }

    /**
     * Instantiate a Builder to be able to create a {@code SimpleEventScheduler}.
     * <p>
     * The {@link ScheduledExecutorService} is defaulted to an {@link Executors#newSingleThreadScheduledExecutor()}
     * which contains an {@link AxonThreadFactory}, and the {@link MessageTypeResolver} to an
     * {@link AnnotationMessageTypeResolver}, as in the configuration. The {@link EventSink} and the
     * {@link UnitOfWorkFactory} are <b>hard requirements</b> and as such should be provided.
     *
     * @return a Builder to be able to create a {@code SimpleEventScheduler}
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * {@inheritDoc}
     *
     * @deprecated scheduling new events through an {@code EventScheduler} is supported only to keep Axon Framework 4
     *             code running while migrating. Schedule with a scheduler of choice that publishes through the
     *             {@link org.axonframework.messaging.eventhandling.gateway.EventGateway} instead
     */
    @Override
    @Deprecated(since = "5.5.0")
    public ScheduleToken schedule(Instant triggerDateTime, Object event) {
        Objects.requireNonNull(triggerDateTime, "The trigger date time may not be null.");
        return schedule(Duration.between(ClockUtils.instant(), triggerDateTime), event);
    }

    /**
     * {@inheritDoc}
     *
     * @deprecated scheduling new events through an {@code EventScheduler} is supported only to keep Axon Framework 4
     *             code running while migrating. Schedule with a scheduler of choice that publishes through the
     *             {@link org.axonframework.messaging.eventhandling.gateway.EventGateway} instead
     */
    @Override
    @Deprecated(since = "5.5.0")
    public ScheduleToken schedule(Duration triggerDuration, Object event) {
        Objects.requireNonNull(triggerDuration, "The trigger duration may not be null.");
        Objects.requireNonNull(event, "The event may not be null.");
        String tokenId = IdentifierFactory.getInstance().generateIdentifier();
        Future<?> future = scheduledExecutorService.schedule(new PublishEventTask(event, tokenId),
                                                             triggerDuration.toMillis(),
                                                             TimeUnit.MILLISECONDS);
        tokens.put(tokenId, future);
        if (future.isDone()) {
            // The task already ran and removed its token before it was put, so it would otherwise stay behind.
            tokens.remove(tokenId);
        }
        return new SimpleScheduleToken(tokenId);
    }

    @Override
    public void cancelSchedule(ScheduleToken scheduleToken) {
        if (!(scheduleToken instanceof SimpleScheduleToken simpleScheduleToken)) {
            throw new IllegalArgumentException("The given ScheduleToken was not provided by this scheduler.");
        }
        Future<?> future = tokens.remove(simpleScheduleToken.getTokenId());
        if (future != null) {
            future.cancel(false);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Shuts down the {@link ScheduledExecutorService}, which ignores every further call.
     */
    @Override
    public void shutdown() {
        scheduledExecutorService.shutdown();
    }

    /**
     * Builder class to instantiate a {@link SimpleEventScheduler}.
     * <p>
     * The {@link ScheduledExecutorService} is defaulted to an {@link Executors#newSingleThreadScheduledExecutor()}
     * which contains an {@link AxonThreadFactory}, and the {@link MessageTypeResolver} to an
     * {@link AnnotationMessageTypeResolver}, as in the configuration. The {@link EventSink} and the
     * {@link UnitOfWorkFactory} are <b>hard requirements</b> and as such should be provided.
     */
    public static class Builder {

        private ScheduledExecutorService scheduledExecutorService =
                Executors.newSingleThreadScheduledExecutor(new AxonThreadFactory(THREAD_FACTORY_GROUP_NAME));
        private @Nullable EventSink eventSink;
        private @Nullable UnitOfWorkFactory unitOfWorkFactory;
        private MessageTypeResolver messageTypeResolver = new AnnotationMessageTypeResolver();

        /**
         * Sets the {@link EventSink} used to publish events on to, once the schedule has been met. Pass the
         * {@code EventSink} of the application's configuration, which is its event store when it has one, so that a
         * fired event is stored and handled like any other published event.
         *
         * @param eventSink the {@link EventSink} used to publish events on to, once the schedule has been met
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder eventSink(EventSink eventSink) {
            assertNonNull(eventSink, "EventSink may not be null");
            this.eventSink = eventSink;
            return this;
        }

        /**
         * Sets the {@link ScheduledExecutorService} used for scheduling and triggering events. Defaults to a
         * {@link Executors#newSingleThreadScheduledExecutor()}, containing an {@link AxonThreadFactory}.
         *
         * @param scheduledExecutorService a {@link ScheduledExecutorService} used for scheduling and triggering
         *                                 events
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder scheduledExecutorService(ScheduledExecutorService scheduledExecutorService) {
            assertNonNull(scheduledExecutorService, "ScheduledExecutorService may not be null");
            this.scheduledExecutorService = scheduledExecutorService;
            return this;
        }

        /**
         * Sets the {@link UnitOfWorkFactory} creating the unit of work a fired event is published in. Pass the factory
         * of the application's configuration, so that a fired event is published in the same kind of unit of work as
         * other messages: transactional if the factory is, and with a
         * {@link org.axonframework.messaging.core.unitofwork.ProcessingContext} that resolves components.
         *
         * @param unitOfWorkFactory the factory creating the unit of work a fired event is published in
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder unitOfWorkFactory(UnitOfWorkFactory unitOfWorkFactory) {
            assertNonNull(unitOfWorkFactory, "UnitOfWorkFactory may not be null");
            this.unitOfWorkFactory = unitOfWorkFactory;
            return this;
        }

        /**
         * Sets the {@link MessageTypeResolver} resolving the {@link MessageType} of a fired event from its payload,
         * when the scheduled event is no {@link EventMessage}. Pass the resolver of the application's configuration,
         * so that a fired event gets the same type as when it is published through the
         * {@link org.axonframework.messaging.eventhandling.gateway.EventGateway}. Defaults to an
         * {@link AnnotationMessageTypeResolver}, the configuration's default, which takes the name of an
         * {@link org.axonframework.messaging.eventhandling.annotation.Event @Event} payload class and the class name
         * otherwise.
         *
         * @param messageTypeResolver the {@link MessageTypeResolver} resolving the type of a fired event
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder messageTypeResolver(MessageTypeResolver messageTypeResolver) {
            assertNonNull(messageTypeResolver, "MessageTypeResolver may not be null");
            this.messageTypeResolver = messageTypeResolver;
            return this;
        }

        /**
         * Initializes a {@link SimpleEventScheduler} as specified through this Builder.
         *
         * @return a {@link SimpleEventScheduler} as specified through this Builder
         */
        public SimpleEventScheduler build() {
            return new SimpleEventScheduler(this);
        }

        /**
         * Validates whether the fields contained in this Builder are set accordingly.
         *
         * @throws AxonConfigurationException if one field is asserted to be incorrect according to the Builder's
         *                                    specifications
         */
        protected void validate() throws AxonConfigurationException {
            assertNonNull(eventSink, "The EventSink is a hard requirement and should be provided");
            assertNonNull(unitOfWorkFactory, "The UnitOfWorkFactory is a hard requirement and should be provided");
        }
    }

    private class PublishEventTask implements Runnable {

        private final Object event;
        private final String tokenId;

        private PublishEventTask(Object event, String tokenId) {
            this.event = event;
            this.tokenId = tokenId;
        }

        @Override
        public void run() {
            try {
                EventMessage eventMessage = createMessage();
                if (logger.isDebugEnabled()) {
                    logger.debug("Triggered the publication of event [{}]", eventMessage.type());
                }
                FutureUtils.joinAndUnwrap(
                        unitOfWorkFactory.create()
                                         .executeWithResult(context -> eventSink.publish(context,
                                                                                         List.of(eventMessage)))
                );
            } catch (Exception e) {
                logger.error("An error occurred while publishing the scheduled event with token [{}]", tokenId, e);
            } finally {
                tokens.remove(tokenId);
            }
        }

        /**
         * Creates a new message for the scheduled event. This ensures that a new identifier and timestamp will always
         * be generated, so that the timestamp will reflect the actual moment the trigger occurred.
         *
         * @return the message to publish
         */
        private EventMessage createMessage() {
            if (event instanceof EventMessage eventMessage) {
                return new GenericEventMessage(eventMessage.type(), eventMessage.payload(), eventMessage.metadata());
            }
            return new GenericEventMessage(messageTypeResolver.resolveOrThrow(event), event);
        }
    }
}
