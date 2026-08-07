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

package io.axoniq.framework.axonserver.connector.event;

import io.axoniq.axonserver.connector.event.PersistentStreamProperties;
import io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration;
import io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager;
import org.axonframework.common.Registration;
import org.axonframework.common.StringUtils;
import org.axonframework.common.annotation.Internal;
import org.axonframework.eventsourcing.eventstore.EventTypeResolver;
import org.axonframework.messaging.core.SubscribableEventSource;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiFunction;

/**
 * A {@link SubscribableEventSource} that receives events from a persistent stream on Axon Server.
 * <p>
 * The persistent stream is identified by a unique {@code name}, which acts as the stream identifier in Axon Server.
 * Using the same name for different instances will join the same server-side stream. Each instance owns an exclusive
 * {@link PersistentStreamConnection} and may have at most one active subscriber at a time.
 * <p>
 * Marked {@link Internal} as a concrete, internal implementation behind the {@link PersistentStreamEventSourceFactory}.
 *
 * @author Marc Gathier
 * @author Jakob Hatzl
 * @see PersistentStreamEventSourceFactory
 * @since 5.2.0
 */
@Internal
public class PersistentStreamEventSource implements SubscribableEventSource {

    private static final BiFunction<List<? extends EventMessage>, ProcessingContext, CompletableFuture<?>>
            NO_OP_CONSUMER = (events, context) -> CompletableFuture.completedFuture(null);

    private final String name;
    private final PersistentStreamConnection persistentStreamConnection;

    private BiFunction<List<? extends EventMessage>, ProcessingContext, CompletableFuture<?>> consumer =
            NO_OP_CONSUMER;

    /**
     * Instantiates a {@code PersistentStreamEventSource}.
     *
     * @param name                       the name of the persistent stream; acts as the unique stream identifier in Axon
     *                                   Server
     * @param connectionManager          the Axon Server connection manager
     * @param serverConfig               the Axon Server configuration
     * @param converter                  the event converter used to deserialize event payloads
     * @param eventTypeResolver          the event type resolver used to resolve the type on inbound events
     * @param persistentStreamProperties the properties for the persistent stream
     * @param scheduler                  the scheduler thread pool to schedule tasks
     * @param unitOfWorkFactory          the {@link UnitOfWorkFactory} used to create a unit of work to span message
     *                                   processing
     * @param batchSize                  the maximum number of events to collect per batch
     */
    public PersistentStreamEventSource(String name,
                                       AxonServerConnectionManager connectionManager,
                                       AxonServerConfiguration serverConfig,
                                       EventConverter converter,
                                       EventTypeResolver eventTypeResolver,
                                       PersistentStreamProperties persistentStreamProperties,
                                       ScheduledExecutorService scheduler,
                                       UnitOfWorkFactory unitOfWorkFactory,
                                       int batchSize) {
        this(name,
             connectionManager,
             serverConfig,
             converter,
             eventTypeResolver,
             persistentStreamProperties,
             scheduler,
             unitOfWorkFactory,
             PersistentStreamContextCustomizer.NO_OP,
             batchSize,
             null);
    }

    /**
     * Instantiates a {@code PersistentStreamEventSource} placing additional resources on the
     * {@link ProcessingContext} of every batch it delivers through the given {@code contextCustomizer}.
     *
     * @param name                       the name of the persistent stream; acts as the unique stream identifier in Axon
     *                                   Server
     * @param connectionManager          the Axon Server connection manager
     * @param serverConfig               the Axon Server configuration
     * @param converter                  the event converter used to deserialize event payloads
     * @param eventTypeResolver          the event type resolver used to resolve the type on inbound events
     * @param persistentStreamProperties the properties for the persistent stream
     * @param scheduler                  the scheduler thread pool to schedule tasks
     * @param unitOfWorkFactory          the {@link UnitOfWorkFactory} used to create a unit of work to span message
     *                                   processing
     * @param contextCustomizer          the customizer placing resources on the {@link ProcessingContext} of every
     *                                   batch, invoked once per batch before any of its events is consumed, returning
     *                                   the context that batch is consumed with
     * @param batchSize                  the maximum number of events to collect per batch
     * @param context                    the Axon Server context in which this stream exists, or {@code null} to use the
     *                                   context from {@link AxonServerConfiguration#getContext()}
     */
    public PersistentStreamEventSource(String name,
                                       AxonServerConnectionManager connectionManager,
                                       AxonServerConfiguration serverConfig,
                                       EventConverter converter,
                                       EventTypeResolver eventTypeResolver,
                                       PersistentStreamProperties persistentStreamProperties,
                                       ScheduledExecutorService scheduler,
                                       UnitOfWorkFactory unitOfWorkFactory,
                                       PersistentStreamContextCustomizer contextCustomizer,
                                       int batchSize,
                                       @Nullable String context) {
        if(StringUtils.emptyOrNull(name)){
            throw new IllegalArgumentException("name must not be null or empty");
        }
        this.name = name;
        this.persistentStreamConnection = new PersistentStreamConnection(name,
                                                                         connectionManager,
                                                                         serverConfig,
                                                                         converter,
                                                                         eventTypeResolver,
                                                                         persistentStreamProperties,
                                                                         scheduler,
                                                                         unitOfWorkFactory,
                                                                         contextCustomizer,
                                                                         batchSize,
                                                                         context);
    }

    @Override
    public Registration subscribe(
            BiFunction<List<? extends EventMessage>, ProcessingContext, CompletableFuture<?>> eventsBatchConsumer
    ) {
        synchronized (this) {
            boolean noConsumer = this.consumer.equals(NO_OP_CONSUMER);
            if (noConsumer) {
                persistentStreamConnection.open(eventsBatchConsumer);
                this.consumer = eventsBatchConsumer;
            } else {
                boolean sameConsumer = this.consumer.equals(eventsBatchConsumer);
                if (!sameConsumer) {
                    throw new IllegalStateException(
                            String.format(
                                    "%s: Cannot subscribe to PersistentStreamEventSource with another consumer:"
                                            + " there is already an active subscription.",
                                    name));
                }
            }
        }
        return () -> {
            synchronized (this) {
                persistentStreamConnection.close();
                this.consumer = NO_OP_CONSUMER;
                return true;
            }
        };
    }
}
