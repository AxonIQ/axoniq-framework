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

package io.axoniq.framework.springcloud;

import io.axoniq.framework.messaging.queryhandling.distributed.QueryBusConnector;
import io.axoniq.framework.springcloud.routing.Member;
import io.axoniq.framework.springcloud.transport.IncomingQueryInvoker;
import io.axoniq.framework.springcloud.transport.QueryDispatchException;
import io.axoniq.framework.springcloud.transport.RemoteQueryDispatcher;
import io.axoniq.license.entitlement.EntitlementManager;
import io.axoniq.license.entitlement.EntitlementMessageType;
import org.axonframework.common.FutureUtils;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.common.lifecycle.Phase;
import org.axonframework.common.lifecycle.ShutdownInProgressException;
import org.axonframework.common.lifecycle.ShutdownLatch;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.queryhandling.GenericQueryMessage;
import org.axonframework.messaging.queryhandling.NoHandlerForQueryException;
import org.axonframework.messaging.queryhandling.QueryMessage;
import org.axonframework.messaging.queryhandling.QueryResponseMessage;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A {@link QueryBusConnector} distributing queries over the members a Spring Cloud discovery implementation reports.
 * <p>
 * Where a command is routed to one member by its routing key, a query asks nothing of where it is handled: any member
 * advertising the query's name may answer it. Successive queries of a name therefore rotate over the members
 * advertising it, spreading the load rather than sending every one to the same member.
 * <p>
 * A query is answered with a stream of responses rather than a single result, so the responses arrive as they are
 * produced. A member answering faster than this application consumes fails the query rather than buffering without
 * limit; see {@code HttpRemoteQueryDispatcher}.
 * <p>
 * Subscription queries are not carried by this connector. Rather than degrade into a query answered once, they are
 * rejected outright, so an application relying on updates learns that at the point it asks.
 *
 * @author Allard Buijze
 * @since 5.4.0
 */
public class SpringCloudQueryBusConnector implements QueryBusConnector {

    private static final Logger logger = LoggerFactory.getLogger(SpringCloudQueryBusConnector.class);

    private final SpringCloudMemberRegistry registry;
    private final IncomingQueryInvoker gateway;
    private final RemoteQueryDispatcher dispatcher;
    private final @Nullable MessageConverter converter;
    private final EntitlementManager entitlementManager;

    private final Set<QualifiedName> subscriptions = ConcurrentHashMap.newKeySet();
    private final ShutdownLatch shutdownLatch = new ShutdownLatch();

    private volatile @Nullable Handler incomingHandler;

    /**
     * Constructs a {@code SpringCloudQueryBusConnector} routing with the given {@code registry}.
     *
     * @param registry   the registry reporting which members handle which queries
     * @param gateway    the gateway queries arriving from other members are handled through
     * @param dispatcher the dispatcher sending queries to other members
     * @param converter  the converter attached to queries routed to this application, so that a locally routed query
     *                   carries the same conversion capability as one that travelled over the wire, or {@code null}
     *                   when none is available.
     */
    public SpringCloudQueryBusConnector(SpringCloudMemberRegistry registry,
                                        IncomingQueryInvoker gateway,
                                        RemoteQueryDispatcher dispatcher,
                                        @Nullable MessageConverter converter) {
        this(registry, gateway, dispatcher, converter, EntitlementManager.INSTANCE);
        EntitlementManager.INSTANCE.registerAddon(SpringCloudAxoniqAddon.class);
    }

    /**
     * Package-private constructor allowing an alternative {@link EntitlementManager} to be injected.
     * <p>
     * Marked {@link Internal} because production code must use
     * {@link #SpringCloudQueryBusConnector(SpringCloudMemberRegistry, IncomingQueryInvoker, RemoteQueryDispatcher,
     * MessageConverter)}, which registers the addon and claims against {@link EntitlementManager#INSTANCE}. This
     * constructor exists so tests need not touch that singleton.
     *
     * @param registry           the registry reporting which members handle which queries
     * @param gateway            the gateway queries arriving from other members are handled through
     * @param dispatcher         the dispatcher sending queries to other members
     * @param converter          the converter attached to queries routed to this application, or {@code null} when
     *                           none is available.
     * @param entitlementManager the manager each dispatched query is claimed against
     */
    @Internal
    SpringCloudQueryBusConnector(SpringCloudMemberRegistry registry,
                                 IncomingQueryInvoker gateway,
                                 RemoteQueryDispatcher dispatcher,
                                 @Nullable MessageConverter converter,
                                 EntitlementManager entitlementManager) {
        this.registry = Objects.requireNonNull(registry, "The registry must not be null.");
        this.gateway = Objects.requireNonNull(gateway, "The gateway must not be null.");
        this.dispatcher = Objects.requireNonNull(dispatcher, "The dispatcher must not be null.");
        this.converter = converter;
        this.entitlementManager = Objects.requireNonNull(entitlementManager,
                                                         "The entitlementManager must not be null.");
    }

    @Override
    public MessageStream<QueryResponseMessage> query(QueryMessage query,
                                                     @Nullable ProcessingContext context) {
        Objects.requireNonNull(query, "The query must not be null.");
        if (shutdownLatch.isShuttingDown()) {
            return MessageStream.failed(new ShutdownInProgressException(
                    "Cannot dispatch new queries as this connector is shutting down."
            ));
        }

        QualifiedName queryName = query.type().qualifiedName();
        Optional<Member> destination = registry.findQueryDestination(queryName);
        if (destination.isEmpty()) {
            return MessageStream.failed(new NoHandlerForQueryException(
                    "No member of the cluster handles queries of type [" + query.type() + "]."
            ));
        }

        entitlementManager.claimMessage(SpringCloudAxoniqAddon.IDENTIFIER, EntitlementMessageType.QUERY, 1);

        Member member = destination.get();
        ShutdownLatch.ActivityHandle inFlight;
        try {
            inFlight = shutdownLatch.registerActivity();
        } catch (ShutdownInProgressException e) {
            return MessageStream.failed(e);
        }
        try {
            MessageStream<QueryResponseMessage> responses =
                    member.local() ? handleLocally(query) : dispatcher.dispatch(member, query);
            // A query is in flight for as long as its responses are still arriving, so the activity ends with the
            // stream rather than with this method. onClose covers all three ways it can end: completed, failed, and
            // released by the caller.
            return responses.onClose(() -> {
                inFlight.end();
                suspectWhenUnreachable(member, responses);
            });
        } catch (Exception e) {
            inFlight.end();
            return MessageStream.failed(e);
        }
    }

    /**
     * Takes the given {@code member} out of the routing ring when the query failed because it could not be reached.
     * <p>
     * Mirrors what the command connector does with a member it could not reach: a member that is briefly unreachable
     * should not go on receiving its share of the queries until the next discovery round notices. A handler that ran
     * and rejected the query says nothing about reachability, so only a dispatch failure counts.
     *
     * @param member    the member the query was sent to
     * @param responses the responses to the query, now in a terminal state
     */
    private void suspectWhenUnreachable(Member member, MessageStream<QueryResponseMessage> responses) {
        responses.error()
                 .filter(cause -> cause instanceof QueryDispatchException)
                 .ifPresent(cause -> registry.markUnreachable(member));
    }

    /**
     * Prepares this connector for dispatching queries, after a previous {@link #shutdownDispatching()} if any.
     * <p>
     * Performed in the {@link Phase#INBOUND_QUERY_CONNECTOR} phase.
     */
    public void start() {
        shutdownLatch.initialize();
        logger.debug("The SpringCloudQueryBusConnector started.");
    }

    /**
     * Stops advertising the queries this member handles, so that other members stop routing them here.
     * <p>
     * Performed in the {@link Phase#INBOUND_QUERY_CONNECTOR} phase, before dispatching is shut down, so that other
     * members learn this member is leaving while it can still answer what is already in flight.
     *
     * @return a future that completes once this member no longer advertises any query
     */
    public CompletableFuture<Void> disconnect() {
        logger.debug("Disconnecting the SpringCloudQueryBusConnector.");
        subscriptions.clear();
        registry.publishLocalQueries(Set.of());
        return FutureUtils.emptyCompletedFuture();
    }

    /**
     * Stops dispatching new queries, and waits for the responses to the ones already dispatched.
     * <p>
     * Performed in the {@link Phase#OUTBOUND_QUERY_CONNECTORS} phase.
     *
     * @return a future that completes once every dispatched query has been answered
     */
    public CompletableFuture<Void> shutdownDispatching() {
        logger.debug("Shutting down dispatching of the SpringCloudQueryBusConnector.");
        return shutdownLatch.initiateShutdown();
    }

    @Override
    public MessageStream<QueryResponseMessage> subscriptionQuery(QueryMessage query,
                                                                 @Nullable ProcessingContext context,
                                                                 int updateBufferSize) {
        Objects.requireNonNull(query, "The query must not be null.");
        return MessageStream.failed(new UnsupportedOperationException(
                ("The Spring Cloud connector does not distribute subscription queries, so query [%s] cannot be "
                        + "answered with updates. Handle it as a regular query, or distribute with a connector that "
                        + "carries subscription queries.").formatted(query.type())
        ));
    }

    private MessageStream<QueryResponseMessage> handleLocally(QueryMessage query) {
        Handler handler = incomingHandler;
        if (handler == null) {
            return MessageStream.failed(new NoHandlerForQueryException(
                    "This member resolved as the destination for query [" + query.type()
                            + "], but no query handler is registered on the connector yet."
            ));
        }
        logger.debug("Handling query [{}] on this member.", query.type());
        return handler.query(withConverterAttached(query));
    }

    /**
     * Attaches the converter to a query handled on this member.
     * <p>
     * A query that travelled over the wire is rebuilt with the converter attached, so one routed to this member
     * carries the same conversion capability rather than behaving differently for having stayed put.
     */
    private QueryMessage withConverterAttached(QueryMessage query) {
        if (converter == null) {
            return query;
        }
        return new GenericQueryMessage(
                new GenericMessage(query.identifier(), query.type(), query.payload(), query.metadata()),
                query.priority().isPresent() ? query.priority().getAsInt() : null
        ).withConverter(converter);
    }

    @Override
    public CompletableFuture<Void> subscribe(QualifiedName queryName) {
        Objects.requireNonNull(queryName, "The queryName must not be null.");
        logger.debug("Subscribing to query [{}].", queryName);
        subscriptions.add(queryName);
        registry.publishLocalQueries(Set.copyOf(subscriptions));
        return FutureUtils.emptyCompletedFuture();
    }

    @Override
    public boolean unsubscribe(QualifiedName queryName) {
        Objects.requireNonNull(queryName, "The queryName must not be null.");
        if (!subscriptions.remove(queryName)) {
            return false;
        }
        logger.debug("Unsubscribing from query [{}].", queryName);
        registry.publishLocalQueries(Set.copyOf(subscriptions));
        return true;
    }

    @Override
    public void onIncomingQuery(Handler handler) {
        Objects.requireNonNull(handler, "The handler must not be null.");
        this.incomingHandler = handler;
        gateway.bind(handler);
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("registry", registry);
        descriptor.describeProperty("dispatcher", dispatcher);
        descriptor.describeProperty("subscriptions",
                                    subscriptions.stream().map(QualifiedName::toString).sorted().toList());
    }
}
