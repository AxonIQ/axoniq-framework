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
import io.axoniq.framework.springcloud.transport.RemoteQueryDispatcher.SubscriptionListener;
import io.axoniq.framework.springcloud.transport.SubscriptionQueryMembersChangedException;
import io.axoniq.license.entitlement.EntitlementManager;
import io.axoniq.license.entitlement.EntitlementMessageType;
import org.axonframework.common.FutureUtils;
import org.axonframework.common.Registration;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.common.lifecycle.Phase;
import org.axonframework.common.lifecycle.ShutdownInProgressException;
import org.axonframework.common.lifecycle.ShutdownLatch;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.DelayedMessageStream;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.QueueMessageStream;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.queryhandling.GenericQueryMessage;
import org.axonframework.messaging.queryhandling.GenericQueryResponseMessage;
import org.axonframework.messaging.queryhandling.NoHandlerForQueryException;
import org.axonframework.messaging.queryhandling.QueryMessage;
import org.axonframework.messaging.queryhandling.QueryResponseMessage;
import org.axonframework.messaging.queryhandling.SubscriptionQueryUpdateMessage;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

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
 * A subscription query reaches every member advertising its name, not just the one a plain query would route to: an
 * update is emitted on whichever member's state changed, and only reaches subscriptions that member holds a
 * registration for. One of them answers the initial result as well, so a subscriber gets one initial result and the
 * updates of the whole cluster. A member that starts advertising the query while a subscription is active fails that
 * subscription rather than joining it late; see {@link SubscriptionQueryMembersChangedException}.
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
     * The updates of every member subscribed to for one subscription query, drained into a single stream.
     * <p>
     * A subscriber sees one stream of updates however many members produce them. There is no meaningful order across
     * members -- an update belongs to whichever member's state changed -- so they are drained as they arrive rather
     * than interleaved by any rule.
     * <p>
     * A member failing fails the whole subscription. Carrying on with the rest would leave the subscriber receiving
     * some of the updates and believing it received all of them.
     */
    private static final class SubscriptionUpdates {

        private final QueryMessage query;
        private final QueueMessageStream<QueryResponseMessage> merged;
        private final List<Source> sources = new CopyOnWriteArrayList<>();
        private final AtomicInteger openSources;
        // Failed alongside the merged updates, so that a subscription whose answering member could not be reached
        // does not leave the initial result waiting on a stream that will never open.
        private final CompletableFuture<Void> answeringMemberOpen;

        private SubscriptionUpdates(QueryMessage query,
                                    int updateBufferSize,
                                    int memberCount,
                                    CompletableFuture<Void> answeringMemberOpen) {
            this.query = query;
            this.answeringMemberOpen = answeringMemberOpen;
            this.merged = new QueueMessageStream<>(new ArrayBlockingQueue<>(updateBufferSize));
            // Counted up front rather than as members are added: members are subscribed to concurrently, and one
            // completing before the next is added would otherwise look like the last of them.
            this.openSources = new AtomicInteger(memberCount);
        }

        private void add(Member member, MessageStream<QueryResponseMessage> updates) {
            Source source = new Source(member, updates);
            sources.add(source);
            updates.setCallback(() -> drain(source));
            drain(source);
        }

        /**
         * Moves what the given {@code source} has available onto the merged stream.
         * <p>
         * Synchronised on the source, because a stream reports availability on whichever thread produced the update
         * and two of those draining the same source would take entries out from under each other.
         */
        private void drain(Source source) {
            synchronized (source) {
                if (source.counted) {
                    return;
                }
                try {
                    while (source.stream.hasNextAvailable()) {
                        Optional<MessageStream.Entry<QueryResponseMessage>> next = source.stream.next();
                        if (next.isEmpty()) {
                            break;
                        }
                        if (!merged.offer(next.get().message(), Context.empty())) {
                            end(source, () -> fail(new IllegalStateException(
                                    ("The members answering query [%s] produced more updates than this application "
                                            + "consumed. Consume them sooner, or raise the update buffer size.")
                                            .formatted(query.type())
                            )));
                            return;
                        }
                    }
                    if (!source.stream.hasNextAvailable() && source.stream.isCompleted()) {
                        end(source, () -> source.stream.error().ifPresentOrElse(this::fail, this::sourceCompleted));
                    }
                } catch (Exception e) {
                    logger.debug("Failed to read the updates member [{}] produced for query [{}].",
                                 source.member.name(), query.type(), e);
                    end(source, () -> fail(e));
                }
            }
        }

        /**
         * Accounts for the given {@code source} having ended, unless it already has been.
         * <p>
         * Reading a stream that has just been sealed reports its completion, which drains that same source again
         * from inside this drain. Checking on the way in is not enough, because the nested drain reaches the end
         * first and the outer one has already passed that check: both would count the same member, taking the tally
         * past zero and cutting off the members still holding subscriptions.
         */
        private static void end(Source source, Runnable ended) {
            if (!source.counted) {
                source.counted = true;
                ended.run();
            }
        }

        /**
         * Ends the subscription because a member reported it over, however many members still hold one.
         * <p>
         * A member saying so means there will never be another update, which is not what a member merely ceasing to
         * answer means. Waiting for the rest would leave the subscriber holding a subscription that has run its
         * course.
         */
        private void completeAll() {
            merged.seal();
        }

        private void sourceCompleted() {
            if (openSources.decrementAndGet() == 0) {
                merged.seal();
            }
        }

        /**
         * Fails the whole subscription, and with it every member's part in it.
         * <p>
         * Releasing the others rides on the merged updates being closed, which failing them leads to: the subscriber
         * observes the failure, and closing what it was reading releases every member's subscription.
         */
        private void fail(Throwable cause) {
            merged.sealExceptionally(cause);
            answeringMemberOpen.completeExceptionally(cause);
        }

        /**
         * Returns the merged updates, releasing every member's subscription when closed.
         */
        private MessageStream<QueryResponseMessage> stream() {
            return merged.onClose(this::release);
        }

        private void release() {
            sources.forEach(source -> source.stream.close());
        }

        /**
         * One member's part in a subscription, and whether its end has already been counted.
         */
        private static final class Source {

            private final Member member;
            private final MessageStream<QueryResponseMessage> stream;

            // Guarded by this source's monitor, which every drain holds.
            private boolean counted;

            private Source(Member member, MessageStream<QueryResponseMessage> stream) {
                this.member = member;
                this.stream = stream;
            }
        }
    }

    /**
     * Offers the updates of a subscription handled on this member onto the stream carrying them to the subscriber.
     */
    private static final class QueueingUpdateCallback implements UpdateCallback {

        private final QueueMessageStream<QueryResponseMessage> updates;
        private final QueryMessage query;
        private final SubscriptionListener listener;

        private QueueingUpdateCallback(QueueMessageStream<QueryResponseMessage> updates,
                                       QueryMessage query,
                                       SubscriptionListener listener) {
            this.updates = updates;
            this.query = query;
            this.listener = listener;
        }

        @Override
        public CompletableFuture<Void> sendUpdate(SubscriptionQueryUpdateMessage update) {
            if (!updates.offer(new GenericQueryResponseMessage(update), Context.empty())) {
                IllegalStateException cause = new IllegalStateException(
                        ("This member produced more updates to query [%s] than the subscriber consumed. Consume them "
                                + "sooner, or raise the update buffer size.").formatted(query.type())
                );
                // Ends the subscription rather than only reporting back to the emitter. An update that was produced
                // and not carried is one the subscriber will never see, and a subscription that continued would leave
                // it holding some of the updates and the belief it has all of them -- which is what a member
                // outpacing this application over the wire fails the subscription for.
                updates.sealExceptionally(cause);
                return CompletableFuture.failedFuture(cause);
            }
            return FutureUtils.emptyCompletedFuture();
        }

        @Override
        public CompletableFuture<Void> complete() {
            // Reported before sealing, so the subscription ends rather than only this member's part in it.
            listener.completed();
            updates.seal();
            return FutureUtils.emptyCompletedFuture();
        }

        @Override
        public CompletableFuture<Void> completeExceptionally(Throwable cause) {
            updates.sealExceptionally(cause);
            return FutureUtils.emptyCompletedFuture();
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
        if (updateBufferSize < 1) {
            throw new IllegalArgumentException(
                    "The update buffer size must be at least 1, but was [" + updateBufferSize + "]."
            );
        }
        if (shutdownLatch.isShuttingDown()) {
            return MessageStream.failed(new ShutdownInProgressException(
                    "Cannot dispatch new queries as this connector is shutting down."
            ));
        }

        QualifiedName queryName = query.type().qualifiedName();
        // Every member advertising the name, not just the one a plain query would route to: an update is emitted on
        // whichever member's state changed, and only reaches subscriptions that member holds a registration for.
        List<Member> members = registry.findAllQueryDestinations(queryName);
        if (members.isEmpty()) {
            return MessageStream.failed(new NoHandlerForQueryException(
                    "No member of the cluster handles queries of type [" + query.type() + "]."
            ));
        }
        // Resolved against the members already snapshotted, rather than taken as whatever the registry answers now.
        // A ring change between the two reads would otherwise name a member no update stream is opened on, leaving
        // the initial result waiting on an opening that never comes.
        Member answering = registry.findQueryDestination(queryName)
                                   .filter(members::contains)
                                   .orElseGet(members::getFirst);

        entitlementManager.claimMessage(SpringCloudAxoniqAddon.IDENTIFIER, EntitlementMessageType.QUERY, 1);

        // Registered and ended within this method, unlike a plain query, which stays in flight until its responses
        // have arrived. A subscription has no such end: waiting for one at shutdown would be waiting for the
        // subscriber to lose interest. What registering buys here is refusing a subscription started while this
        // connector is already going down.
        try (ShutdownLatch.ActivityHandle ignored = shutdownLatch.registerActivity()) {
            return openSubscription(query, queryName, members, answering, updateBufferSize);
        } catch (ShutdownInProgressException e) {
            return MessageStream.failed(e);
        }
    }

    private MessageStream<QueryResponseMessage> openSubscription(QueryMessage query,
                                                                 QualifiedName queryName,
                                                                 List<Member> members,
                                                                 Member answering,
                                                                 int updateBufferSize) {
        // Two activities, in this order. Opening the update streams comes first, and the initial result waits for
        // the answering member's to be open, so that an update emitted while that result is being produced still has
        // somewhere to arrive. Only the answering member is waited for: no other member is asked for a result, so
        // there is nothing its updates could arrive ahead of.
        CompletableFuture<Void> answeringMemberOpen = new CompletableFuture<>();
        SubscriptionUpdates updates =
                new SubscriptionUpdates(query, updateBufferSize, members.size(), answeringMemberOpen);
        for (Member member : members) {
            // Only the answering member's opening is waited on; every member's completion ends the subscription.
            boolean answersInitialResult = member.equals(answering);
            SubscriptionListener listener = new SubscriptionListener() {
                @Override
                public void opened() {
                    if (answersInitialResult) {
                        answeringMemberOpen.complete(null);
                    }
                }

                @Override
                public void completed() {
                    updates.completeAll();
                }
            };
            updates.add(member, updateStreamOn(member, query, updateBufferSize, listener));
        }
        MessageStream<QueryResponseMessage> initialResult = DelayedMessageStream.create(
                answeringMemberOpen.thenApply(open -> initialResultFrom(answering, query))
        );

        Registration watch = watchMembership(query, queryName, members, updates);
        return initialResult.concatWith(updates.stream())
                            // Releasing every member's subscription rides on the merged updates being closed, which
                            // concatenating does however the composed stream ends.
                            .onClose(watch::cancel);
    }

    private MessageStream<QueryResponseMessage> updateStreamOn(Member member,
                                                              QueryMessage query,
                                                              int updateBufferSize,
                                                              SubscriptionListener listener) {
        return member.local()
                ? localUpdateStream(query, updateBufferSize, listener)
                : dispatcher.openSubscriptionQueryUpdateStream(member, query, updateBufferSize, listener);
    }

    /**
     * Opens the update stream of a subscription handled on this member, without the wire.
     * <p>
     * Registering is immediate here, so the subscription is open by the time this returns.
     */
    private MessageStream<QueryResponseMessage> localUpdateStream(QueryMessage query,
                                                                  int updateBufferSize,
                                                                  SubscriptionListener listener) {
        Handler handler = incomingHandler;
        if (handler == null) {
            return MessageStream.failed(new NoHandlerForQueryException(
                    "This member resolved as a destination for query [" + query.type()
                            + "], but no query handler is registered on the connector yet."
            ));
        }
        QueueMessageStream<QueryResponseMessage> updates =
                new QueueMessageStream<>(new ArrayBlockingQueue<>(updateBufferSize));
        Registration registration;
        try {
            registration = handler.registerUpdateHandler(withConverterAttached(query),
                                                         new QueueingUpdateCallback(updates, query, listener));
        } catch (Exception e) {
            return MessageStream.failed(e);
        }
        listener.opened();
        return updates.onClose(registration::cancel);
    }

    /**
     * Asks the given {@code member} for the initial result, which is a query like any other.
     */
    private MessageStream<QueryResponseMessage> initialResultFrom(Member member, QueryMessage query) {
        return member.local() ? handleLocally(query) : dispatcher.dispatch(member, query);
    }

    /**
     * Fails the subscription when a member starts advertising the query while it is active.
     * <p>
     * Such a member has been emitting updates since before it was subscribed to, and those updates are gone. Failing
     * lets the subscriber establish the query again and get a complete stream from that point, rather than carrying
     * on silently incomplete.
     */
    private Registration watchMembership(QueryMessage query,
                                         QualifiedName queryName,
                                         List<Member> subscribedTo,
                                         SubscriptionUpdates updates) {
        Set<Member> known = Set.copyOf(subscribedTo);
        Registration watch =
                registry.onMembershipChanged(ring -> failWhenAMemberJoined(query, queryName, known, updates));
        // Checked once more now that the watch is in place. Opening a subscription on every member takes as long as
        // reaching them all does, and a member that started advertising the query in that time changed the ring
        // before there was a listener to hear it -- so the watch alone would never report it.
        failWhenAMemberJoined(query, queryName, known, updates);
        return watch;
    }

    /**
     * Fails the subscription if any member now advertises the query that was not among those subscribed to.
     * <p>
     * Failing twice is harmless: the merged updates are sealed once, and sealing a stream that has already ended does
     * nothing. So the watch and the check that follows registering it may both report the same member.
     */
    private void failWhenAMemberJoined(QueryMessage query,
                                       QualifiedName queryName,
                                       Set<Member> known,
                                       SubscriptionUpdates updates) {
        List<Member> current = registry.findAllQueryDestinations(queryName);
        for (Member member : current) {
            if (!known.contains(member)) {
                logger.info("Member [{}] started handling query [{}] while a subscription for it was active; "
                                    + "failing that subscription.", member.name(), query.type());
                updates.fail(new SubscriptionQueryMembersChangedException(
                        ("Member [%s] started handling query [%s] after this subscription began, so the updates "
                                + "it emitted before being subscribed to are lost. Establish the subscription "
                                + "query again to receive a complete stream.")
                                .formatted(member.name(), query.type())
                ));
                return;
            }
        }
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
