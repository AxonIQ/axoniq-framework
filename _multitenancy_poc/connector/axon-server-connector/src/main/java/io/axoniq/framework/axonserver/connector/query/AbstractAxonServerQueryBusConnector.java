/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the
 * specific language governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */

package io.axoniq.framework.axonserver.connector.query;

import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.axonserver.connector.FlowControl;
import io.axoniq.axonserver.connector.Registration;
import io.axoniq.axonserver.connector.ReplyChannel;
import io.axoniq.axonserver.connector.ResultStream;
import io.axoniq.axonserver.connector.query.QueryDefinition;
import io.axoniq.axonserver.connector.query.QueryHandler;
import io.axoniq.axonserver.grpc.query.QueryRequest;
import io.axoniq.axonserver.grpc.query.QueryResponse;
import io.axoniq.axonserver.grpc.query.SubscriptionQuery;
import io.axoniq.framework.axonserver.connector.api.ConnectorLifecycle;
import io.axoniq.framework.axonserver.connector.shared.ErrorCode;
import io.axoniq.framework.messaging.queryhandling.distributed.QueryBusConnector;
import org.axonframework.common.FutureUtils;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.common.lifecycle.Phase;
import org.axonframework.common.lifecycle.ShutdownLatch;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.queryhandling.QueryMessage;
import org.axonframework.messaging.queryhandling.QueryResponseMessage;
import org.axonframework.messaging.queryhandling.SubscriptionQueryUpdateMessage;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.LockSupport;

import static java.util.Objects.requireNonNull;

/**
 * Shared Axon Server query-bus connector behavior.
 * <p>
 * Concrete connectors provide the connection(s) they dispatch to. This base class keeps the common lifecycle,
 * conversion and incoming-query handler plumbing in one place so that single-context and multi-tenant connectors can
 * reuse the same behavior.
 *
 * @author Jan Galinski
 * @since 5.2.0
 */
public abstract class AbstractAxonServerQueryBusConnector implements QueryBusConnector, ConnectorLifecycle {

    protected final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    protected final String clientId;
    protected final String componentName;
    protected final @Nullable MessageConverter converter;
    protected final ShutdownLatch shutdownLatch = new ShutdownLatch();
    protected @Nullable Handler incomingHandler;

    protected AbstractAxonServerQueryBusConnector(String clientId,
                                                  String componentName,
                                                  @Nullable MessageConverter converter) {
        this.clientId = requireNonNull(clientId, "The clientId must not be null.");
        this.componentName = requireNonNull(componentName, "The componentName must not be null.");
        this.converter = converter;
    }

    @Override
    public void start() {
        shutdownLatch.initialize();
        logger.trace("The {} started.", getClass().getSimpleName());
    }

    @Override
    public void onIncomingQuery(Handler handler) {
        this.incomingHandler = requireNonNull(handler, "The incoming query handler must not be null.");
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("clientId", clientId);
        descriptor.describeProperty("componentName", componentName);
    }

    protected final MessageStream<QueryResponseMessage> doQuery(QueryMessage query,
                                                                AxonServerConnection connection) {
        shutdownLatch.ifShuttingDown("Cannot dispatch new queries as this bus is being shut down");

        try (ShutdownLatch.ActivityHandle queryInTransit = shutdownLatch.registerActivity()) {
            ResultStream<QueryResponse> resultStream = connection.queryChannel()
                                                                 .query(QueryConverter.convertQueryMessage(
                                                                         query,
                                                                         clientId,
                                                                         componentName)
                                                                 );
            return new QueryResponseMessageStream(resultStream, converter).onClose(queryInTransit::end);
        }
    }

    protected final MessageStream<QueryResponseMessage> doSubscriptionQuery(QueryMessage query,
                                                                            AxonServerConnection connection,
                                                                            int updateBufferSize) {
        shutdownLatch.ifShuttingDown("Cannot dispatch new queries as this bus is being shut down");

        try (ShutdownLatch.ActivityHandle queryInTransit = shutdownLatch.registerActivity()) {
            var result = connection.queryChannel()
                                   .subscriptionQuery(QueryConverter.convertQueryMessage(query,
                                                                                         clientId,
                                                                                         componentName),
                                                      updateBufferSize,
                                                      Math.min(updateBufferSize / 4, 8));
            return new QueryResponseMessageStream(result.initialResults(), converter)
                    .concatWith(new QueryUpdateMessageStream(result.updates(), converter))
                    .onClose(queryInTransit::end);
        }
    }

    protected final CompletableFuture<Void> doSubscribe(AxonServerConnection connection,
                                                        QualifiedName name,
                                                        LocalSegmentAdapter localSegmentAdapter,
                                                        Map<QualifiedName, Registration> subscriptions) {
        logger.debug("Subscribing to query handler [{}].", name);
        QueryDefinition definition = new QueryDefinition(name.fullName(), "");
        Registration registration = connection.queryChannel()
                                              .registerQueryHandler(localSegmentAdapter, definition);
        subscriptions.put(name, registration);

        CompletableFuture<Void> completion = new CompletableFuture<>();
        registration.onAck(() -> completion.complete(null));
        return completion;
    }

    protected final boolean doUnsubscribe(QualifiedName name,
                                          Map<QualifiedName, Registration> subscriptions) {
        Registration subscription = subscriptions.remove(name);
        if (subscription != null) {
            subscription.cancel();
            return true;
        }
        return false;
    }

    protected final CompletableFuture<Void> doDisconnect(AxonServerConnection connection,
                                                         LocalSegmentAdapter localSegmentAdapter) {
        if (connection.isConnected()) {
            logger.trace("Disconnecting the {}.", getClass().getSimpleName());
            connection.queryChannel().prepareDisconnect();
        }
        if (!localSegmentAdapter.awaitTermination(Duration.ofSeconds(5))) {
            logger.info("Awaited termination of queries in progress without success. "
                                + "Going to cancel remaining queries in progress.");
            localSegmentAdapter.cancel();
        }
        return FutureUtils.emptyCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> shutdownDispatching() {
        logger.trace("Shutting down dispatching of the {}.", getClass().getSimpleName());
        return shutdownLatch.initiateShutdown();
    }

    protected final LocalSegmentAdapter newLocalSegmentAdapter() {
        return new LocalSegmentAdapter(new ConcurrentHashMap<>());
    }

    protected final LocalSegmentAdapter newLocalSegmentAdapter(Map<String, Runnable> queriesInProgress) {
        return new LocalSegmentAdapter(queriesInProgress);
    }

    protected final class LocalSegmentAdapter implements QueryHandler {

        private final Map<String, Runnable> queriesInProgress;

        protected LocalSegmentAdapter(Map<String, Runnable> queriesInProgress) {
            this.queriesInProgress = requireNonNull(queriesInProgress, "queriesInProgress may not be null");
        }

        @Override
        public void handle(QueryRequest query, ReplyChannel<QueryResponse> responseHandler) {
            stream(query, responseHandler).request(Long.MAX_VALUE);
        }

        @Override
        public FlowControl stream(QueryRequest query, ReplyChannel<QueryResponse> responseHandler) {
            var result = requireNonNull(incomingHandler, "incomingHandler not configured")
                    .query(QueryConverter.convertQueryRequest(query, converter));
            var previous = queriesInProgress.put(query.getMessageIdentifier(), result::close);
            if (previous != null) {
                previous.run();
            }
            return new FlowControlledResponseSender(clientId, query.getMessageIdentifier(),
                                                    result.onClose(queriesInProgress.remove(
                                                            query.getMessageIdentifier())),
                                                    responseHandler);
        }

        @Override
        public Registration registerSubscriptionQuery(SubscriptionQuery query, UpdateHandler sendUpdate) {
            var registration = requireNonNull(incomingHandler, "incomingHandler not configured")
                    .registerUpdateHandler(QueryConverter.convertSubscriptionQueryMessage(query, converter),
                                           new AxonServerUpdateCallback(sendUpdate));
            return () -> {
                registration.cancel();
                return FutureUtils.emptyCompletedFuture();
            };
        }

        protected boolean awaitTermination(Duration timeout) {
            Instant startAwait = Instant.now();
            Instant endAwait = startAwait.plusSeconds(timeout.getSeconds());
            while (Instant.now().isBefore(endAwait) && !queriesInProgress.isEmpty()) {
                queriesInProgress.values()
                                 .stream()
                                 .findFirst()
                                 .ifPresent(queryInProgress -> {
                                     while (Instant.now().isBefore(endAwait)) {
                                         LockSupport.parkNanos(10_000_000);
                                     }
                                 });
            }
            return queriesInProgress.isEmpty();
        }

        protected void cancel() {
            queriesInProgress.values()
                             .iterator()
                             .forEachRemaining(Runnable::run);
        }
    }

    protected final class AxonServerUpdateCallback implements UpdateCallback {

        private final QueryHandler.UpdateHandler updateHandler;

        protected AxonServerUpdateCallback(QueryHandler.UpdateHandler updateHandler) {
            this.updateHandler = updateHandler;
        }

        @Override
        public CompletableFuture<Void> sendUpdate(SubscriptionQueryUpdateMessage update) {
            updateHandler.sendUpdate(QueryConverter.convertQueryUpdate(update));
            return FutureUtils.emptyCompletedFuture();
        }

        @Override
        public CompletableFuture<Void> complete() {
            updateHandler.complete();
            return FutureUtils.emptyCompletedFuture();
        }

        @Override
        public CompletableFuture<Void> completeExceptionally(Throwable error) {
            updateHandler.sendUpdate(QueryConverter.convertQueryUpdate(clientId,
                                                                       ErrorCode.QUERY_EXECUTION_ERROR,
                                                                       error));
            updateHandler.complete();
            return FutureUtils.emptyCompletedFuture();
        }
    }
}
