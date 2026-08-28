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

package io.axoniq.framework.messaging.queryhandling.distributed;

import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.queryhandling.GenericQueryMessage;
import org.axonframework.messaging.queryhandling.GenericQueryResponseMessage;
import org.axonframework.messaging.queryhandling.QueryBus;
import org.axonframework.messaging.queryhandling.QueryMessage;
import org.axonframework.messaging.queryhandling.QueryResponseMessage;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Wiring test that assembles a full {@link MessagingConfigurer} stack - {@link DistributedQueryBus} over a
 * {@link LocalShortcutQueryBusConnector} decorating a recording connector - and verifies end-to-end that point-to-point
 * queries take the local shortcut exactly when the {@link LocalQueryDispatchPredicate} and a local subscription allow
 * it, and are routed through the connector otherwise.
 *
 * @author Allard Buijze
 */
class LocalShortcutQueryBusWiringTest {

    private static final QualifiedName SUBSCRIBED = new QualifiedName("io.axoniq.test.SubscribedQuery");
    private static final QualifiedName UNSUBSCRIBED = new QualifiedName("io.axoniq.test.UnsubscribedQuery");
    private static final String LOCAL_HANDLER_RESULT = "local-result";

    private RecordingQueryBusConnector connector;
    private AtomicBoolean localHandlerInvoked;

    @BeforeEach
    void setUp() {
        connector = new RecordingQueryBusConnector();
        localHandlerInvoked = new AtomicBoolean(false);
    }

    private QueryBus queryBusWith(LocalQueryDispatchPredicate predicate) {
        Configuration config =
                MessagingConfigurer.create()
                                   .componentRegistry(cr -> {
                                       cr.registerComponent(QueryBusConnector.class, c -> connector);
                                       cr.registerComponent(LocalQueryDispatchPredicate.class, c -> predicate);
                                   })
                                   .build();
        QueryBus queryBus = config.getComponent(QueryBus.class);
        queryBus.subscribe(SUBSCRIBED, (query, context) -> {
            localHandlerInvoked.set(true);
            return MessageStream.just(queryResponseMessage(LOCAL_HANDLER_RESULT)).cast();
        });
        return queryBus;
    }

    @Test
    void shortcutsToLocalHandlerWhenSubscribedAndPredicateMatches() {
        // given
        QueryBus queryBus = queryBusWith((query, context) -> true);

        // when
        CompletableFuture<QueryResponseMessage> result =
                queryBus.query(queryMessage(SUBSCRIBED), null)
                        .first()
                        .asCompletableFuture()
                        .thenApply(MessageStream.Entry::message);

        // then
        assertThat(result).succeedsWithin(Duration.ofSeconds(5))
                          .satisfies(response -> assertThat(response.payload()).isEqualTo(LOCAL_HANDLER_RESULT));
        assertThat(localHandlerInvoked).isTrue();
        assertThat(connector.queryCount).hasValue(0);
    }

    @Test
    void routesThroughConnectorWhenQueryNotLocallySubscribed() {
        // given
        QueryBus queryBus = queryBusWith((query, context) -> true);

        // when
        queryBus.query(queryMessage(UNSUBSCRIBED), null);

        // then
        await().atMost(Duration.ofSeconds(5)).until(() -> connector.queryCount.get() == 1);
        assertThat(localHandlerInvoked).isFalse();
    }

    @Test
    void routesThroughConnectorWhenPredicateDoesNotMatch() {
        // given
        QueryBus queryBus = queryBusWith((query, context) -> false);

        // when
        queryBus.query(queryMessage(SUBSCRIBED), null);

        // then
        await().atMost(Duration.ofSeconds(5)).until(() -> connector.queryCount.get() == 1);
        assertThat(localHandlerInvoked).isFalse();
    }

    private static QueryMessage queryMessage(QualifiedName name) {
        return new GenericQueryMessage(new MessageType(name), "payload");
    }

    private static QueryResponseMessage queryResponseMessage(String payload) {
        return new GenericQueryResponseMessage(new MessageType("io.axoniq.test.QueryResult"), payload);
    }

    /**
     * Minimal {@link QueryBusConnector} that records point-to-point query invocations and never subscribes a remote
     * handler, standing in for a remote segment.
     */
    private static class RecordingQueryBusConnector implements QueryBusConnector {

        private final AtomicInteger queryCount = new AtomicInteger();

        @Override
        public MessageStream<QueryResponseMessage> query(QueryMessage query, @Nullable ProcessingContext context) {
            queryCount.incrementAndGet();
            return MessageStream.empty().cast();
        }

        @Override
        public MessageStream<QueryResponseMessage> subscriptionQuery(QueryMessage query,
                                                                     @Nullable ProcessingContext context,
                                                                     int updateBufferSize) {
            return MessageStream.empty().cast();
        }

        @Override
        public CompletableFuture<Void> subscribe(QualifiedName name) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public boolean unsubscribe(QualifiedName name) {
            return true;
        }

        @Override
        public void onIncomingQuery(Handler handler) {
            // No remote inbound handling needed for this test.
        }

        @Override
        public void describeTo(ComponentDescriptor descriptor) {
            descriptor.describeProperty("name", "RecordingQueryBusConnector");
        }
    }
}
