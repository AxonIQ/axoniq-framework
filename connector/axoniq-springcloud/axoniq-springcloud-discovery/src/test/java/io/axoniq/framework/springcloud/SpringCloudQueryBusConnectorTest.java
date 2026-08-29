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

import io.axoniq.framework.springcloud.discovery.RecordingCapabilityDiscoveryMode;
import io.axoniq.framework.springcloud.routing.Member;
import io.axoniq.framework.springcloud.routing.MemberCapabilities;
import io.axoniq.framework.springcloud.transport.IncomingQueryGateway;
import io.axoniq.framework.springcloud.transport.RecordingRemoteQueryDispatcher;
import io.axoniq.framework.springcloud.transport.SubscriptionQueryMembersChangedException;
import io.axoniq.framework.springcloud.util.RecordingDiscoveryClient;
import io.axoniq.framework.springcloud.util.RecordingEntitlementManager;
import io.axoniq.framework.springcloud.util.RecordingQueryHandler;
import io.axoniq.framework.springcloud.util.TestServiceInstance;
import io.axoniq.license.entitlement.EntitlementMessageType;
import org.axonframework.common.lifecycle.ShutdownInProgressException;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.QueueMessageStream;
import org.axonframework.messaging.queryhandling.GenericQueryMessage;
import org.axonframework.messaging.queryhandling.GenericQueryResponseMessage;
import org.axonframework.messaging.queryhandling.GenericSubscriptionQueryUpdateMessage;
import org.axonframework.messaging.queryhandling.NoHandlerForQueryException;
import org.axonframework.messaging.queryhandling.QueryExecutionException;
import org.axonframework.messaging.queryhandling.QueryMessage;
import org.axonframework.messaging.queryhandling.QueryResponseMessage;
import org.axonframework.messaging.queryhandling.SubscriptionQueryUpdateMessage;
import org.junit.jupiter.api.*;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests how {@link SpringCloudQueryBusConnector} routes queries across the cluster and reports what it cannot carry.
 *
 * @author Allard Buijze
 */
class SpringCloudQueryBusConnectorTest {

    private static final MessageType FIND_COURSE_TYPE = new MessageType("university.FindCourse", "1.0.0");
    private static final QualifiedName FIND_COURSE = FIND_COURSE_TYPE.qualifiedName();
    private static final QualifiedName LIST_COURSES = new QualifiedName("university.ListCourses");
    private static final MessageType RESPONSE_TYPE = new MessageType("university.Course", "1.0.0");
    private static final byte[] PAYLOAD = "{\"id\":\"course-1\"}".getBytes(StandardCharsets.UTF_8);

    private TestServiceInstance localInstance;
    private TestServiceInstance remoteInstance;
    private RecordingDiscoveryClient discoveryClient;
    private RecordingCapabilityDiscoveryMode discoveryMode;
    private SpringCloudMemberRegistry registry;
    private RecordingQueryHandler handler;
    private RecordingRemoteQueryDispatcher dispatcher;
    private RecordingEntitlementManager entitlementManager;
    private SpringCloudQueryBusConnector testSubject;

    @BeforeEach
    void setUp() {
        localInstance = TestServiceInstance.instance("university", "node-a", 8080);
        remoteInstance = TestServiceInstance.instance("university", "node-b", 8080);
        discoveryClient = new RecordingDiscoveryClient().register("university", localInstance);
        discoveryMode = new RecordingCapabilityDiscoveryMode();
        registry = new SpringCloudMemberRegistry(discoveryClient, localInstance, discoveryMode);
        handler = new RecordingQueryHandler();
        dispatcher = new RecordingRemoteQueryDispatcher();
        entitlementManager = new RecordingEntitlementManager();
        testSubject = new SpringCloudQueryBusConnector(registry,
                                                       new IncomingQueryGateway(() -> "node-a", null),
                                                       dispatcher,
                                                       null,
                                                       entitlementManager);
        testSubject.onIncomingQuery(handler);
    }

    private static QueryMessage query() {
        return new GenericQueryMessage(new GenericMessage("query-1", FIND_COURSE_TYPE, PAYLOAD, Map.of()), null);
    }

    private static QueryResponseMessage response(String identifier) {
        return new GenericQueryResponseMessage(
                new GenericMessage(identifier, RESPONSE_TYPE, PAYLOAD, Map.of())
        );
    }

    private static List<QueryResponseMessage> drain(MessageStream<QueryResponseMessage> stream) {
        List<QueryResponseMessage> collected = new ArrayList<>();
        while (stream.hasNextAvailable()) {
            stream.next().ifPresent(entry -> collected.add(entry.message()));
        }
        return collected;
    }

    private static SubscriptionQueryUpdateMessage update(String identifier) {
        return new GenericSubscriptionQueryUpdateMessage(
                new GenericMessage(identifier, RESPONSE_TYPE, PAYLOAD, Map.of())
        );
    }

    private Member remoteMember() {
        return registry.ring().getMembers().stream()
                       .filter(member -> !member.local())
                       .filter(member -> member.name().contains("node-b"))
                       .findFirst()
                       .orElseThrow();
    }

    private Member otherRemoteMember() {
        return registry.ring().getMembers().stream()
                       .filter(member -> !member.local())
                       .filter(member -> member.name().contains("node-c"))
                       .findFirst()
                       .orElseThrow();
    }

    /**
     * Makes two other members advertise the query, so that a subscription has to reach both.
     */
    private void twoRemoteMembersHandleTheQuery() {
        TestServiceInstance other = TestServiceInstance.instance("university", "node-c", 8080);
        discoveryClient.register("university", localInstance, remoteInstance, other);
        discoveryMode.answering(remoteInstance, new MemberCapabilities(0, Set.of(), Set.of(FIND_COURSE)));
        discoveryMode.answering(other, new MemberCapabilities(0, Set.of(), Set.of(FIND_COURSE)));
        registry.updateMemberships();
    }

    /**
     * Makes the remote member the only one advertising the query, so that routing has to reach it.
     */
    private void remoteMemberHandlesTheQuery() {
        discoveryClient.register("university", localInstance, remoteInstance);
        discoveryMode.answering(remoteInstance, new MemberCapabilities(0, Set.of(), Set.of(FIND_COURSE)));
        registry.updateMemberships();
    }

    @Nested
    class RoutingToAnotherMember {

        @Test
        void sendsTheQueryToTheMemberAdvertisingIt() {
            // given
            remoteMemberHandlesTheQuery();
            dispatcher.answeringWith(response("response-1"), response("response-2"));

            // when
            List<QueryResponseMessage> responses = drain(testSubject.query(query(), null));

            // then
            assertThat(dispatcher.dispatches()).hasSize(1);
            assertThat(dispatcher.members()).singleElement().satisfies(m -> assertThat(m.local()).isFalse());
            assertThat(responses).extracting(QueryResponseMessage::identifier)
                                 .containsExactly("response-1", "response-2");
        }

        @Test
        void leavesTheLocalHandlerUntouched() {
            // given
            remoteMemberHandlesTheQuery();

            // when
            drain(testSubject.query(query(), null));

            // then
            assertThat(handler.queries()).isEmpty();
        }

        @Test
        void claimsTheQueryAgainstTheEntitlementManager() {
            // given
            remoteMemberHandlesTheQuery();

            // when
            drain(testSubject.query(query(), null));

            // then
            assertThat(entitlementManager.claims()).singleElement().satisfies(claim -> {
                assertThat(claim.messageType()).isEqualTo(EntitlementMessageType.QUERY);
                assertThat(claim.count()).isEqualTo(1);
            });
        }
    }

    @Nested
    class RoutingToThisMember {

        @Test
        void answersFromTheLocalHandler() {
            // given
            testSubject.subscribe(FIND_COURSE);
            handler.answeringWith(response("response-1"));

            // when
            List<QueryResponseMessage> responses = drain(testSubject.query(query(), null));

            // then
            assertThat(handler.queries()).hasSize(1);
            assertThat(dispatcher.dispatches()).isEmpty();
            assertThat(responses).extracting(QueryResponseMessage::identifier).containsExactly("response-1");
        }

        @Test
        void reportsNoHandlerWhenTheConnectorHasNoneYet() {
            // given a member advertising the query before its handler was registered on the connector
            SpringCloudQueryBusConnector unbound = new SpringCloudQueryBusConnector(
                    registry, new IncomingQueryGateway(() -> "node-a", null), dispatcher, null, entitlementManager
            );
            unbound.subscribe(FIND_COURSE);

            // when
            MessageStream<QueryResponseMessage> responses = unbound.query(query(), null);

            // then
            assertThat(responses.error()).containsInstanceOf(NoHandlerForQueryException.class);
        }
    }

    @Nested
    class WhenNoMemberHandlesTheQuery {

        @Test
        void reportsNoHandlerRatherThanAnEmptyAnswer() {
            // when — an empty answer would read as "no such course" rather than "nobody asked"
            MessageStream<QueryResponseMessage> responses = testSubject.query(query(), null);

            // then
            assertThat(responses.error()).containsInstanceOf(NoHandlerForQueryException.class);
        }

        @Test
        void claimsNothingAgainstTheEntitlementManager() {
            // when
            testSubject.query(query(), null);

            // then a query that reached no member is not a query the cluster carried
            assertThat(entitlementManager.claims()).isEmpty();
        }
    }

    @Nested
    class Subscribing {

        @Test
        void advertisesTheSubscribedQueries() {
            // when
            testSubject.subscribe(FIND_COURSE);
            testSubject.subscribe(LIST_COURSES);

            // then this is what other members read from the capabilities endpoint
            assertThat(discoveryMode.localCapabilities().queries()).containsExactlyInAnyOrder(FIND_COURSE,
                                                                                             LIST_COURSES);
        }

        @Test
        void asksForNoShareOfTheCommandLoad() {
            // when — a query carries no routing key, so advertising one says nothing about command routing
            testSubject.subscribe(FIND_COURSE);

            // then
            assertThat(discoveryMode.localCapabilities().loadFactor()).isZero();
            assertThat(discoveryMode.localCapabilities().commands()).isEmpty();
        }

        @Test
        void stopsAdvertisingAnUnsubscribedQuery() {
            // given
            testSubject.subscribe(FIND_COURSE);
            testSubject.subscribe(LIST_COURSES);

            // when
            boolean removed = testSubject.unsubscribe(FIND_COURSE);

            // then
            assertThat(removed).isTrue();
            assertThat(discoveryMode.localCapabilities().queries()).containsExactly(LIST_COURSES);
        }

        @Test
        void reportsNothingRemovedForAQueryItNeverAdvertised() {
            // when / then
            assertThat(testSubject.unsubscribe(FIND_COURSE)).isFalse();
        }
    }

    @Nested
    class SubscriptionQueries {

        @Test
        void subscribeToEveryMemberAdvertisingTheQuery() {
            // given two other members both handling the query
            twoRemoteMembersHandleTheQuery();

            // when
            testSubject.subscriptionQuery(query(), null, 16);

            // then an update is emitted on whichever member's state changed, so none of them may be left out
            assertThat(dispatcher.subscriptions()).extracting(subscription -> subscription.member().name())
                                                  .containsExactlyInAnyOrder(remoteMember().name(),
                                                                             otherRemoteMember().name());
        }

        @Test
        void askOneMemberForTheInitialResult() {
            // given
            twoRemoteMembersHandleTheQuery();
            dispatcher.answeringWith(response("initial-1"));

            // when
            drain(testSubject.subscriptionQuery(query(), null, 16));

            // then the initial result is a query like any other, answered once however many members are subscribed to
            assertThat(dispatcher.dispatches()).hasSize(1);
        }

        @Test
        void waitForTheAnsweringMembersSubscriptionBeforeAskingForTheInitialResult() {
            // given a member that has not registered the subscription yet
            remoteMemberHandlesTheQuery();
            dispatcher.openingOnDemand().answeringWith(response("initial-1"));

            // when
            MessageStream<QueryResponseMessage> responses = testSubject.subscriptionQuery(query(), null, 16);
            drain(responses);

            // then asking now would leave an update emitted while the result is produced with nowhere to arrive
            assertThat(dispatcher.subscriptions()).hasSize(1);
            assertThat(dispatcher.dispatches()).isEmpty();

            // when the member reports the subscription registered
            dispatcher.open(remoteMember());

            // then
            assertThat(dispatcher.dispatches()).hasSize(1);
            assertThat(drain(responses)).extracting(QueryResponseMessage::identifier).containsExactly("initial-1");
        }

        @Test
        void failWhenTheAnsweringMembersSubscriptionCannotBeOpened() {
            // given a member that cannot be subscribed to at all
            remoteMemberHandlesTheQuery();
            dispatcher.openingOnDemand().failingWith(new QueryExecutionException("node-b is unreachable.", null));

            // when
            MessageStream<QueryResponseMessage> responses = testSubject.subscriptionQuery(query(), null, 16);

            // then the initial result is not left waiting on a subscription that will never open
            drain(responses);
            assertThat(dispatcher.dispatches()).isEmpty();
            assertThat(responses.error()).isPresent();
            assertThat(responses.error().orElseThrow()).hasMessageContaining("node-b is unreachable.");
        }

        @Test
        void answerWithTheInitialResultBeforeTheUpdates() {
            // given
            remoteMemberHandlesTheQuery();
            dispatcher.answeringWith(response("initial-1"));

            // when
            MessageStream<QueryResponseMessage> responses = testSubject.subscriptionQuery(query(), null, 16);
            List<QueryResponseMessage> received = new ArrayList<>(drain(responses));
            dispatcher.emit(remoteMember(), response("update-1"));
            received.addAll(drain(responses));

            // then
            assertThat(received).extracting(QueryResponseMessage::identifier)
                                .containsExactly("initial-1", "update-1");
        }

        @Test
        void carryTheUpdatesOfEveryMember() {
            // given
            twoRemoteMembersHandleTheQuery();

            // when
            MessageStream<QueryResponseMessage> responses = testSubject.subscriptionQuery(query(), null, 16);
            drain(responses);
            dispatcher.emit(remoteMember(), response("update-from-b"));
            dispatcher.emit(otherRemoteMember(), response("update-from-c"));

            // then a subscriber sees one stream of updates however many members produce them
            assertThat(drain(responses)).extracting(QueryResponseMessage::identifier)
                                        .containsExactlyInAnyOrder("update-from-b", "update-from-c");
        }

        @Test
        void failWhenAMemberStartsHandlingTheQuery() {
            // given a subscription across the members that handle the query today
            remoteMemberHandlesTheQuery();
            MessageStream<QueryResponseMessage> responses = testSubject.subscriptionQuery(query(), null, 16);
            drain(responses);

            // when another member starts advertising it
            TestServiceInstance joining = TestServiceInstance.instance("university", "node-c", 8080);
            discoveryClient.register("university", localInstance, remoteInstance, joining);
            discoveryMode.answering(joining, new MemberCapabilities(0, Set.of(), Set.of(FIND_COURSE)));
            registry.updateMemberships();

            // then the updates it emitted before being subscribed to are gone, so carrying on would be silently
            // incomplete
            drain(responses);
            assertThat(responses.error()).containsInstanceOf(SubscriptionQueryMembersChangedException.class);
        }

        @Test
        void carryOnWhenAMemberThatDoesNotHandleTheQueryJoins() {
            // given
            remoteMemberHandlesTheQuery();
            MessageStream<QueryResponseMessage> responses = testSubject.subscriptionQuery(query(), null, 16);
            drain(responses);

            // when a member joins that advertises something else entirely
            TestServiceInstance joining = TestServiceInstance.instance("university", "node-c", 8080);
            discoveryClient.register("university", localInstance, remoteInstance, joining);
            discoveryMode.answering(joining, new MemberCapabilities(0, Set.of(), Set.of(LIST_COURSES)));
            registry.updateMemberships();

            // then it emits no updates for this query, so there is nothing to have missed
            drain(responses);
            assertThat(responses.error()).isEmpty();
        }

        @Test
        void failWhenAMemberEndsTheSubscription() {
            // given
            remoteMemberHandlesTheQuery();
            MessageStream<QueryResponseMessage> responses = testSubject.subscriptionQuery(query(), null, 16);
            drain(responses);

            // when one member fails, the subscriber would otherwise receive some of the updates and believe it
            // received all of them
            dispatcher.fail(remoteMember(), new QueryExecutionException("The course store is unavailable.", null));

            // then
            drain(responses);
            assertThat(responses.error()).isPresent();
            assertThat(responses.error().orElseThrow()).hasMessageContaining("The course store is unavailable.");
        }

        @Test
        void releaseTheOtherMembersSubscriptionsWhenOneOfThemFails() {
            // given a subscription across two members
            twoRemoteMembersHandleTheQuery();
            MessageStream<QueryResponseMessage> responses = testSubject.subscriptionQuery(query(), null, 16);
            drain(responses);

            // when one of them fails
            dispatcher.fail(remoteMember(), new QueryExecutionException("The course store is unavailable.", null));
            drain(responses);

            // then the member still standing is not left emitting into a subscription that has already failed
            assertThat(dispatcher.subscriptions()).allMatch(RecordingRemoteQueryDispatcher.Subscription::released);
        }

        @Test
        void keepTheSubscriptionOpenWhileAnyMemberStillHoldsIt() {
            // given a subscription across two members
            twoRemoteMembersHandleTheQuery();
            MessageStream<QueryResponseMessage> responses = testSubject.subscriptionQuery(query(), null, 16);
            drain(responses);

            // when one of them stops answering, as a member leaving the cluster does
            dispatcher.stopAnswering(remoteMember());
            drain(responses);

            // then the other still has updates to give
            assertThat(responses.isCompleted()).isFalse();
            dispatcher.emit(otherRemoteMember(), response("update-from-c"));
            assertThat(drain(responses)).extracting(QueryResponseMessage::identifier)
                                        .containsExactly("update-from-c");
        }

        @Test
        void endTheWholeSubscriptionWhenAMemberReportsItOver() {
            // given a subscription across two members
            twoRemoteMembersHandleTheQuery();
            MessageStream<QueryResponseMessage> responses = testSubject.subscriptionQuery(query(), null, 16);
            drain(responses);

            // when one of them reports the subscription over, which says there will never be another update
            dispatcher.completeSubscription(remoteMember());
            drain(responses);

            // then waiting on the other members would be waiting for updates that are never coming
            assertThat(responses.isCompleted()).isTrue();
            assertThat(responses.error()).isEmpty();
        }

        @Test
        void completeOnceEveryMemberHasCompleted() {
            // given
            twoRemoteMembersHandleTheQuery();
            MessageStream<QueryResponseMessage> responses = testSubject.subscriptionQuery(query(), null, 16);
            drain(responses);

            // when
            dispatcher.stopAnswering(remoteMember());
            dispatcher.stopAnswering(otherRemoteMember());
            drain(responses);

            // then
            assertThat(responses.isCompleted()).isTrue();
            assertThat(responses.error()).isEmpty();
        }

        @Test
        void subscribeOnThisMemberWhenItHandlesTheQuery() {
            // given this member is the only one advertising the query
            testSubject.subscribe(FIND_COURSE);

            // when
            MessageStream<QueryResponseMessage> responses = testSubject.subscriptionQuery(query(), null, 16);
            drain(responses);
            handler.emit(update("update-1"));

            // then a local subscription carries updates just as a remote one does
            assertThat(handler.subscriptions()).hasSize(1);
            assertThat(drain(responses)).extracting(QueryResponseMessage::identifier).containsExactly("update-1");
        }

        @Test
        void releaseEverySubscriptionWhenTheStreamIsClosed() {
            // given
            remoteMemberHandlesTheQuery();
            MessageStream<QueryResponseMessage> responses = testSubject.subscriptionQuery(query(), null, 16);
            drain(responses);

            // when
            responses.close();

            // then the answering member is not left emitting into nothing
            assertThat(dispatcher.subscriptions()).allMatch(RecordingRemoteQueryDispatcher.Subscription::released);
        }

        @Test
        void reportNoHandlerWhenNoMemberAdvertisesTheQuery() {
            // given a query nothing in the cluster handles
            QueryMessage unhandled = new GenericQueryMessage(
                    new GenericMessage("query-2", new MessageType("university.Unknown", "1.0.0"), PAYLOAD, Map.of()),
                    null
            );

            // when
            MessageStream<QueryResponseMessage> responses = testSubject.subscriptionQuery(unhandled, null, 16);

            // then
            assertThat(responses.error()).containsInstanceOf(NoHandlerForQueryException.class);
        }

        @Test
        void rejectANonPositiveUpdateBufferSize() {
            assertThatThrownBy(() -> testSubject.subscriptionQuery(query(), null, 0))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("update buffer size");
        }
    }

    @Nested
    class ShuttingDown {

        @BeforeEach
        void startTheConnector() {
            testSubject.start();
        }

        @Test
        void stopsAdvertisingTheQueriesThisMemberHandled() {
            // given a member other members are routing queries to
            testSubject.subscribe(FIND_COURSE);

            // when
            testSubject.disconnect();

            // then other members stop routing queries here rather than discovering it went away by timing out
            assertThat(discoveryMode.localCapabilities().queries()).isEmpty();
            assertThat(registry.findQueryDestination(FIND_COURSE)).isEmpty();
        }

        @Test
        void doesNotWaitOnAnOpenSubscriptionQuery() {
            // given a subscription that is behaving correctly, which is to say lasting until the subscriber ends it
            remoteMemberHandlesTheQuery();
            MessageStream<QueryResponseMessage> responses = testSubject.subscriptionQuery(query(), null, 16);
            drain(responses);

            // when
            CompletableFuture<Void> shutdown = testSubject.shutdownDispatching();

            // then waiting for it would be waiting for the subscriber, which has no reason to ever finish
            assertThat(shutdown).isCompleted();
        }

        @Test
        void refusesNewSubscriptionQueriesOnceDispatchingIsShutDown() {
            // given
            remoteMemberHandlesTheQuery();
            testSubject.shutdownDispatching();

            // when
            MessageStream<QueryResponseMessage> responses = testSubject.subscriptionQuery(query(), null, 16);

            // then
            assertThat(responses.error()).containsInstanceOf(ShutdownInProgressException.class);
        }

        @Test
        void refusesNewQueriesOnceDispatchingIsShutDown() {
            // given
            remoteMemberHandlesTheQuery();
            testSubject.shutdownDispatching();

            // when
            MessageStream<QueryResponseMessage> responses = testSubject.query(query(), null);

            // then
            assertThat(responses.error()).containsInstanceOf(ShutdownInProgressException.class);
            assertThat(dispatcher.dispatches()).isEmpty();
        }

        @Test
        void waitsForAQueryStillBeingAnswered() {
            // given a query whose responses are still arriving
            remoteMemberHandlesTheQuery();
            QueueMessageStream<QueryResponseMessage> answering = new QueueMessageStream<>();
            dispatcher.answeringWith(answering);
            MessageStream<QueryResponseMessage> responses = testSubject.query(query(), null);

            // when
            CompletableFuture<Void> shutdown = testSubject.shutdownDispatching();

            // then shutting down does not abandon a query that is still being answered
            assertThat(shutdown).isNotCompleted();

            // and when the answer completes, so does the shutdown
            answering.offer(response("response-1"), Context.empty());
            answering.seal();
            drain(responses);
            assertThat(shutdown).isCompleted();
        }

        @Test
        void completesImmediatelyWhenNoQueryIsInFlight() {
            // when
            CompletableFuture<Void> shutdown = testSubject.shutdownDispatching();

            // then
            assertThat(shutdown).isCompleted();
        }

        @Test
        void endsTheQueryWhenTheAnswerFailsRatherThanWaitingOnIt() {
            // given
            remoteMemberHandlesTheQuery();
            dispatcher.failingWith(new QueryExecutionException("The course store is unavailable.", null));
            MessageStream<QueryResponseMessage> responses = testSubject.query(query(), null);
            responses.error();

            // when
            CompletableFuture<Void> shutdown = testSubject.shutdownDispatching();

            // then a failed query is a finished query, and must not hold shutdown open
            assertThat(shutdown).isCompleted();
        }

        @Test
        void endsAQueryThatFailedBeforeAnyoneReadIt() {
            // given a query that failed on dispatch, whose failure the caller never looked at
            remoteMemberHandlesTheQuery();
            dispatcher.failingWith(new QueryExecutionException("The course store is unavailable.", null));
            testSubject.query(query(), null);

            // when
            CompletableFuture<Void> shutdown = testSubject.shutdownDispatching();

            // then a query that never started must not hold shutdown open waiting to be read
            assertThat(shutdown).isCompleted();
        }

        @Test
        void dispatchesAgainAfterStartingFollowingAShutdown() {
            // given a connector that was shut down and started again
            remoteMemberHandlesTheQuery();
            testSubject.shutdownDispatching();
            testSubject.start();

            // when
            dispatcher.answeringWith(response("response-1"));
            MessageStream<QueryResponseMessage> responses = testSubject.query(query(), null);

            // then
            assertThat(responses.error()).isEmpty();
            assertThat(drain(responses)).hasSize(1);
        }
    }

    @Nested
    class Validation {

        @Test
        void rejectsANullQuery() {
            assertThatThrownBy(() -> testSubject.query(null, null)).isInstanceOf(NullPointerException.class);
        }

        @Test
        void rejectsANullQueryName() {
            assertThatThrownBy(() -> testSubject.subscribe(null)).isInstanceOf(NullPointerException.class);
        }

        @Test
        void rejectsANullHandler() {
            assertThatThrownBy(() -> testSubject.onIncomingQuery(null)).isInstanceOf(NullPointerException.class);
        }
    }
}
