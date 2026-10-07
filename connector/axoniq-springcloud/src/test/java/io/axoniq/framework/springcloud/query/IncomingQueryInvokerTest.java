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

package io.axoniq.framework.springcloud.query;

import io.axoniq.framework.springcloud.util.RecordingQueryHandler;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QueueMessageStream;
import org.axonframework.messaging.queryhandling.GenericQueryResponseMessage;
import org.axonframework.messaging.queryhandling.GenericSubscriptionQueryUpdateMessage;
import org.axonframework.messaging.queryhandling.QueryExecutionException;
import org.axonframework.messaging.queryhandling.QueryMessage;
import org.axonframework.messaging.queryhandling.QueryResponseMessage;
import org.axonframework.messaging.queryhandling.SubscriptionQueryUpdateMessage;
import org.junit.jupiter.api.*;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests how {@link IncomingQueryInvoker} answers a query from another member, and how it reports what it cannot
 * answer.
 *
 * @author Allard Buijze
 */
class IncomingQueryInvokerTest {

    private static final MessageType FIND_COURSE_TYPE = new MessageType("university.FindCourse", "1.0.0");
    private static final MessageType RESPONSE_TYPE = new MessageType("university.Course", "1.0.0");
    private static final String PAYLOAD = "{\"id\":\"course-1\"}";

    private RecordingQueryHandler handler;
    private RecordingQueryResponseSink sink;
    private IncomingQueryInvoker testSubject;

    @BeforeEach
    void setUp() {
        handler = new RecordingQueryHandler();
        sink = new RecordingQueryResponseSink();
        testSubject = new IncomingQueryInvoker(() -> "node-b", null);
        testSubject.bind(handler);
    }

    private static QueryDispatchRequest request() {
        return new QueryDispatchRequest("query-1",
                                        FIND_COURSE_TYPE.toString(),
                                        PAYLOAD,
                                        Map.of(),
                                        null);
    }

    private static SubscriptionQueryUpdateMessage update(String identifier) {
        return new GenericSubscriptionQueryUpdateMessage(
                new GenericMessage(identifier, RESPONSE_TYPE, PAYLOAD, Map.of())
        );
    }

    private static QueryResponseMessage response(String identifier) {
        return new GenericQueryResponseMessage(
                new GenericMessage(identifier, RESPONSE_TYPE, PAYLOAD, Map.of())
        );
    }

    @Nested
    class AnsweringAQuery {

        @Test
        void writesEveryResponseTheHandlerProduces() {
            // given
            handler.answeringWith(response("response-1"), response("response-2"), response("response-3"));

            // when
            testSubject.handle(request(), sink);

            // then
            assertThat(sink.responses()).extracting(QueryDispatchResponse::identifier)
                                        .containsExactly("response-1", "response-2", "response-3");
            assertThat(sink.completed()).isTrue();
            assertThat(sink.error()).isNull();
        }

        @Test
        void namesTheQueryEveryResponseAnswers() {
            // given
            handler.answeringWith(response("response-1"));

            // when
            testSubject.handle(request(), sink);

            // then the member that asked matches the responses to the query it sent
            assertThat(sink.responses()).singleElement()
                                        .extracting(QueryDispatchResponse::requestIdentifier)
                                        .isEqualTo("query-1");
        }

        @Test
        void completesWithoutResponsesWhenTheHandlerProducesNone() {
            // when — a query with no answer is answered, just not with anything
            testSubject.handle(request(), sink);

            // then
            assertThat(sink.responses()).isEmpty();
            assertThat(sink.completed()).isTrue();
            assertThat(sink.error()).isNull();
        }

        @Test
        void handsTheHandlerTheQueryThatWasSent() {
            // when
            testSubject.handle(request(), sink);

            // then
            assertThat(handler.queries()).singleElement().satisfies(query -> {
                assertThat(query.identifier()).isEqualTo("query-1");
                assertThat(query.type()).isEqualTo(FIND_COURSE_TYPE);
            });
        }
    }

    @Nested
    class AnsweringAsResponsesArrive {

        @Test
        void writesResponsesTheHandlerProducesAfterItReturned() {
            // given a handler answering asynchronously, as one reading from a store does
            QueueMessageStream<QueryResponseMessage> responses = new QueueMessageStream<>();
            handler = new RecordingQueryHandler() {
                @Override
                public MessageStream<QueryResponseMessage> query(QueryMessage query) {
                    return responses;
                }
            };
            testSubject = new IncomingQueryInvoker(() -> "node-b", null);
            testSubject.bind(handler);

            // when
            testSubject.handle(request(), sink);
            assertThat(sink.responses()).isEmpty();
            responses.offer(response("response-1"), Context.empty());
            responses.offer(response("response-2"), Context.empty());
            responses.seal();

            // then
            assertThat(sink.responses()).extracting(QueryDispatchResponse::identifier)
                                        .containsExactly("response-1", "response-2");
            assertThat(sink.completed()).isTrue();
        }
    }

    @Nested
    class WhenNothingIsReadingTheAnswer {

        private QueueMessageStream<QueryResponseMessage> responses;

        @BeforeEach
        void answerAsynchronously() {
            responses = new QueueMessageStream<>();
            testSubject = new IncomingQueryInvoker(() -> "node-b", null);
            testSubject.bind(new RecordingQueryHandler() {
                @Override
                public MessageStream<QueryResponseMessage> query(QueryMessage query) {
                    return responses;
                }
            });
        }

        @Test
        void releasesTheHandlersResponseStream() {
            // given a query being answered
            testSubject.handle(request(), sink);
            responses.offer(response("response-1"), Context.empty());

            // when the member that asked stops reading, or waits longer than it was given
            sink.becomeUnavailable();

            // then the handler is not left producing responses into a stream nobody holds
            assertThat(responses.offer(response("response-2"), Context.empty())).isFalse();
        }

        @Test
        void writesNothingFurther() {
            // given
            testSubject.handle(request(), sink);
            responses.offer(response("response-1"), Context.empty());
            sink.becomeUnavailable();

            // when a response arrives after the member stopped reading
            responses.offer(response("response-2"), Context.empty());

            // then
            assertThat(sink.responses()).extracting(QueryDispatchResponse::identifier).containsExactly("response-1");
        }

        @Test
        void reportsNoFailureForAnAnswerNobodyIsWaitingFor() {
            // given
            testSubject.handle(request(), sink);

            // when
            sink.becomeUnavailable();

            // then the member that asked is gone, so there is nobody to report a failure to
            assertThat(sink.error()).isNull();
            assertThat(sink.completed()).isFalse();
        }

        @Test
        void leavesAQueryAnsweredInFullAlone() {
            // given a query that completed before the container reported the stream closed
            testSubject.handle(request(), sink);
            responses.offer(response("response-1"), Context.empty());
            responses.seal();

            // when the container reports completion, as it does for every stream it closes
            sink.becomeUnavailable();

            // then the answer that was already delivered stands
            assertThat(sink.responses()).extracting(QueryDispatchResponse::identifier).containsExactly("response-1");
            assertThat(sink.completed()).isTrue();
        }
    }

    @Nested
    class AnsweringASubscriptionQuery {

        private static SubscriptionQueryRequest subscription() {
            return new SubscriptionQueryRequest("query-1", FIND_COURSE_TYPE.toString(), PAYLOAD, Map.of(), null, 16);
        }

        @Test
        void registersAnUpdateHandlerRatherThanAnsweringOnce() {
            // when
            testSubject.handleSubscription(subscription(), sink);

            // then the initial result is a query of its own; this carries updates alone
            assertThat(handler.subscriptions()).hasSize(1);
            assertThat(handler.queries()).isEmpty();
            assertThat(sink.responses()).isEmpty();
        }

        @Test
        void writesEveryUpdateTheHandlerEmits() {
            // given
            testSubject.handleSubscription(subscription(), sink);

            // when
            handler.emit(update("update-1"));
            handler.emit(update("update-2"));

            // then
            assertThat(sink.updates()).extracting(QueryDispatchResponse::identifier)
                                      .containsExactly("update-1", "update-2");
        }

        @Test
        void namesTheSubscriptionEveryUpdateBelongsTo() {
            // given
            testSubject.handleSubscription(subscription(), sink);

            // when
            handler.emit(update("update-1"));

            // then the subscribing member matches the updates to the subscription it opened
            assertThat(sink.updates()).singleElement()
                                      .extracting(QueryDispatchResponse::requestIdentifier)
                                      .isEqualTo("query-1");
        }

        @Test
        void reportsTheSubscriptionOverWhenTheHandlerCompletesIt() {
            // given
            testSubject.handleSubscription(subscription(), sink);

            // when the application says there will never be another update
            handler.subscriptions().forEach(registered -> registered.callback().complete());

            // then the subscriber is told the subscription is over, not merely that this member stopped answering
            assertThat(sink.subscriptionCompletedFor()).isEqualTo("query-1");
        }

        @Test
        void cancelsTheRegistrationWhenTheSubscriberGoesAway() {
            // given
            testSubject.handleSubscription(subscription(), sink);

            // when the member that subscribed stops reading
            sink.becomeUnavailable();

            // then this member is not left emitting into nothing
            assertThat(handler.subscriptions()).isEmpty();
        }

        @Test
        void reportsNoHandlerWhenNoneIsBoundYet() {
            // given a member still starting up
            IncomingQueryInvoker unbound = new IncomingQueryInvoker(() -> "node-b", null);

            // when
            unbound.handleSubscription(subscription(), sink);

            // then the member that subscribed may usefully try again
            assertThat(sink.error()).isNotNull();
            assertThat(sink.error().errorCode()).isEqualTo(QueryErrorCode.NO_HANDLER_FOR_QUERY);
        }

        @Test
        void reportsAnUnreadableRequestAsWorthNoRetry() {
            // given a subscription naming a type that cannot be read
            SubscriptionQueryRequest unreadable =
                    new SubscriptionQueryRequest("query-1", "not a message type", null, Map.of(), null, 16);

            // when
            testSubject.handleSubscription(unreadable, sink);

            // then
            assertThat(sink.error()).isNotNull();
            assertThat(sink.error().errorCode()).isEqualTo(QueryErrorCode.QUERY_EXECUTION_NON_TRANSIENT_ERROR);
        }

        @Test
        void rejectsAMissingRequest() {
            assertThatThrownBy(() -> testSubject.handleSubscription(null, sink))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        void rejectsAMissingSink() {
            assertThatThrownBy(() -> testSubject.handleSubscription(subscription(), null))
                    .isInstanceOf(NullPointerException.class);
        }
    }

    @Nested
    class WhenThisMemberLeaves {

        private static SubscriptionQueryRequest subscription(String identifier) {
            return new SubscriptionQueryRequest(identifier, FIND_COURSE_TYPE.toString(), PAYLOAD, Map.of(), null, 16);
        }

        @Test
        void announcesLeavingOnAnOpenSubscriptionRatherThanFailingOrCompletingIt() {
            // given
            testSubject.handleSubscription(subscription("query-1"), sink);

            // when
            testSubject.leave();

            // then only this member's part ends: the subscriber carries on with the members that remain
            assertThat(sink.leaving()).containsExactly("query-1");
            assertThat(sink.error()).isNull();
            assertThat(sink.subscriptionCompletedFor()).isNull();
        }

        @Test
        void cancelsTheUpdateRegistrationSoNothingGoesOnBeingProduced() {
            // given
            testSubject.handleSubscription(subscription("query-1"), sink);

            // when
            testSubject.leave();
            handler.emit(update("update-1"));

            // then
            assertThat(sink.updates()).isEmpty();
        }

        @Test
        void announcesLeavingOnEverySubscriptionThisMemberAnswers() {
            // given
            RecordingQueryResponseSink other = new RecordingQueryResponseSink();
            testSubject.handleSubscription(subscription("query-1"), sink);
            testSubject.handleSubscription(subscription("query-2"), other);

            // when
            testSubject.leave();

            // then
            assertThat(sink.leaving()).containsExactly("query-1");
            assertThat(other.leaving()).containsExactly("query-2");
        }

        @Test
        void announcesLeavingStraightAwayOnASubscriptionArrivingAfterwards() {
            // given
            testSubject.leave();

            // when a subscription arrives while this member is still advertising the query
            testSubject.handleSubscription(subscription("query-1"), sink);
            handler.emit(update("update-1"));

            // then it is not registered, as nothing on this member emits updates anymore
            assertThat(sink.leaving()).containsExactly("query-1");
            assertThat(sink.updates()).isEmpty();
        }

        @Test
        void announcesLeavingOnlyOnceWhenLeavingTwice() {
            // given
            testSubject.handleSubscription(subscription("query-1"), sink);
            testSubject.leave();

            // when
            testSubject.leave();

            // then
            assertThat(sink.leaving()).containsExactly("query-1");
        }

        @Test
        void leavesASubscriptionTheSubscriberAlreadyReleasedAlone() {
            // given the subscriber went away first, which cancels the registration through the sink
            testSubject.handleSubscription(subscription("query-1"), sink);
            sink.becomeUnavailable();

            // when
            testSubject.leave();

            // then nothing is reported to a stream nobody is reading
            assertThat(sink.leaving()).isEmpty();
        }

        @Test
        void isHarmlessWhenNoSubscriptionIsOpen() {
            // when / then
            testSubject.leave();
            assertThat(sink.leaving()).isEmpty();
        }
    }

    @Nested
    class ReportingFailures {

        @Test
        void reportsNoHandlerWhenNoneIsBoundYet() {
            // given a member still starting up
            IncomingQueryInvoker unbound = new IncomingQueryInvoker(() -> "node-b", null);

            // when
            unbound.handle(request(), sink);

            // then the member that asked may usefully try again
            assertThat(sink.error()).isNotNull();
            assertThat(sink.error().errorCode()).isEqualTo(QueryErrorCode.NO_HANDLER_FOR_QUERY);
        }

        @Test
        void reportsAnUnreadableRequestAsWorthNoRetry() {
            // given a request naming a type that cannot be read
            QueryDispatchRequest unreadable =
                    new QueryDispatchRequest("query-1", "not a message type", null, Map.of(), null);

            // when
            testSubject.handle(unreadable, sink);

            // then sending the same bytes again cannot succeed
            assertThat(sink.error()).isNotNull();
            assertThat(sink.error().errorCode()).isEqualTo(QueryErrorCode.QUERY_EXECUTION_NON_TRANSIENT_ERROR);
        }

        @Test
        void reportsAFailingHandler() {
            // given
            handler.failingWith(new QueryExecutionException("The course store is unavailable.", null));

            // when
            testSubject.handle(request(), sink);

            // then
            assertThat(sink.error()).isNotNull();
            assertThat(sink.error().errorMessage()).contains("The course store is unavailable.");
            assertThat(sink.completed()).isFalse();
        }

        @Test
        void namesTheMemberAFailureOccurredOn() {
            // given
            handler.failingWith(new QueryExecutionException("The course store is unavailable.", null));

            // when
            testSubject.handle(request(), sink);

            // then
            assertThat(sink.error()).isNotNull();
            assertThat(sink.error().errorOrigin()).isEqualTo("node-b");
        }

        @Test
        void abandonsAQueryWhoseAskerStoppedReading() {
            // given a member that has gone away mid-answer
            handler.answeringWith(response("response-1"));
            sink.failingOnWriteWith(new IllegalStateException("The stream is closed."));

            // when
            testSubject.handle(request(), sink);

            // then reporting a failure to a member that is not reading would be pointless
            assertThat(sink.error()).isNull();
            assertThat(sink.completed()).isFalse();
        }
    }

    @Nested
    class Validation {

        @Test
        void rejectsAMissingMemberName() {
            assertThatThrownBy(() -> new IncomingQueryInvoker(null, null))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        void rejectsAMissingRequest() {
            assertThatThrownBy(() -> testSubject.handle(null, sink)).isInstanceOf(NullPointerException.class);
        }

        @Test
        void rejectsAMissingSink() {
            assertThatThrownBy(() -> testSubject.handle(request(), null)).isInstanceOf(NullPointerException.class);
        }

        @Test
        void rejectsAMissingHandler() {
            assertThatThrownBy(() -> testSubject.bind(null)).isInstanceOf(NullPointerException.class);
        }
    }
}
