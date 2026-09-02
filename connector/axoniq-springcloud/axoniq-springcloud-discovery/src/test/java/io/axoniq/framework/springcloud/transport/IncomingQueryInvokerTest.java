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

package io.axoniq.framework.springcloud.transport;

import io.axoniq.framework.springcloud.util.RecordingQueryHandler;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QueueMessageStream;
import org.axonframework.messaging.queryhandling.GenericQueryResponseMessage;
import org.axonframework.messaging.queryhandling.QueryExecutionException;
import org.axonframework.messaging.queryhandling.QueryMessage;
import org.axonframework.messaging.queryhandling.QueryResponseMessage;
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
