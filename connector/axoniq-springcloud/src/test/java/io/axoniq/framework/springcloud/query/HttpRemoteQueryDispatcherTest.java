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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import io.axoniq.framework.springcloud.query.RemoteQueryDispatcher.SubscriptionListener;
import io.axoniq.framework.springcloud.routing.Member;
import org.axonframework.common.ExceptionUtils;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.conversion.DelegatingMessageConverter;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.queryhandling.GenericQueryMessage;
import org.axonframework.messaging.queryhandling.NoHandlerForQueryException;
import org.axonframework.messaging.queryhandling.QueryMessage;
import org.axonframework.messaging.queryhandling.QueryResponseMessage;
import org.axonframework.messaging.queryhandling.SubscriptionQueryUpdateMessage;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.*;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.http.client.ClientHttpRequest;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.mock.http.client.MockClientHttpRequest;
import org.springframework.web.client.RestClient;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests how {@link HttpRemoteQueryDispatcher} behaves while reading a member's response stream, in particular when
 * that member outpaces this application, when this application stops reading, and when either end fails.
 * <p>
 * The member's response body is fed a byte at a time from the test, so a read can be observed mid-stream rather than
 * only after it finished. That is what makes contention and cancellation observable at all.
 *
 * @author Allard Buijze
 */
class HttpRemoteQueryDispatcherTest {

    private static final MessageType FIND_COURSE_TYPE = new MessageType("university.FindCourse", "1.0.0");
    private static final MessageType RESPONSE_TYPE = new MessageType("university.Course", "1.0.0");
    private static final String PAYLOAD = "{\"id\":\"course-1\"}";
    private static final String ENDPOINT = "/axoniq-springcloud/query";
    private static final Member MEMBER = new Member("node-b", URI.create("http://node-b:8080"), false);

    private final MessageConverter converter = new DelegatingMessageConverter(new JacksonConverter());
    // Writes what the answering member would, standing in for the other end of the stream. Deliberately not the
    // converter under test, so that what is read is asserted against plain JSON rather than against itself.
    private final ObjectMapper objectMapper =
            JsonMapper.builder().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES).build();

    private SseBody body;
    private StubRequestFactory requestFactory;
    private ExecutorService executor;
    private ScheduledExecutorService scheduler;

    @BeforeEach
    void setUp() {
        body = new SseBody();
        requestFactory = new StubRequestFactory(body);
        executor = Executors.newSingleThreadExecutor();
        scheduler = Executors.newSingleThreadScheduledExecutor();
    }

    @AfterEach
    void tearDown() {
        executor.shutdownNow();
        scheduler.shutdownNow();
    }

    private HttpRemoteQueryDispatcher dispatcher(int bufferSize) {
        return dispatcher(bufferSize, Duration.ofMinutes(5));
    }

    private HttpRemoteQueryDispatcher dispatcher(int bufferSize, Duration responseTimeout) {
        return dispatcher(bufferSize, responseTimeout, Duration.ofMinutes(5));
    }

    private HttpRemoteQueryDispatcher dispatcher(int bufferSize, Duration responseTimeout, Duration inactivityTimeout) {
        return new HttpRemoteQueryDispatcher(RestClient.builder().requestFactory(requestFactory).build(),
                                             ENDPOINT,
                                             executor,
                                             converter,
                                             bufferSize,
                                             responseTimeout,
                                             inactivityTimeout,
                                             scheduler);
    }

    private static QueryMessage query() {
        return new GenericQueryMessage(new GenericMessage("query-1", FIND_COURSE_TYPE, PAYLOAD, Map.of()), null);
    }

    private String responseEvent(String identifier) throws IOException {
        QueryDispatchResponse response = new QueryDispatchResponse(identifier,
                                                             "query-1",
                                                             RESPONSE_TYPE.toString(),
                                                             PAYLOAD,
                                                             Map.of());
        return "event: " + QueryConverter.RESPONSE_EVENT + "\ndata: "
                + objectMapper.writeValueAsString(response) + "\n\n";
    }

    private String errorEvent(QueryErrorCode code) throws IOException {
        QueryDispatchFailure error = new QueryDispatchFailure("query-1",
                                                    code,
                                                    "The course store is unavailable.",
                                                    List.of("The course store is unavailable."),
                                                    "node-b",
                                                    null,
                                                    null);
        return "event: " + QueryConverter.ERROR_EVENT + "\ndata: " + objectMapper.writeValueAsString(error) + "\n\n";
    }

    /**
     * Waits for the given {@code condition}, polling often because these tests feed a body a byte at a time.
     */
    private static void awaitUntil(BooleanSupplier condition) {
        Awaitility.await()
                  .atMost(Duration.ofSeconds(10))
                  .pollDelay(Duration.ZERO)
                  .pollInterval(Duration.ofMillis(5))
                  .until(condition::getAsBoolean);
    }

    /**
     * Drains everything currently available from the given {@code stream} into {@code target}, as a consumer keeping
     * up with the member does.
     */
    private static void drainInto(MessageStream<QueryResponseMessage> stream, List<QueryResponseMessage> target) {
        while (stream.hasNextAvailable()) {
            stream.next().ifPresent(entry -> target.add(entry.message()));
        }
    }

    /**
     * Reads the given {@code stream} until the member has finished answering, one way or the other.
     * <p>
     * A stream only notices that it completed or failed while it is being read, so waiting on it means reading it.
     */
    private static List<QueryResponseMessage> awaitAnswer(MessageStream<QueryResponseMessage> stream) {
        List<QueryResponseMessage> received = new ArrayList<>();
        awaitUntil(() -> {
            drainInto(stream, received);
            return stream.isCompleted() || stream.error().isPresent();
        });
        return received;
    }

    @Nested
    class ReadingAMembersAnswer {

        @Test
        void reportsEveryResponseTheMemberStreamed() throws IOException {
            // given a member that has already answered in full
            body.write(responseEvent("response-1") + responseEvent("response-2") + responseEvent("response-3"));
            body.end();

            // when
            MessageStream<QueryResponseMessage> responses = dispatcher(1024).dispatch(MEMBER, query());

            // then
            List<QueryResponseMessage> received = awaitAnswer(responses);
            assertThat(received).extracting(QueryResponseMessage::identifier)
                                .containsExactly("response-1", "response-2", "response-3");
            assertThat(responses.error()).isEmpty();
        }

        @Test
        void completesWithoutResponsesWhenTheMemberAnswersWithNone() {
            // given
            body.end();

            // when
            MessageStream<QueryResponseMessage> responses = dispatcher(1024).dispatch(MEMBER, query());

            // then an answer of nothing is a completed stream, not a failure
            assertThat(awaitAnswer(responses)).isEmpty();
            assertThat(responses.isCompleted()).isTrue();
            assertThat(responses.error()).isEmpty();
        }

        @Test
        void ignoresEventsOfATypeItDoesNotKnow() throws IOException {
            // given a member sending something a later version might add
            body.write(responseEvent("response-1"));
            body.write("event: something-else\ndata: {}\n\n");
            body.write(responseEvent("response-2"));
            body.end();

            // when
            MessageStream<QueryResponseMessage> responses = dispatcher(1024).dispatch(MEMBER, query());

            // then the unknown event neither failed the query nor was read as a response
            List<QueryResponseMessage> received = awaitAnswer(responses);
            assertThat(received).extracting(QueryResponseMessage::identifier)
                                .containsExactly("response-1", "response-2");
            assertThat(responses.error()).isEmpty();
        }

        @Test
        void sendsTheQueryToTheMembersQueryEndpoint() throws IOException {
            // given
            body.end();

            // when
            dispatcher(1024).dispatch(MEMBER, query());

            // then
            awaitUntil(() -> !requestFactory.requests().isEmpty());
            assertThat(requestFactory.requests().getFirst().getURI())
                    .hasToString("http://node-b:8080" + ENDPOINT);
        }
    }

    @Nested
    class Subscribing {

        private static final SubscriptionListener NO_OP = listening(new AtomicBoolean(), new AtomicBoolean());

        /**
         * A listener recording what a member reported about the subscription.
         */
        private static SubscriptionListener listening(AtomicBoolean opened, AtomicBoolean completed) {
            return new SubscriptionListener() {
                @Override
                public void opened() {
                    opened.set(true);
                }

                @Override
                public void completed() {
                    completed.set(true);
                }
            };
        }

        private String updateEvent(String identifier) throws IOException {
            QueryDispatchResponse update = new QueryDispatchResponse(identifier,
                                                                     "query-1",
                                                                     RESPONSE_TYPE.toString(),
                                                                     PAYLOAD,
                                                                     Map.of());
            return "event: " + QueryConverter.UPDATE_EVENT + "\ndata: "
                    + objectMapper.writeValueAsString(update) + "\n\n";
        }

        @Test
        void opensTheSubscriptionOnTheMembersSubscriptionEndpoint() {
            // when
            dispatcher(1024).openSubscriptionQueryUpdateStream(MEMBER, query(), 16, NO_OP);

            // then
            awaitUntil(() -> !requestFactory.requests().isEmpty());
            assertThat(requestFactory.requests().getFirst().getURI())
                    .hasToString("http://node-b:8080" + ENDPOINT + SpringCloudQueryController.SUBSCRIPTION_PATH);
        }

        @Test
        void reportsTheSubscriptionOpenOnlyOnceTheMemberHasAnswered() {
            // given
            AtomicBoolean opened = new AtomicBoolean();
            requestFactory.holdingResponses();

            // when the member has not answered yet
            dispatcher(1024).openSubscriptionQueryUpdateStream(MEMBER,
                                                               query(),
                                                               16,
                                                               listening(opened, new AtomicBoolean()));

            // then the initial result must not be asked for, as this member could still miss an update
            awaitUntil(() -> !requestFactory.requests().isEmpty());
            assertThat(opened).isFalse();

            // when the member answers, which it does only after registering the subscription
            requestFactory.releaseResponses();

            // then
            awaitUntil(opened::get);
            assertThat(opened).isTrue();
        }

        @Test
        void leavesTheSubscriptionUnopenedWhenTheMemberCannotBeReached() {
            // given
            AtomicBoolean opened = new AtomicBoolean();
            requestFactory.respondingWithStatus(HttpStatus.SERVICE_UNAVAILABLE);

            // when
            MessageStream<QueryResponseMessage> updates = dispatcher(1024)
                    .openSubscriptionQueryUpdateStream(MEMBER, query(), 16, listening(opened, new AtomicBoolean()));

            // then the failure is on the stream, and nothing is told the subscription is in place
            awaitUntil(() -> {
                drainInto(updates, new ArrayList<>());
                return updates.error().isPresent();
            });
            assertThat(opened).isFalse();
        }

        @Test
        void carriesNoFailureWhileTheMemberIsSimplyQuiet() {
            // when — the member answers, without emitting anything yet
            MessageStream<QueryResponseMessage> updates =
                    dispatcher(1024).openSubscriptionQueryUpdateStream(MEMBER, query(), 16, NO_OP);

            // then a subscription with nothing to say is behaving correctly, not failing
            assertThat(updates.error()).isEmpty();
            assertThat(updates.isCompleted()).isFalse();
        }

        @Test
        void readsTheUpdatesTheMemberEmits() throws IOException {
            // given
            MessageStream<QueryResponseMessage> updates =
                    dispatcher(1024).openSubscriptionQueryUpdateStream(MEMBER, query(), 16, NO_OP);

            // when
            body.write(updateEvent("update-1"));
            body.write(updateEvent("update-2"));

            // then
            List<QueryResponseMessage> received = new ArrayList<>();
            awaitUntil(() -> {
                drainInto(updates, received);
                return received.size() == 2;
            });
            assertThat(received).extracting(QueryResponseMessage::identifier)
                                .containsExactly("update-1", "update-2");
        }

        @Test
        void readsTheUpdatesTheMemberEmitsAsSubscriptionQueryUpdates() throws IOException {
            // given
            MessageStream<QueryResponseMessage> updates =
                    dispatcher(1024).openSubscriptionQueryUpdateStream(MEMBER, query(), 16, NO_OP);

            // when
            body.write(updateEvent("update-1"));

            // then an update is told apart from an initial result by its type, which is what a subscriber to the
            // updates alone filters on
            List<QueryResponseMessage> received = new ArrayList<>();
            awaitUntil(() -> {
                drainInto(updates, received);
                return !received.isEmpty();
            });
            assertThat(received).singleElement().satisfies(update -> {
                assertThat(update).isInstanceOf(SubscriptionQueryUpdateMessage.class);
                assertThat(update.identifier()).isEqualTo("update-1");
                assertThat(update.type()).isEqualTo(RESPONSE_TYPE);
                assertThat(update.payloadAs(String.class)).isEqualTo(PAYLOAD);
            });
        }

        @Test
        void reportsTheSubscriptionOverWhenTheMemberSaysSo() throws IOException {
            // given
            AtomicBoolean completed = new AtomicBoolean();
            MessageStream<QueryResponseMessage> updates = dispatcher(1024)
                    .openSubscriptionQueryUpdateStream(MEMBER, query(), 16,
                                                       listening(new AtomicBoolean(), completed));

            // when
            body.write("event: " + QueryConverter.COMPLETE_EVENT + "\ndata: query-1\n\n");
            body.end();

            // then the subscription is over, which is more than this member having stopped answering
            awaitUntil(() -> {
                drainInto(updates, new ArrayList<>());
                return updates.isCompleted();
            });
            assertThat(completed).isTrue();
            assertThat(updates.error()).isEmpty();
        }

        @Test
        void endsOnlyThisMembersPartWhenTheMemberSaysItIsLeaving() throws IOException {
            // given
            AtomicBoolean completed = new AtomicBoolean();
            MessageStream<QueryResponseMessage> updates = dispatcher(1024)
                    .openSubscriptionQueryUpdateStream(MEMBER, query(), 16,
                                                       listening(new AtomicBoolean(), completed));

            // when the member announces it is leaving, as one shutting down cleanly does
            body.write("event: " + QueryConverter.LEAVING_EVENT + "\ndata: query-1\n\n");
            body.end();

            // then this member's part is over, and nothing claims the subscription itself is
            awaitUntil(() -> {
                drainInto(updates, new ArrayList<>());
                return updates.isCompleted();
            });
            assertThat(updates.error()).isEmpty();
            assertThat(completed).isFalse();
        }

        @Test
        void failsTheSubscriptionWhenTheMemberEndsItWithoutSayingWhy() throws IOException {
            // given
            AtomicBoolean completed = new AtomicBoolean();
            MessageStream<QueryResponseMessage> updates = dispatcher(1024)
                    .openSubscriptionQueryUpdateStream(MEMBER, query(), 16,
                                                       listening(new AtomicBoolean(), completed));

            // when the stream ends without the member completing the subscription or announcing it is leaving
            body.end();

            // then the stream may have been cut short, so whatever the member emitted in the meantime may be lost
            awaitUntil(() -> {
                drainInto(updates, new ArrayList<>());
                return updates.error().isPresent();
            });
            assertThat(updates.error().orElseThrow()).isInstanceOf(QueryDispatchException.class)
                                                     .hasMessageContaining("Lost the subscription");
            assertThat(completed).isFalse();
        }

        @Test
        void endsTheSubscriptionWithTheFailureTheMemberReports() throws IOException {
            // given
            MessageStream<QueryResponseMessage> updates =
                    dispatcher(1024).openSubscriptionQueryUpdateStream(MEMBER, query(), 16, NO_OP);

            // when
            body.write(errorEvent(QueryErrorCode.QUERY_EXECUTION_ERROR));
            body.end();

            // then
            awaitUntil(() -> {
                drainInto(updates, new ArrayList<>());
                return updates.error().isPresent();
            });
            assertThat(updates.error().orElseThrow()).hasMessageContaining("The course store is unavailable.");
        }

        @Test
        void stopsReadingTheMemberWhenTheSubscriptionIsReleased() {
            // given
            MessageStream<QueryResponseMessage> updates =
                    dispatcher(1024).openSubscriptionQueryUpdateStream(MEMBER, query(), 16, NO_OP);

            // when
            updates.close();

            // then the member is not left emitting into a subscription nobody holds
            awaitUntil(body::closed);
            assertThat(body.closed()).isTrue();
        }

        @Test
        void givesUpOnAMemberThatSaysNothingAtAll() {
            // given a subscription whose member stops sending even a keep-alive
            MessageStream<QueryResponseMessage> updates =
                    dispatcher(1024, Duration.ofMinutes(5), Duration.ofMillis(100))
                            .openSubscriptionQueryUpdateStream(MEMBER, query(), 16, NO_OP);

            // then silence is how a member that disappeared without the socket reporting it looks
            awaitUntil(() -> {
                drainInto(updates, new ArrayList<>());
                return updates.error().isPresent();
            });
            assertThat(updates.error().orElseThrow()).hasMessageContaining("sent nothing");
        }

        @Test
        void failsWhenTheMemberHasNoEndpoint() {
            // given a member with nothing to reach it at, which only this application's own ever is
            Member unreachable = new Member("UNIVERSITY[local]", null, true);

            // when / then
            MessageStream<QueryResponseMessage> updates =
                    dispatcher(1024).openSubscriptionQueryUpdateStream(unreachable, query(), 16, NO_OP);
            assertThat(updates.error()).containsInstanceOf(QueryDispatchException.class);
        }

        @Test
        void rejectsANonPositiveUpdateBufferSize() {
            assertThatThrownBy(() -> dispatcher(1024).openSubscriptionQueryUpdateStream(MEMBER, query(), 0, NO_OP))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("update buffer size");
        }
    }

    @Nested
    class ReadingFromAMemberOnANewerVersion {

        @Test
        void readsAResponseCarryingAFieldItDoesNotKnow() throws IOException {
            // given a response as a later version of the connector might write it
            body.write("""
                               event: response
                               data: {"identifier":"response-1","requestIdentifier":"query-1",\
                               "type":"university.Course#1.0.0","payload":null,"metadata":{},\
                               "somethingAddedLater":"whatever"}

                               """);
            body.end();

            // when
            MessageStream<QueryResponseMessage> responses = dispatcher(1024).dispatch(MEMBER, query());

            // then a field this member does not know about is no reason to fail the query
            assertThat(awaitAnswer(responses)).extracting(QueryResponseMessage::identifier)
                                              .containsExactly("response-1");
            assertThat(responses.error()).isEmpty();
        }

        @Test
        void reportsAFailureWhoseCodeItDoesNotKnow() throws IOException {
            // given a failure reported with an error code a later version of the connector might add
            body.write("""
                               event: error
                               data: {"requestIdentifier":"query-1","errorCode":"SOMETHING_ADDED_LATER",\
                               "errorMessage":"The course store is unavailable.","errorDetails":[],\
                               "errorOrigin":"node-b"}

                               """);
            body.end();

            // when
            MessageStream<QueryResponseMessage> responses = dispatcher(1024).dispatch(MEMBER, query());

            // then the failure is reported as what it is rather than lost behind a failure to read it
            awaitAnswer(responses);
            assertThat(responses.error()).isPresent();
            assertThat(responses.error().orElseThrow())
                    .hasMessageContaining("The course store is unavailable.");
        }
    }

    @Nested
    class WhenTheMemberOutpacesThisApplication {

        @Test
        void failsTheQueryRatherThanBufferingWithoutLimit() throws IOException {
            // given a member answering far faster than a consumer that never reads
            for (int i = 1; i <= 20; i++) {
                body.write(responseEvent("response-" + i));
            }
            body.end();

            // when the responses are left unread, so the buffer fills
            MessageStream<QueryResponseMessage> responses = dispatcher(2).dispatch(MEMBER, query());

            // then the query fails instead of the buffer growing until memory runs out
            awaitAnswer(responses);
            assertThat(responses.error()).isPresent();
            assertThat(responses.error().orElseThrow()).hasMessageContaining("Could not read the responses");
            assertThat(ExceptionUtils.findException(responses.error().orElseThrow(), IllegalStateException.class))
                    .get()
                    .satisfies(cause -> assertThat(cause).hasMessageContaining("raise the buffer size"));
        }

        @Test
        void stopsReadingTheMemberOnceTheBufferOverflowed() throws IOException {
            // given
            for (int i = 1; i <= 20; i++) {
                body.write(responseEvent("response-" + i));
            }

            // when
            MessageStream<QueryResponseMessage> responses = dispatcher(2).dispatch(MEMBER, query());

            // then the member is not left streaming into a query that already failed
            awaitUntil(body::closed);
            assertThat(body.closed()).isTrue();
        }

        @Test
        void carriesOnWhenTheConsumerKeepsUp() throws IOException {
            // given the same small buffer, and a consumer draining as responses arrive
            List<QueryResponseMessage> received = new ArrayList<>();
            MessageStream<QueryResponseMessage> responses = dispatcher(2).dispatch(MEMBER, query());
            responses.setCallback(() -> drainInto(responses, received));

            // when more responses than the buffer holds are streamed
            for (int i = 1; i <= 20; i++) {
                body.write(responseEvent("response-" + i));
            }
            body.end();

            // then a bounded buffer is no limit on how many responses a query may have
            awaitUntil(() -> received.size() == 20);
            drainInto(responses, received);
            assertThat(received).hasSize(20);
            assertThat(responses.error()).isEmpty();
        }
    }

    @Nested
    class WhenThisApplicationStopsReading {

        @Test
        void closesTheStreamItWasReadingFromTheMember() throws IOException {
            // given a member part-way through answering
            body.write(responseEvent("response-1"));
            MessageStream<QueryResponseMessage> responses = dispatcher(1024).dispatch(MEMBER, query());
            awaitUntil(responses::hasNextAvailable);

            // when the consumer abandons the query
            responses.close();

            // then the member is not left writing responses nobody reads
            awaitUntil(body::closed);
            assertThat(body.closed()).isTrue();
        }

        @Test
        void doesNotReportItsOwnCancellationAsAFailure() throws IOException {
            // given
            body.write(responseEvent("response-1"));
            MessageStream<QueryResponseMessage> responses = dispatcher(1024).dispatch(MEMBER, query());
            awaitUntil(responses::hasNextAvailable);

            // when
            responses.close();
            awaitUntil(body::closed);

            // then the read ending is the outcome the consumer asked for, not something that went wrong
            assertThat(responses.error()).isEmpty();
        }
    }

    @Nested
    class WhenSomethingFails {

        @Test
        void reportsAMemberWithNoEndpointToSendTo() {
            // given a member discovery reported without a usable address
            Member unreachable = Member.localMember("node-c");

            // when
            MessageStream<QueryResponseMessage> responses = dispatcher(1024).dispatch(unreachable, query());

            // then
            assertThat(responses.error()).isPresent();
            assertThat(responses.error().orElseThrow()).hasMessageContaining("no endpoint");
        }

        @Test
        void reportsAMemberThatCannotBeReached() {
            // given
            requestFactory.failingWith(new IOException("Connection refused."));

            // when
            MessageStream<QueryResponseMessage> responses = dispatcher(1024).dispatch(MEMBER, query());

            // then
            awaitAnswer(responses);
            assertThat(responses.error().orElseThrow()).hasMessageContaining("Could not read the responses");
        }

        @Test
        void reportsAMemberRejectingTheQuery() {
            // given a member answering with a status rather than a stream
            requestFactory.respondingWithStatus(HttpStatus.INTERNAL_SERVER_ERROR);

            // when
            MessageStream<QueryResponseMessage> responses = dispatcher(1024).dispatch(MEMBER, query());

            // then
            awaitAnswer(responses);
            assertThat(responses.error().orElseThrow()).hasMessageContaining("Could not read the responses");
        }

        @Test
        void reportsTheFailureTheMemberSentAsTheFailureItWas() throws IOException {
            // given a member reporting that it has no handler
            body.write(errorEvent(QueryErrorCode.NO_HANDLER_FOR_QUERY));
            body.end();

            // when
            MessageStream<QueryResponseMessage> responses = dispatcher(1024).dispatch(MEMBER, query());

            // then the dispatching member sees a failure it can act on, not a generic read failure
            awaitAnswer(responses);
            assertThat(responses.error()).containsInstanceOf(NoHandlerForQueryException.class);
            assertThat(responses.error().orElseThrow()).hasMessageContaining("node-b");
        }

        @Test
        void reportsAFailureWorthNoRetryAsSuch() throws IOException {
            // given
            body.write(errorEvent(QueryErrorCode.QUERY_EXECUTION_NON_TRANSIENT_ERROR));
            body.end();

            // when
            MessageStream<QueryResponseMessage> responses = dispatcher(1024).dispatch(MEMBER, query());

            // then a caller deciding whether to retry can tell this one apart
            awaitAnswer(responses);
            assertThat(ExceptionUtils.isExplicitlyNonTransient(responses.error().orElseThrow())).isTrue();
        }

        @Test
        void reportsResponsesItAlreadyReadBeforeTheFailure() throws IOException {
            // given a member that answered twice before failing
            body.write(responseEvent("response-1"));
            body.write(responseEvent("response-2"));
            body.write(errorEvent(QueryErrorCode.QUERY_EXECUTION_ERROR));
            body.end();

            // when
            MessageStream<QueryResponseMessage> responses = dispatcher(1024).dispatch(MEMBER, query());

            // then the responses that did arrive are not discarded along with the failure
            List<QueryResponseMessage> received = awaitAnswer(responses);
            assertThat(received).extracting(QueryResponseMessage::identifier)
                                .containsExactly("response-1", "response-2");
        }

        @Test
        void reportsAnEventItCannotRead() {
            // given a member sending something that is not a response
            body.write("event: " + QueryConverter.RESPONSE_EVENT + "\ndata: not json\n\n");
            body.end();

            // when
            MessageStream<QueryResponseMessage> responses = dispatcher(1024).dispatch(MEMBER, query());

            // then
            awaitAnswer(responses);
            assertThat(responses.error().orElseThrow()).hasMessageContaining("Could not read the responses");
        }
    }

    @Nested
    class WhenTheMemberStopsAnswering {

        @Test
        void failsTheQueryOnceItsResponsesAreNoLongerWaitedFor() throws IOException {
            // given a member that answered once and then went silent without closing the stream
            body.write(responseEvent("response-1"));

            // when
            MessageStream<QueryResponseMessage> responses =
                    dispatcher(1024, Duration.ofMillis(200)).dispatch(MEMBER, query());
            List<QueryResponseMessage> received = awaitAnswer(responses);

            // then the query ends rather than waiting on a socket that will never report anything, and it ends as a
            // failure to reach the member so the connector takes it out of the ring
            assertThat(responses.error()).containsInstanceOf(QueryDispatchException.class);
            assertThat(responses.error().orElseThrow()).hasMessageContaining("did not answer query");
            // and what the member did answer before going silent is kept
            assertThat(received).extracting(QueryResponseMessage::identifier).containsExactly("response-1");
        }

        @Test
        void stopsReadingTheMemberItGaveUpOn() throws IOException {
            // given
            body.write(responseEvent("response-1"));

            // when
            awaitAnswer(dispatcher(1024, Duration.ofMillis(200)).dispatch(MEMBER, query()));

            // then the member is not left streaming into a query that has already failed
            awaitUntil(body::closed);
            assertThat(body.closed()).isTrue();
        }

        @Test
        void leavesAQueryAnsweredInTimeAlone() throws IOException {
            // given a member that answers well within the deadline
            body.write(responseEvent("response-1"));
            body.end();

            // when
            MessageStream<QueryResponseMessage> responses =
                    dispatcher(1024, Duration.ofSeconds(30)).dispatch(MEMBER, query());
            List<QueryResponseMessage> received = awaitAnswer(responses);

            // then
            assertThat(responses.error()).isEmpty();
            assertThat(received).extracting(QueryResponseMessage::identifier).containsExactly("response-1");
        }
    }

    @Nested
    class Validation {

        @Test
        void rejectsADeadlineThatLeavesNoTimeToAnswer() {
            assertThatThrownBy(() -> dispatcher(1024, Duration.ZERO))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("response timeout");
        }

        @Test
        void rejectsABufferThatCannotHoldAResponse() {
            assertThatThrownBy(() -> dispatcher(0))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("buffer size");
        }

        @Test
        void rejectsAMissingMember() {
            assertThatThrownBy(() -> dispatcher(1024).dispatch(null, query()))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        void rejectsAMissingQuery() {
            assertThatThrownBy(() -> dispatcher(1024).dispatch(MEMBER, null))
                    .isInstanceOf(NullPointerException.class);
        }
    }

    /**
     * A response body fed from the test, a byte at a time, so that a read can be observed while it is still going.
     */
    private static class SseBody extends InputStream {

        private static final int END_OF_STREAM = -1;

        private final BlockingQueue<Integer> bytes = new LinkedBlockingQueue<>();

        private volatile boolean closed;

        private void write(String text) {
            for (byte b : text.getBytes(UTF_8)) {
                bytes.add(b & 0xFF);
            }
        }

        private void end() {
            bytes.add(END_OF_STREAM);
        }

        private boolean closed() {
            return closed;
        }

        @Override
        public int read() throws IOException {
            if (closed) {
                throw new IOException("Stream closed.");
            }
            Integer next;
            try {
                next = bytes.poll(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while reading.", e);
            }
            if (closed) {
                // Closing is how a caller stops a read it no longer needs, and a real socket fails the same way.
                throw new IOException("Stream closed.");
            }
            if (next == null) {
                throw new IOException("Timed out waiting for the member to write.");
            }
            return next;
        }

        @Override
        public int read(byte[] target, int offset, int length) throws IOException {
            // One byte at a time, so a blocked read never waits for a buffer to fill before reporting what arrived.
            int next = read();
            if (next == END_OF_STREAM) {
                return END_OF_STREAM;
            }
            target[offset] = (byte) next;
            return 1;
        }

        @Override
        public void close() {
            closed = true;
            // Unblocks a read waiting on the queue, which then sees the stream closed.
            bytes.add(0);
        }
    }

    /**
     * Answers every request with the one response body, or with the failure or status it was told to.
     */
    private static class StubRequestFactory implements ClientHttpRequestFactory {

        private final SseBody body;
        private final List<MockClientHttpRequest> requests = new ArrayList<>();

        private IOException failure;
        private HttpStatusCode status = HttpStatus.OK;
        private volatile CountDownLatch responseGate;

        private StubRequestFactory(SseBody body) {
            this.body = body;
        }

        private void failingWith(IOException failure) {
            this.failure = failure;
        }

        private void respondingWithStatus(HttpStatusCode status) {
            this.status = status;
        }

        /**
         * Withholds every response until {@link #releaseResponses()}, as a member that has not answered yet does.
         */
        private void holdingResponses() {
            this.responseGate = new CountDownLatch(1);
        }

        private void releaseResponses() {
            CountDownLatch gate = responseGate;
            if (gate != null) {
                gate.countDown();
            }
        }

        private List<MockClientHttpRequest> requests() {
            return List.copyOf(requests);
        }

        @Override
        public ClientHttpRequest createRequest(URI uri, HttpMethod httpMethod) {
            MockClientHttpRequest request = new MockClientHttpRequest(httpMethod, uri) {
                @Override
                protected ClientHttpResponse executeInternal() throws IOException {
                    if (failure != null) {
                        throw failure;
                    }
                    CountDownLatch gate = responseGate;
                    if (gate != null) {
                        try {
                            gate.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new IOException("Interrupted while withholding the response.", e);
                        }
                    }
                    return new StubResponse(status, body);
                }
            };
            requests.add(request);
            return request;
        }
    }

    /**
     * A response whose body is the stream the test feeds.
     */
    private record StubResponse(HttpStatusCode statusCode, SseBody body) implements ClientHttpResponse {

        @Override
        public HttpStatusCode getStatusCode() {
            return statusCode;
        }

        @Override
        public String getStatusText() {
            return statusCode.toString();
        }

        @Override
        public HttpHeaders getHeaders() {
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.TEXT_EVENT_STREAM);
            return headers;
        }

        @Override
        public InputStream getBody() {
            return body;
        }

        @Override
        public void close() {
            body.close();
        }
    }
}
