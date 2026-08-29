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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
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
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;

/**
 * Tests how a query's responses are framed on the wire, by answering a query through the endpoint and reading back
 * the bytes that came out.
 * <p>
 * The framing is the thing under test, and it is what the two halves of the transport agree on. A query is answered
 * with a stream of responses, and the member reading them can only tell one response from the next if each is written
 * as an event of its own; responses run together into one event would arrive as a single answer that no longer
 * matches what the handler produced. Everything from {@link IncomingQueryGateway} through {@link SseQueryResponseSink}
 * to {@link ServerSentEventReader} takes part, so this pins the whole agreement rather than either end of it.
 *
 * @author Allard Buijze
 */
class SseQueryResponseSinkFramingTest {

    private static final MessageType FIND_COURSE_TYPE = new MessageType("university.FindCourse", "1.0.0");
    private static final MessageType RESPONSE_TYPE = new MessageType("university.Course", "1.0.0");
    private static final String PAYLOAD = "{\"id\":\"course-1\"}";

    // Reads back what the container wrote, standing in for the member on the other end of the stream.
    private final ObjectMapper objectMapper =
            JsonMapper.builder().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES).build();

    private IncomingQueryGateway gateway;
    private MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        gateway = new IncomingQueryGateway(() -> "node-b", null);
        mockMvc = MockMvcBuilders
                .standaloneSetup(new SpringCloudQueryController(gateway, Duration.ofSeconds(30)))
                .build();
    }

    private static QueryDispatchRequest request() {
        return new QueryDispatchRequest("query-1",
                                        FIND_COURSE_TYPE.toString(),
                                        PAYLOAD,
                                        Map.of(),
                                        null);
    }

    private static QueryResponseMessage response(String identifier) {
        return response(identifier, Map.of());
    }

    private static QueryResponseMessage response(String identifier, Map<String, String> metadata) {
        return new GenericQueryResponseMessage(new GenericMessage(identifier, RESPONSE_TYPE, PAYLOAD, metadata));
    }

    /**
     * Sends the query to the endpoint, as another member would.
     */
    private MvcResult dispatch() throws Exception {
        return mockMvc.perform(post(SpringCloudQueryController.DEFAULT_QUERY_ENDPOINT)
                                       .contentType(MediaType.APPLICATION_JSON)
                                       .content(objectMapper.writeValueAsBytes(request())))
                      .andReturn();
    }

    /**
     * Reads what went onto the wire the way the member that asked would.
     */
    private List<ServerSentEvent> wireOf(MvcResult result) throws IOException {
        List<ServerSentEvent> events = new ArrayList<>();
        ServerSentEventReader.read(
                new ByteArrayInputStream(result.getResponse().getContentAsString().getBytes(UTF_8)), events::add
        );
        return events;
    }

    private QueryDispatchResponse parse(ServerSentEvent event) throws IOException {
        return objectMapper.readValue(event.data(), QueryDispatchResponse.class);
    }

    @Nested
    class FramingAStreamOfResponses {

        @Test
        void writesOneEventPerResponseMessage() throws Exception {
            // given a handler answering with a stream of three responses
            gateway.bind(new RecordingQueryHandler()
                                 .answeringWith(response("response-1"),
                                                response("response-2"),
                                                response("response-3")));

            // when
            List<ServerSentEvent> wire = wireOf(dispatch());

            // then three responses came back as three events, not as one run-together answer
            assertThat(wire).hasSize(3);
            assertThat(wire).extracting(ServerSentEvent::event).containsOnly(QueryConverter.RESPONSE_EVENT);
        }

        @Test
        void writesEachResponseWholeAndInOrder() throws Exception {
            // given
            gateway.bind(new RecordingQueryHandler()
                                 .answeringWith(response("response-1"), response("response-2")));

            // when
            List<ServerSentEvent> wire = wireOf(dispatch());

            // then each event carries exactly one response, in the order the handler produced them
            List<QueryDispatchResponse> responses = new ArrayList<>();
            for (ServerSentEvent event : wire) {
                responses.add(parse(event));
            }
            assertThat(responses).extracting(QueryDispatchResponse::identifier)
                                 .containsExactly("response-1", "response-2");
            assertThat(responses).extracting(QueryDispatchResponse::requestIdentifier).containsOnly("query-1");
        }

        @Test
        void writesOneEventForASingleResponse() throws Exception {
            // given a query answered once, which is the common case
            gateway.bind(new RecordingQueryHandler().answeringWith(response("response-1")));

            // when
            List<ServerSentEvent> wire = wireOf(dispatch());

            // then
            assertThat(wire).hasSize(1);
            assertThat(parse(wire.getFirst()).identifier()).isEqualTo("response-1");
        }

        @Test
        void writesNothingForAQueryWithNoAnswer() throws Exception {
            // given a handler producing no responses at all
            gateway.bind(new RecordingQueryHandler());

            // when
            List<ServerSentEvent> wire = wireOf(dispatch());

            // then an answered-with-nothing query is an empty stream, not an event carrying nothing
            assertThat(wire).isEmpty();
        }

        @Test
        void keepsAResponseWhoseContentContainsNewlinesInOneEvent() throws Exception {
            // given metadata carrying the newlines the protocol frames events on
            Map<String, String> metadata = new LinkedHashMap<>();
            metadata.put("note", "line one\nline two\n\nline four");
            gateway.bind(new RecordingQueryHandler().answeringWith(response("response-1", metadata)));

            // when
            List<ServerSentEvent> wire = wireOf(dispatch());

            // then the content did not split one response across several events
            assertThat(wire).hasSize(1);
            assertThat(parse(wire.getFirst()).metadata()).containsEntry("note", "line one\nline two\n\nline four");
        }
    }

    @Nested
    class FramingResponsesThatArriveLater {

        @Test
        void writesOneEventPerResponseAsEachArrives() throws Exception {
            // given a handler answering asynchronously, as one reading from a store does
            QueueMessageStream<QueryResponseMessage> responses = new QueueMessageStream<>();
            gateway.bind(new RecordingQueryHandler() {
                @Override
                public MessageStream<QueryResponseMessage> query(QueryMessage query) {
                    return responses;
                }
            });

            // when the responses are produced after the handler returned
            MvcResult result = dispatch();
            assertThat(wireOf(result)).isEmpty();
            responses.offer(response("response-1"), Context.empty());
            responses.offer(response("response-2"), Context.empty());
            responses.offer(response("response-3"), Context.empty());
            responses.seal();

            // then each arrival was framed on its own rather than batched into one event at the end
            List<ServerSentEvent> wire = wireOf(result);
            assertThat(wire).hasSize(3);
            assertThat(wire).extracting(ServerSentEvent::event).containsOnly(QueryConverter.RESPONSE_EVENT);
        }
    }

    @Nested
    class FramingTheOutcome {

        @Test
        void writesTheFailureAsAnEventOfItsOwn() throws Exception {
            // given a query answered once before its handler failed
            QueueMessageStream<QueryResponseMessage> responses = new QueueMessageStream<>();
            gateway.bind(new RecordingQueryHandler() {
                @Override
                public MessageStream<QueryResponseMessage> query(QueryMessage query) {
                    return responses;
                }
            });

            // when
            MvcResult result = dispatch();
            responses.offer(response("response-1"), Context.empty());
            responses.sealExceptionally(new QueryExecutionException("The course store is unavailable.", null));

            // then the response and the failure are separate events, told apart by their type
            List<ServerSentEvent> wire = wireOf(result);
            assertThat(wire).extracting(ServerSentEvent::event)
                            .containsExactly(QueryConverter.RESPONSE_EVENT, QueryConverter.ERROR_EVENT);
        }

        @Test
        void writesNoErrorEventForAQueryAnsweredInFull() throws Exception {
            // given
            gateway.bind(new RecordingQueryHandler().answeringWith(response("response-1")));

            // when
            List<ServerSentEvent> wire = wireOf(dispatch());

            // then a stream that ends without an error event is how success is reported
            assertThat(wire).extracting(ServerSentEvent::event).containsExactly(QueryConverter.RESPONSE_EVENT);
        }

        @Test
        void writesTheFailureAsSomethingTheAskingMemberCanActOn() throws Exception {
            // given
            gateway.bind(new RecordingQueryHandler()
                                 .failingWith(new QueryExecutionException("The course store is unavailable.", null)));

            // when
            List<ServerSentEvent> wire = wireOf(dispatch());

            // then
            assertThat(wire).hasSize(1);
            QueryDispatchFailure error = objectMapper.readValue(wire.getFirst().data(), QueryDispatchFailure.class);
            assertThat(error.errorCode()).isEqualTo(QueryErrorCode.QUERY_EXECUTION_ERROR);
            assertThat(error.errorOrigin()).isEqualTo("node-b");
            assertThat(error.requestIdentifier()).isEqualTo("query-1");
        }

        @Test
        void writesNoHandlerAsAnErrorEventWhenTheMemberIsStillStartingUp() throws Exception {
            // given a member whose handler has not been registered on the connector yet

            // when
            List<ServerSentEvent> wire = wireOf(dispatch());

            // then
            assertThat(wire).hasSize(1);
            assertThat(objectMapper.readValue(wire.getFirst().data(), QueryDispatchFailure.class).errorCode())
                    .isEqualTo(QueryErrorCode.NO_HANDLER_FOR_QUERY);
        }
    }

    @Nested
    class Validation {

        @Test
        void rejectsAMissingEmitter() {
            assertThatThrownBy(() -> new SseQueryResponseSink(null))
                    .isInstanceOf(NullPointerException.class);
        }
    }
}
