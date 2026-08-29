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

import io.axoniq.framework.springcloud.routing.Member;
import io.axoniq.framework.springcloud.util.StubClientHttpRequestFactory;
import org.axonframework.conversion.ConversionException;
import org.axonframework.messaging.commandhandling.CommandDispatchException;
import org.axonframework.messaging.commandhandling.CommandExecutionException;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.commandhandling.CommandResultMessage;
import org.axonframework.messaging.commandhandling.GenericCommandMessage;
import org.axonframework.messaging.commandhandling.NoHandlerForCommandException;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.MessageType;
import org.junit.jupiter.api.*;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.web.client.RestClient;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests how {@link HttpRemoteCommandDispatcher} sends a command to another member and reads back the outcome.
 *
 * @author Allard Buijze
 */
class HttpRemoteCommandDispatcherTest {

    private static final MessageType COMMAND_TYPE = new MessageType("university.CreateCourse", "1.0.0");
    private static final String PAYLOAD = "{\"name\":\"Axon 5\"}";
    private static final Member REMOTE_MEMBER =
            new Member("UNIVERSITY[http://node-b:8080]", URI.create("http://node-b:8080"), false);

    private StubClientHttpRequestFactory requestFactory;
    private HttpRemoteCommandDispatcher testSubject;

    @BeforeEach
    void setUp() {
        requestFactory = new StubClientHttpRequestFactory();
        RestClient restClient = RestClient.builder().requestFactory(requestFactory).build();
        // A same-thread executor keeps these tests deterministic; production uses virtual threads.
        Executor directExecutor = Runnable::run;
        testSubject = new HttpRemoteCommandDispatcher(restClient,
                                                     SpringCloudCommandController.DEFAULT_COMMAND_ENDPOINT,
                                                     directExecutor,
                                                     null);
    }

    /**
     * Quotes the given {@code text} as a JSON string, the way the answering member writes a payload into its reply.
     */
    private static String asJsonString(String text) {
        return '"' + text.replace("\\", "\\\\").replace("\"", "\\\"") + '"';
    }

    private static CommandMessage command() {
        return new GenericCommandMessage(
                new GenericMessage("command-1", COMMAND_TYPE, PAYLOAD, Map.of()), "course-1", 5
        );
    }

    @Nested
    class Sending {

        @Test
        void postsToTheCommandEndpointOnTheMembersUri() {
            // given
            requestFactory.respondingWith(
                    "{\"identifier\":\"reply-1\",\"requestIdentifier\":\"command-1\"}", null);

            // when
            testSubject.dispatch(REMOTE_MEMBER, command()).join();

            // then
            assertThat(requestFactory.lastRequest().getMethod()).isEqualTo(HttpMethod.POST);
            assertThat(requestFactory.lastRequest().getURI())
                    .hasToString("http://node-b:8080" + SpringCloudCommandController.DEFAULT_COMMAND_ENDPOINT);
        }

        @Test
        void carriesTheCommandInTheRequestBody() {
            // given
            requestFactory.respondingWith(
                    "{\"identifier\":\"reply-1\",\"requestIdentifier\":\"command-1\"}", null);

            // when
            testSubject.dispatch(REMOTE_MEMBER, command()).join();

            // then
            String body = requestFactory.lastRequest().getBodyAsString();
            assertThat(body).contains("\"identifier\":\"command-1\"")
                            .contains("\"type\":\"university.CreateCourse#1.0.0\"")
                            .contains("\"routingKey\":\"course-1\"")
                            .contains("\"priority\":5");
        }

        @Test
        void completesWithoutAResultWhenTheMemberReturnedNone() {
            // given
            requestFactory.respondingWith(
                    "{\"identifier\":\"reply-1\",\"requestIdentifier\":\"command-1\"}", null);

            // when
            CommandResultMessage result = testSubject.dispatch(REMOTE_MEMBER, command()).join();

            // then
            assertThat(result).isNull();
        }

        @Test
        void completesWithTheResultTheMemberReturned() {
            // given
            requestFactory.respondingWith(
                    "{\"identifier\":\"reply-1\",\"requestIdentifier\":\"command-1\","
                            + "\"type\":\"university.CourseId#1.0.0\",\"payload\":" + asJsonString(PAYLOAD) + "}",
                    null);

            // when
            CommandResultMessage result = testSubject.dispatch(REMOTE_MEMBER, command()).join();

            // then
            assertThat(result).isNotNull();
            assertThat(result.type()).isEqualTo(new MessageType("university.CourseId", "1.0.0"));
            assertThat(result.payload()).isEqualTo(PAYLOAD);
        }
    }

    @Nested
    class WhenHandlingFailedOnTheOtherMember {

        @Test
        void raisesTheFailureTheMemberReported() {
            // given
            requestFactory.respondingWith(
                    "{\"identifier\":\"reply-1\",\"requestIdentifier\":\"command-1\","
                            + "\"errorCode\":\"COMMAND_EXECUTION_ERROR\",\"errorMessage\":\"course is full\","
                            + "\"errorOrigin\":\"node-b\"}", null);

            // when / then — a failed handler must not read as an unreachable member
            assertThatThrownBy(() -> testSubject.dispatch(REMOTE_MEMBER, command()).join())
                    .isInstanceOf(CompletionException.class)
                    .hasCauseInstanceOf(CommandExecutionException.class)
                    .cause()
                    .isNotInstanceOf(CommandDispatchException.class)
                    .hasMessageContaining("course is full");
        }

        @Test
        void raisesAMissingHandlerAsSuch() {
            // given
            requestFactory.respondingWith(
                    "{\"identifier\":\"reply-1\",\"requestIdentifier\":\"command-1\","
                            + "\"errorCode\":\"NO_HANDLER_FOR_COMMAND\",\"errorMessage\":\"still starting up\"}",
                    null);

            // when / then
            assertThatThrownBy(() -> testSubject.dispatch(REMOTE_MEMBER, command()).join())
                    .hasCauseInstanceOf(NoHandlerForCommandException.class);
        }
    }

    @Nested
    class WhenTheMemberCouldNotBeReached {

        @Test
        void failsWithADispatchFailureOnAConnectionError() {
            // given
            requestFactory.failingToConnect();

            // when / then — the connector relies on this to decide the member is unreachable
            assertThatThrownBy(() -> testSubject.dispatch(REMOTE_MEMBER, command()).join())
                    .hasCauseInstanceOf(CommandDispatchException.class)
                    .hasMessageContaining("Could not send command");
        }

        @Test
        void failsWithADispatchFailureOnAnErrorStatus() {
            // given
            requestFactory.respondingWithStatus(HttpStatus.SERVICE_UNAVAILABLE);

            // when / then
            assertThatThrownBy(() -> testSubject.dispatch(REMOTE_MEMBER, command()).join())
                    .hasCauseInstanceOf(CommandDispatchException.class);
        }

        @Test
        void failsWithADispatchFailureOnAnEmptyReply() {
            // given
            requestFactory.respondingWithStatus(HttpStatus.OK);

            // when / then
            assertThatThrownBy(() -> testSubject.dispatch(REMOTE_MEMBER, command()).join())
                    .hasCauseInstanceOf(CommandDispatchException.class)
                    .hasMessageContaining("empty body");
        }

        @Test
        void failsWithADispatchFailureWhenTheMemberHasNoEndpoint() {
            // given
            Member withoutEndpoint = Member.unregisteredLocalMember("UNIVERSITY[LOCAL]");

            // when / then
            assertThatThrownBy(() -> testSubject.dispatch(withoutEndpoint, command()).join())
                    .hasCauseInstanceOf(CommandDispatchException.class)
                    .hasMessageContaining("no endpoint");
        }
    }

    @Nested
    class Validation {

        @Test
        void failsWhenThePayloadWasNeverConverted() {
            // given — a connector not wrapped in a PayloadConvertingCommandBusConnector would produce this
            CommandMessage unconverted = new GenericCommandMessage(COMMAND_TYPE, Map.of("name", "Axon 5"));

            // when / then the command is not sent at all, rather than sent in a form no member can read
            assertThatThrownBy(() -> testSubject.dispatch(REMOTE_MEMBER, unconverted).join())
                    .hasCauseInstanceOf(ConversionException.class);
            assertThat(requestFactory.requests()).isEmpty();
        }

        @Test
        void rejectsNullArguments() {
            // when / then
            assertThatThrownBy(() -> testSubject.dispatch(null, command()))
                    .isInstanceOf(NullPointerException.class);
            assertThatThrownBy(() -> testSubject.dispatch(REMOTE_MEMBER, null))
                    .isInstanceOf(NullPointerException.class);
        }
    }
}
