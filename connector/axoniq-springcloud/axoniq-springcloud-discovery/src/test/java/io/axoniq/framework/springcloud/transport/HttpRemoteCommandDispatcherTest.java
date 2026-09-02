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
import java.time.Duration;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests how {@link HttpRemoteCommandDispatcher} sends a command to another member and reads back the outcome.
 *
 * @author Allard Buijze
 */
class HttpRemoteCommandDispatcherTest {

    private static final MessageType COMMAND_TYPE = new MessageType("university.CreateCourse", "1.0.0");
    private static final byte[] PAYLOAD = "{\"name\":\"Axon 5\"}".getBytes(StandardCharsets.UTF_8);
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
            String payload = Base64.getEncoder().encodeToString(PAYLOAD);
            requestFactory.respondingWith(
                    "{\"identifier\":\"reply-1\",\"requestIdentifier\":\"command-1\","
                            + "\"type\":\"university.CourseId#1.0.0\",\"payload\":\"" + payload + "\"}", null);

            // when
            CommandResultMessage result = testSubject.dispatch(REMOTE_MEMBER, command()).join();

            // then
            assertThat(result).isNotNull();
            assertThat(result.type()).isEqualTo(new MessageType("university.CourseId", "1.0.0"));
            assertThat((byte[]) result.payload()).isEqualTo(PAYLOAD);
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
        void reportsTheMemberAsUnreachableOnAConnectionError() {
            // given
            requestFactory.failingToConnect();

            // when / then — the connector relies on this to decide the member is unreachable
            assertThatThrownBy(() -> testSubject.dispatch(REMOTE_MEMBER, command()).join())
                    .hasCauseInstanceOf(MemberUnreachableException.class)
                    .hasMessageContaining("Could not send command");
        }

        @Test
        void reportsTheMemberAsUnreachableOnAnErrorStatus() {
            // given
            requestFactory.respondingWithStatus(HttpStatus.SERVICE_UNAVAILABLE);

            // when / then
            assertThatThrownBy(() -> testSubject.dispatch(REMOTE_MEMBER, command()).join())
                    .hasCauseInstanceOf(MemberUnreachableException.class);
        }

        @Test
        void reportsTheMemberAsUnreachableOnAnEmptyReply() {
            // given
            requestFactory.respondingWithStatus(HttpStatus.OK);

            // when / then
            assertThatThrownBy(() -> testSubject.dispatch(REMOTE_MEMBER, command()).join())
                    .hasCauseInstanceOf(MemberUnreachableException.class)
                    .hasMessageContaining("empty body");
        }

        @Test
        void reportsTheMemberAsUnreachableWhenItHasNoEndpoint() {
            // given
            Member withoutEndpoint = Member.unregisteredLocalMember("UNIVERSITY[LOCAL]");

            // when / then
            assertThatThrownBy(() -> testSubject.dispatch(withoutEndpoint, command()).join())
                    .hasCauseInstanceOf(MemberUnreachableException.class)
                    .hasMessageContaining("no endpoint");
        }
    }

    @Nested
    class WhenTheMemberDoesNotAnswer {

        @Test
        void reportsTheMemberAsUnreachableOnceTheReplyDeadlinePasses() throws Exception {
            // given — a member that accepted the request and then never answers, which is what a killed or
            // partitioned member leaves behind. A real executor is needed here: the deadline can only pass while the
            // round trip is still in flight on another thread.
            CountDownLatch releaseTheRequest = new CountDownLatch(1);
            ExecutorService executor = Executors.newSingleThreadExecutor();
            try {
                RestClient stalling = RestClient.builder()
                                                .requestFactory((uri, method) -> {
                                                    awaitQuietly(releaseTheRequest);
                                                    throw new IllegalStateException("Never reached.");
                                                })
                                                .build();
                HttpRemoteCommandDispatcher dispatcher = new HttpRemoteCommandDispatcher(
                        stalling,
                        SpringCloudCommandController.DEFAULT_COMMAND_ENDPOINT,
                        executor,
                        null,
                        Duration.ofMillis(50)
                );

                // when / then — reported as a failure to reach the member, so the connector takes it out of the ring
                assertThatThrownBy(() -> dispatcher.dispatch(REMOTE_MEMBER, command()).join())
                        .isInstanceOf(CompletionException.class)
                        .cause()
                        .isInstanceOf(MemberUnreachableException.class)
                        .hasMessageContaining("did not answer command")
                        .hasMessageContaining("PT0.05S");
            } finally {
                releaseTheRequest.countDown();
                executor.shutdownNow();
            }
        }

        private static void awaitQuietly(CountDownLatch latch) {
            try {
                latch.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Nested
    class Validation {

        @Test
        void failsWhenThePayloadIsNotBytes() {
            // given — a connector not wrapped in a PayloadConvertingCommandBusConnector would produce this
            CommandMessage unconverted = new GenericCommandMessage(COMMAND_TYPE, "not bytes");

            // when / then
            assertThatThrownBy(() -> testSubject.dispatch(REMOTE_MEMBER, unconverted).join())
                    .hasCauseInstanceOf(IllegalArgumentException.class);
            assertThat(requestFactory.requests()).isEmpty();
        }

        @Test
        void rejectsANonPositiveReplyTimeout() {
            RestClient restClient = RestClient.builder().requestFactory(requestFactory).build();

            // when / then
            assertThatThrownBy(() -> new HttpRemoteCommandDispatcher(
                    restClient, SpringCloudCommandController.DEFAULT_COMMAND_ENDPOINT,
                    Runnable::run, null, Duration.ZERO
            )).isInstanceOf(IllegalArgumentException.class)
              .hasMessageContaining("must be positive");
            assertThatThrownBy(() -> new HttpRemoteCommandDispatcher(
                    restClient, SpringCloudCommandController.DEFAULT_COMMAND_ENDPOINT,
                    Runnable::run, null, Duration.ofSeconds(-1)
            )).isInstanceOf(IllegalArgumentException.class)
              .hasMessageContaining("must be positive");
            assertThatThrownBy(() -> new HttpRemoteCommandDispatcher(
                    restClient, SpringCloudCommandController.DEFAULT_COMMAND_ENDPOINT,
                    Runnable::run, null, null
            )).isInstanceOf(NullPointerException.class);
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
