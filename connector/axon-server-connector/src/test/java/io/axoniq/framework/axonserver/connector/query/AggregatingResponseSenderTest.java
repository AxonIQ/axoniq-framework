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

package io.axoniq.framework.axonserver.connector.query;

import io.axoniq.axonserver.connector.ErrorCategory;
import io.axoniq.axonserver.connector.ReplyChannel;
import io.axoniq.axonserver.grpc.query.QueryResponse;
import org.axonframework.common.TypeReference;
import org.axonframework.conversion.Converter;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.QueueMessageStream;
import org.axonframework.messaging.queryhandling.GenericQueryResponseMessage;
import org.axonframework.messaging.queryhandling.QueryResponseMessage;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.*;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

/**
 * Test class validating the {@link AggregatingResponseSender}.
 *
 * @author Steven van Beelen
 */
@SuppressWarnings("removal")
class AggregatingResponseSenderTest {

    private static final TypeReference<List<String>> LIST_OF_STRINGS = new TypeReference<>() {
    };

    private QueueMessageStream<QueryResponseMessage> upstream;
    private ReplyChannel<QueryResponse> stubDownstream;
    private Converter converter;

    @BeforeEach
    void setUp() {
        stubDownstream = mock();
        converter = new JacksonConverter();
        upstream = new QueueMessageStream<>();
    }

    @Test
    void sendsSingleElementUnchangedWhenUpstreamProducesExactlyOneMessage() {
        // given...
        AggregatingResponseSender testSubject =
                new AggregatingResponseSender("testCase", "test", upstream, stubDownstream, null);

        byte[] payload = "test".getBytes(StandardCharsets.UTF_8);
        upstream.offer(responseMsgFor(payload), Context.empty());
        upstream.seal();
        // when...
        testSubject.request(1);
        // then...
        verify(stubDownstream, never()).send(any());
        verify(stubDownstream, never()).complete();
        verify(stubDownstream).sendLast(assertArg(response -> {
            assertThat(response.getRequestIdentifier()).isEqualTo("test");
            assertThat(response.getPayload().getData().toByteArray()).isEqualTo(payload);
        }));
    }

    @Test
    void combinesMultipleElementsIntoOneResponseCarryingTheFullList() {
        // given...
        AggregatingResponseSender testSubject =
                new AggregatingResponseSender("testCase", "test", upstream, stubDownstream, converter);

        upstream.offer(responseMsgFor(converter.convert("a", byte[].class)), Context.empty());
        upstream.offer(responseMsgFor(converter.convert("b", byte[].class)), Context.empty());
        upstream.offer(responseMsgFor(converter.convert("c", byte[].class)), Context.empty());
        upstream.seal();
        // when...
        testSubject.request(1);
        // then...
        verify(stubDownstream, never()).send(any());
        verify(stubDownstream).sendLast(assertArg(response -> {
            assertThat(response.getRequestIdentifier()).isEqualTo("test");
            List<String> combined = converter.convert(
                    response.getPayload().getData().toByteArray(), LIST_OF_STRINGS.getType()
            );
            assertThat(combined).containsExactly("a", "b", "c");
        }));
    }

    @Test
    void sendsErrorResponseInsteadOfCombiningWhenElementTypeCannotBeResolved() {
        // given...
        AggregatingResponseSender testSubject =
                new AggregatingResponseSender("testCase", "test", upstream, stubDownstream, converter);

        MessageType unresolvableType = new MessageType(new QualifiedName("this.does.not.Exist"), "0.0.1");
        upstream.offer(new GenericQueryResponseMessage(unresolvableType, converter.convert("a", byte[].class)),
                       Context.empty());
        upstream.offer(new GenericQueryResponseMessage(unresolvableType, converter.convert("b", byte[].class)),
                       Context.empty());
        upstream.seal();
        // when...
        testSubject.request(1);
        // then...
        verify(stubDownstream, never()).send(any());
        verify(stubDownstream).sendLast(assertArg(
                response -> assertThat(response.getErrorCode())
                        .isEqualTo(ErrorCategory.QUERY_EXECUTION_ERROR.errorCode())
        ));
    }

    @Test
    void sendsEmptySentinelResponseWhenUpstreamCompletesWithoutAnyMessage() {
        // given...
        AggregatingResponseSender testSubject =
                new AggregatingResponseSender("testCase", "test", upstream, stubDownstream, null);

        upstream.seal();
        // when...
        testSubject.request(1);
        // then...
        verify(stubDownstream, never()).send(any());
        verify(stubDownstream, never()).complete();
        verify(stubDownstream).sendLast(assertArg(response -> {
            assertThat(response.getRequestIdentifier()).isEqualTo("test");
            assertThat(response.getPayload().getType()).isEqualTo("empty");
            assertThat(response.getPayload().getData().isEmpty()).isTrue();
        }));
    }

    @Test
    void sendsErrorResponseWhenUpstreamCompletesExceptionally() {
        // given...
        AggregatingResponseSender testSubject =
                new AggregatingResponseSender("testCase", "test", upstream, stubDownstream, null);

        upstream.offer(responseMsgFor("test".getBytes(StandardCharsets.UTF_8)), Context.empty());
        upstream.sealExceptionally(new RuntimeException("Custom message"));
        // when...
        testSubject.request(1);
        // then...
        verify(stubDownstream, never()).send(any());
        verify(stubDownstream).sendLast(assertArg(e -> {
            assertThat(e.getErrorMessage().getMessage()).isEqualTo("Custom message");
            assertThat(e.getErrorCode()).isEqualTo(ErrorCategory.QUERY_EXECUTION_ERROR.errorCode());
        }));
    }

    @Test
    void onlyDrainsUpstreamOnceEvenWhenRequestedMultipleTimes() {
        // given...
        AggregatingResponseSender testSubject =
                new AggregatingResponseSender("testCase", "test", upstream, stubDownstream, null);

        upstream.offer(responseMsgFor("test".getBytes(StandardCharsets.UTF_8)), Context.empty());
        upstream.seal();
        // when...
        testSubject.request(1);
        testSubject.request(1);
        // then...
        verify(stubDownstream, times(1)).sendLast(any());
    }

    @Test
    void cancellingClosesUpstream() {
        // given...
        AggregatingResponseSender testSubject =
                new AggregatingResponseSender("testCase", "test", upstream, stubDownstream, null);

        upstream.offer(responseMsgFor("test".getBytes(StandardCharsets.UTF_8)), Context.empty());
        // when...
        testSubject.cancel();
        // then...
        assertThat(upstream.hasNextAvailable()).isFalse();
        assertThat(upstream.isCompleted()).isTrue();
        assertThat(upstream.offer(responseMsgFor("test".getBytes(StandardCharsets.UTF_8)), Context.empty())).isFalse();
    }

    private static @NonNull GenericQueryResponseMessage responseMsgFor(Object payload) {
        return new GenericQueryResponseMessage(new MessageType(String.class), payload);
    }
}
