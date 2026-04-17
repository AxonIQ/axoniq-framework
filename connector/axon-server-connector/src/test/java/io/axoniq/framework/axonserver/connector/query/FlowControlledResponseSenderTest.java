/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */

package io.axoniq.framework.axonserver.connector.query;

import io.axoniq.axonserver.connector.ErrorCategory;
import io.axoniq.axonserver.connector.ReplyChannel;
import io.axoniq.axonserver.grpc.query.QueryResponse;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QueueMessageStream;
import org.axonframework.messaging.queryhandling.GenericQueryResponseMessage;
import org.axonframework.messaging.queryhandling.QueryResponseMessage;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.*;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class FlowControlledResponseSenderTest {

    public static final byte[] PAYLOAD = "test".getBytes(StandardCharsets.UTF_8);
    private QueueMessageStream<QueryResponseMessage> upstream;
    private ReplyChannel<QueryResponse> stubDownstream;
    private FlowControlledResponseSender testSubject;

    private static @NonNull GenericQueryResponseMessage newMessage() {
        return new GenericQueryResponseMessage(new MessageType(String.class), PAYLOAD);
    }

    @BeforeEach
    void setUp() {
        stubDownstream = mock();
        upstream = new QueueMessageStream<>();
        testSubject = new FlowControlledResponseSender("testCase", "test", upstream, stubDownstream);
    }

    @Test
    void shouldSendAllAlreadyAvailableMessagesUntilCompletionOnUnlimitedPermits() {
        for (int i = 0; i < 10; i++) {
            upstream.offer(newMessage(), Context.empty());
        }
        upstream.complete();
        testSubject.request(Long.MAX_VALUE);
        verify(stubDownstream, times(10)).send(any());
        verify(stubDownstream).complete();
    }

    @Test
    void shouldSendMessagesUntilCompletionOnUnlimitedPermitsWhenMessagesBecomeAvailable() {
        testSubject.request(Long.MAX_VALUE);
        for (int i = 0; i < 10; i++) {
            upstream.offer(newMessage(), Context.empty());
        }
        upstream.complete();
        verify(stubDownstream, times(10)).send(any());
        verify(stubDownstream).complete();
    }

    @Test
    void shouldNotOverlowOnMoreThanMaxLongRequests() {
        testSubject.request(5);
        testSubject.request(Long.MAX_VALUE);
        for (int i = 0; i < 10; i++) {
            upstream.offer(newMessage(), Context.empty());
        }
        upstream.complete();
        verify(stubDownstream, times(10)).send(any());
        verify(stubDownstream).complete();
    }

    @Test
    void shouldHonorTheFlowRequestsWhenEnoughMessagesAreAvailable() {
        for (int i = 0; i < 10; i++) {
            upstream.offer(newMessage(), Context.empty());
        }
        upstream.complete();
        for (int i = 0; i < 10; i++) {
            testSubject.request(1);
            verify(stubDownstream, times(i + 1)).send(any());
        }
        verify(stubDownstream).complete();
    }

    @Test
    void shouldTriggerCloseWhenUpstreamCompletesAfterSendingAllRequestedMessages() {
        for (int i = 0; i < 10; i++) {
            upstream.offer(newMessage(), Context.empty());
        }
        for (int i = 0; i < 10; i++) {
            testSubject.request(1);
            verify(stubDownstream, times(i + 1)).send(any());
        }
        upstream.complete();
        verify(stubDownstream).complete();
    }

    @Test
    void shouldOnlySendMessagesWhenRequested() {
        upstream.offer(newMessage(), Context.empty());
        upstream.offer(newMessage(), Context.empty());

        testSubject.request(1);
        verify(stubDownstream, times(1)).send(any());

        testSubject.request(2);
        verify(stubDownstream, times(2)).send(any());

        // there is 1 outstanding request for this message
        upstream.offer(newMessage(), Context.empty());
        verify(stubDownstream, times(3)).send(any());

        testSubject.request(1);
        // this message won't arrive
        upstream.complete();

        // still 3 messages sent
        verify(stubDownstream, times(3)).send(any());
        verify(stubDownstream).complete();
    }

    @Test
    void shouldSendLastMessageWithErrorWhenUpstreamCompletesExceptionally() {
        upstream.offer(newMessage(), Context.empty());
        upstream.completeExceptionally(new RuntimeException("Custom message"));

        verify(stubDownstream, never()).complete();
        verify(stubDownstream, never()).completeWithError(any());

        // this should also trigger the completion
        testSubject.request(1);

        verify(stubDownstream, times(1)).send(any());
        verify(stubDownstream).sendLast(assertArg(e -> {
            assertThat(e.getErrorMessage().getMessage()).isEqualTo("Custom message");
            assertThat(e.getErrorMessage().getErrorCode()).isEqualTo(ErrorCategory.QUERY_EXECUTION_ERROR.errorCode());
            assertThat(e.getErrorCode()).isEqualTo(ErrorCategory.QUERY_EXECUTION_ERROR.errorCode());
        }));
    }

    @Test
    void cancellingConsumerClosesUpstream() {
        upstream.offer(newMessage(), Context.empty());
        upstream.offer(newMessage(), Context.empty());

        testSubject.request(1);
        testSubject.cancel();

        assertThat(upstream.isClosed()).isTrue();
        assertThat(upstream.offer(newMessage(), Context.empty())).isFalse();
    }
}