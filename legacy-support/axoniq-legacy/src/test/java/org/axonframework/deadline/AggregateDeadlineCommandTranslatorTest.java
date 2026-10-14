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

package org.axonframework.deadline;

import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.ScopeDescriptor;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.commandhandling.NoHandlerForCommandException;
import org.axonframework.messaging.commandhandling.gateway.CommandGateway;
import org.axonframework.messaging.commandhandling.gateway.CommandResult;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.modelling.command.AggregateScopeDescriptor;
import org.axonframework.modelling.saga.SagaScopeDescriptor;
import org.junit.jupiter.api.*;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.axonframework.deadline.AggregateDeadlineEntityIdResolverDefinition.DESCRIPTOR_BASED_ID;

/**
 * Test class validating the {@link AggregateDeadlineCommandTranslator}.
 *
 * @author Steven van Beelen
 */
class AggregateDeadlineCommandTranslatorTest {

    private static final AggregateScopeDescriptor TEST_SCOPE =
            new AggregateScopeDescriptor("MyAggregate", "aggregateId");

    private RecordingCommandGateway commandGateway;
    private AggregateDeadlineCommandTranslator testSubject;

    @BeforeEach
    void setUp() {
        commandGateway = new RecordingCommandGateway();
        testSubject = new AggregateDeadlineCommandTranslator(commandGateway);
    }

    @Nested
    class CanResolve {

        @Test
        void returnsTrueForAnyAggregateScopeDescriptor() {
            assertThat(testSubject.canResolve(TEST_SCOPE)).isTrue();
            assertThat(testSubject.canResolve(new AggregateScopeDescriptor("OtherAggregate", "otherId"))).isTrue();
        }

        @Test
        void returnsFalseForAnyOtherScopeDescriptor() {
            ScopeDescriptor sagaScope = new SagaScopeDescriptor("MySaga", "sagaId");

            assertThat(testSubject.canResolve(sagaScope)).isFalse();
        }
    }

    @Nested
    class Send {

        @Test
        void dispatchesPayloadAndMetadataAsCommandNamedAfterTheDeadlineWithGivenContext() throws Exception {
            // given
            record TestPayload(String data) {

            }
            TestPayload payload = new TestPayload("some-data");
            Metadata metadata = Metadata.with("key", "value");
            DeadlineMessage deadline = new GenericDeadlineMessage(
                    "paymentDue", new MessageType(TestPayload.class), payload, metadata
            );
            ProcessingContext context = StubProcessingContext.forMessage(deadline);

            // when
            testSubject.send(deadline, context, TEST_SCOPE);

            // then
            assertThat(commandGateway.sent).hasSize(1);
            CommandMessage command = commandGateway.sentCommand(0);
            assertThat(command.type().qualifiedName().name()).isEqualTo("paymentDue");
            assertThat(command.payload()).isEqualTo(payload);
            assertThat(command.metadata())
                    .containsEntry("key", "value")
                    .containsEntry(DESCRIPTOR_BASED_ID, "aggregateId");
            assertThat(commandGateway.sent.getFirst().context()).isSameAs(context);
        }

        @Test
        void dispatchesDeadlinesSharingAPayloadTypeAsDifferentCommands() throws Exception {
            // given
            record TestPayload(String data) {

            }
            DeadlineMessage paymentDue = new GenericDeadlineMessage(
                    "paymentDue", new MessageType(TestPayload.class), new TestPayload("some-data")
            );
            DeadlineMessage shipmentDue = new GenericDeadlineMessage(
                    "shipmentDue", new MessageType(TestPayload.class), new TestPayload("some-data")
            );

            // when
            testSubject.send(paymentDue, StubProcessingContext.forMessage(paymentDue), TEST_SCOPE);
            testSubject.send(shipmentDue, StubProcessingContext.forMessage(shipmentDue), TEST_SCOPE);

            // then
            assertThat(commandGateway.sentCommand(0).type().qualifiedName().name()).isEqualTo("paymentDue");
            assertThat(commandGateway.sentCommand(1).type().qualifiedName().name()).isEqualTo("shipmentDue");
        }

        @Test
        void dispatchesThePayloadAgainNamedAfterItsTypeWhenNoHandlerExistsForTheDeadlineName() throws Exception {
            // given
            record TestPayload(String data) {

            }
            TestPayload payload = new TestPayload("some-data");
            Metadata metadata = Metadata.with("key", "value");
            DeadlineMessage deadline = new GenericDeadlineMessage(
                    "paymentDue", new MessageType(TestPayload.class), payload, metadata
            );
            ProcessingContext context = StubProcessingContext.forMessage(deadline);
            // only a handler migrated with an earlier version, named after the payload type, exists
            commandGateway.results.add(CompletableFuture.failedFuture(new NoHandlerForCommandException("none")));

            // when
            testSubject.send(deadline, context, TEST_SCOPE);

            // then
            assertThat(commandGateway.sent).hasSize(2);
            RecordingCommandGateway.Sent fallback = commandGateway.sent.get(1);
            assertThat(fallback.payload()).isEqualTo(payload);
            assertThat(fallback.metadata())
                    .containsEntry("key", "value")
                    .containsEntry(DESCRIPTOR_BASED_ID, "aggregateId");
            assertThat(fallback.context()).isSameAs(context);
        }

        @Test
        void rethrowsTheFallbacksFailureWhenNoHandlerExistsForThePayloadTypeEither() {
            // given
            record TestPayload(String data) {

            }
            DeadlineMessage deadline = new GenericDeadlineMessage(
                    "paymentDue", new MessageType(TestPayload.class), new TestPayload("some-data")
            );
            ProcessingContext context = StubProcessingContext.forMessage(deadline);
            NoHandlerForCommandException fallbackFailure = new NoHandlerForCommandException("none for payload type");
            commandGateway.results.add(CompletableFuture.failedFuture(new NoHandlerForCommandException("none")));
            commandGateway.results.add(CompletableFuture.failedFuture(fallbackFailure));

            // when / then
            assertThatThrownBy(() -> testSubject.send(deadline, context, TEST_SCOPE)).isSameAs(fallbackFailure);
            assertThat(commandGateway.sent).hasSize(2);
        }

        @Test
        void doesNotFallBackForAPayloadLessDeadline() {
            // given
            DeadlineMessage deadline =
                    new GenericDeadlineMessage("paymentDue", new MessageType(Void.class), null);
            ProcessingContext context = StubProcessingContext.forMessage(deadline);
            NoHandlerForCommandException noHandler = new NoHandlerForCommandException("none");
            commandGateway.results.add(CompletableFuture.failedFuture(noHandler));

            // when / then
            assertThatThrownBy(() -> testSubject.send(deadline, context, TEST_SCOPE)).isSameAs(noHandler);
            assertThat(commandGateway.sent).hasSize(1);
        }

        @Test
        void dispatchesGenericCommandMessageWithDeadlineNameWhenPayloadIsNull() throws Exception {
            // given
            Metadata metadata = Metadata.with("key", "value");
            DeadlineMessage deadline =
                    new GenericDeadlineMessage("paymentDue", new MessageType(Void.class), null, metadata);
            ProcessingContext context = StubProcessingContext.forMessage(deadline);

            // when
            testSubject.send(deadline, context, TEST_SCOPE);

            // then
            CommandMessage command = commandGateway.sentCommand(0);
            assertThat(command.type().qualifiedName().name()).isEqualTo("paymentDue");
            assertThat(command.payload()).isEqualTo("paymentDue");
            assertThat(command.metadata())
                    .containsEntry("key", "value")
                    .containsEntry(DESCRIPTOR_BASED_ID, "aggregateId");
            assertThat(commandGateway.sent.getFirst().context()).isSameAs(context);
        }

        @Test
        void rethrowsTheFailedCommandsCauseWithoutFallingBack() {
            // given
            RuntimeException commandFailure = new RuntimeException("Command handling failed");
            commandGateway.results.add(CompletableFuture.failedFuture(commandFailure));
            record TestPayload(String data) {

            }
            DeadlineMessage deadline = new GenericDeadlineMessage(
                    "paymentDue", new MessageType(TestPayload.class), new TestPayload("some-data")
            );
            ProcessingContext context = StubProcessingContext.forMessage(deadline);

            // when / then
            assertThatThrownBy(() -> testSubject.send(deadline, context, TEST_SCOPE))
                    .isSameAs(commandFailure);
            assertThat(commandGateway.sent).hasSize(1);
        }

        @Test
        void rejectsAMessageThatIsNotADeadlineMessage() {
            // given
            record TestPayload(String data) {

            }
            Message notADeadline = new GenericMessage(new MessageType(TestPayload.class), new TestPayload("x"));
            ProcessingContext context = StubProcessingContext.forMessage(notADeadline);

            // when / then
            assertThatThrownBy(() -> testSubject.send(notADeadline, context, TEST_SCOPE))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThat(commandGateway.sent).isEmpty();
        }

        @Test
        void rejectsAScopeDescriptorThatIsNotAnAggregateScopeDescriptor() {
            // given
            record TestPayload(String data) {

            }
            DeadlineMessage deadline = new GenericDeadlineMessage(
                    "paymentDue", new MessageType(TestPayload.class), new TestPayload("some-data")
            );
            ProcessingContext context = StubProcessingContext.forMessage(deadline);
            ScopeDescriptor sagaScope = new SagaScopeDescriptor("MySaga", "sagaId");

            // when / then
            assertThatThrownBy(() -> testSubject.send(deadline, context, sagaScope))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThat(commandGateway.sent).isEmpty();
        }

        @Test
        void skipsDispatchForAnUnknownDeadlinePayload() throws Exception {
            // given
            UnknownDeadlinePayload unknownPayload = new UnknownDeadlinePayload("com.example.Removed", null, null);
            DeadlineMessage deadline = new GenericDeadlineMessage(
                    "paymentDue", new MessageType(UnknownDeadlinePayload.class), unknownPayload
            );
            ProcessingContext context = StubProcessingContext.forMessage(deadline);

            // when
            testSubject.send(deadline, context, TEST_SCOPE);

            // then
            assertThat(commandGateway.sent).isEmpty();
        }
    }

    /**
     * Records every dispatched command, and answers each with the next queued result, or a successful one once the
     * queue is empty.
     */
    private static class RecordingCommandGateway implements CommandGateway {

        private final List<Sent> sent = new ArrayList<>();
        private final Queue<CompletableFuture<? extends Message>> results = new ArrayDeque<>();

        @Override
        public CommandResult send(Object payload, Metadata metadata, ProcessingContext context) {
            sent.add(new Sent(payload, metadata, context));
            CompletableFuture<? extends Message> result = results.poll();
            CompletableFuture<? extends Message> answer =
                    result != null ? result : CompletableFuture.completedFuture(null);
            return () -> answer;
        }

        private CommandMessage sentCommand(int index) {
            assertThat(sent.get(index).payload()).isInstanceOf(CommandMessage.class);
            return (CommandMessage) sent.get(index).payload();
        }

        private record Sent(Object payload, Metadata metadata, ProcessingContext context) {

        }

        @Override
        public void describeTo(ComponentDescriptor descriptor) {
            // Not relevant for this recording implementation.
        }
    }
}
