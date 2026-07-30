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

package io.axoniq.framework.messaging.commandhandling.distributed.tracing;

import io.axoniq.framework.messaging.commandhandling.distributed.CommandBusConnector;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.commandhandling.CommandResultMessage;
import org.axonframework.messaging.commandhandling.GenericCommandMessage;
import org.axonframework.messaging.commandhandling.GenericCommandResultMessage;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkTestUtils;
import org.axonframework.messaging.tracing.support.TestSpanFactory;
import org.axonframework.messaging.tracing.support.TestSpanFactory.TestSpanType;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class TracingCommandBusConnectorTest {

    private static final String DISPATCH_SPAN = "CommandBusConnector.dispatch MyCommand";
    private static final String HANDLE_SPAN = "CommandBusConnector.handle MyCommand";

    private TestSpanFactory spanFactory;
    private RecordingConnector delegate;
    private TracingCommandBusConnector testSubject;

    private final CommandMessage command =
            new GenericCommandMessage(new MessageType("MyCommand"), "payload");

    @BeforeEach
    void setUp() {
        spanFactory = new TestSpanFactory();
        delegate = new RecordingConnector();
        testSubject = new TracingCommandBusConnector(delegate, spanFactory);
    }

    @Nested
    class SendLeg {

        @Test
        void opensADispatchSpanAroundTheSendLeg() {
            // given
            delegate.dispatchResult = CompletableFuture.completedFuture(
                    new GenericCommandResultMessage(new MessageType("Result"), "ok"));

            // when
            testSubject.dispatch(command, null).orTimeout(5, TimeUnit.SECONDS).join();

            // then
            spanFactory.verifySpanCompleted(DISPATCH_SPAN);
            spanFactory.verifySpanHasType(DISPATCH_SPAN, TestSpanType.DISPATCH);
            spanFactory.verifySpanPropagated(DISPATCH_SPAN, command);
        }

        @Test
        void dispatchPassesABranchCarryingTheConnectorSpanToTheDelegate() {
            // given
            delegate.dispatchResult = new CompletableFuture<>();

            // when / then
            UnitOfWorkTestUtils.aUnitOfWork().executeWithResult(context -> {
                CompletableFuture<CommandResultMessage> result = testSubject.dispatch(command, context);
                assertThat(delegate.dispatchContext).isNotSameAs(context);
                // the branch carries specifically the connector dispatch span's scope, not just any active span
                spanFactory.verifyContextCarriesScopeOf(DISPATCH_SPAN, delegate.dispatchContext);
                delegate.dispatchResult.complete(
                        new GenericCommandResultMessage(new MessageType("Result"), "ok")
                );
                return result;
            }).orTimeout(5, TimeUnit.SECONDS).join();
        }
    }

    @Nested
    class ReceiveLeg {

        @Test
        void opensAndClosesAHandleSpanAroundTheReceiveLeg() {
            // given
            ResultCollector resultCollector = new ResultCollector();
            testSubject.onIncomingCommand((cmd, callback) -> callback.onSuccess(
                    new GenericCommandResultMessage(new MessageType("Result"), "ok")));

            // when the underlying connector delivers an inbound command, the inner delegate calls onSuccess
            // synchronously -- that completes the result callback, which closes the span scope.
            delegate.handler.handle(command, resultCollector);

            // then the handle span was opened AND closed on the receive leg
            spanFactory.verifySpanCompleted(HANDLE_SPAN);
            spanFactory.verifySpanHasType(HANDLE_SPAN, TestSpanType.HANDLER);
        }
    }

    private static final class RecordingConnector implements CommandBusConnector {

        private Handler handler;
        private CompletableFuture<CommandResultMessage> dispatchResult = new CompletableFuture<>();
        private ProcessingContext dispatchContext;

        @Override
        public CompletableFuture<CommandResultMessage> dispatch(CommandMessage command,
                                                                @Nullable ProcessingContext processingContext) {
            this.dispatchContext = processingContext;
            return dispatchResult;
        }

        @Override
        public CompletableFuture<Void> subscribe(QualifiedName commandName, int loadFactor) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public boolean unsubscribe(QualifiedName commandName) {
            return true;
        }

        @Override
        public void onIncomingCommand(Handler handler) {
            this.handler = handler;
        }

        @Override
        public void describeTo(ComponentDescriptor descriptor) {
            // No component details are needed for this recording test stub.
        }
    }

    private static final class ResultCollector implements CommandBusConnector.ResultCallback {

        @Override
        public void onSuccess(@Nullable CommandResultMessage resultMessage) {
            // The test only needs completion to close the tracing scope.
        }

        @Override
        public void onError(Throwable cause) {
            // The test only needs completion to close the tracing scope.
        }
    }
}
