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

package org.axonframework.axonserver.connector;

import io.axoniq.axonserver.grpc.ErrorMessage;
import io.axoniq.axonserver.grpc.InstructionAck;
import io.grpc.stub.StreamObserver;

import java.util.function.Function;

/**
 * Default implementation of {@link InstructionAckSource}.
 *
 * @param <T> the type of message to be sent
 * @author Milan Savic
 * @since 4.2.1
 */
public class DefaultInstructionAckSource<T> implements InstructionAckSource<T> {

    private final Function<InstructionAck, T> messageCreator;

    /**
     * Instantiates {@link DefaultInstructionAckSource}.
     *
     * @param messageCreator creates a message based on {@link InstructionAck}
     */
    public DefaultInstructionAckSource(Function<InstructionAck, T> messageCreator) {
        this.messageCreator = messageCreator;
    }

    @Override
    public void sendAck(String instructionId, boolean success, ErrorMessage error, StreamObserver<T> stream) {
        // Do not send an ack if instructionId is not set.
        // This way, requesting side can control for which instructions ack is needed
        if (instructionId == null || instructionId.equals("")) {
            return;
        }
        InstructionAck.Builder builder = InstructionAck.newBuilder()
                                                       .setInstructionId(instructionId)
                                                       .setSuccess(success);
        if (error != null) {
            builder.setError(error);
        }

        stream.onNext(messageCreator.apply(builder.build()));
    }
}
