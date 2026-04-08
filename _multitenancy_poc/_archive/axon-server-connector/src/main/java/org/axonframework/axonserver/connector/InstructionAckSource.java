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
import io.grpc.stub.StreamObserver;

/**
 * Responsible for sending instruction acknowledgements.
 *
 * @param <T> the type of message to be sent
 * @author Milan Savic
 * @since 4.2.1
 */
@FunctionalInterface
public interface InstructionAckSource<T> {

    /**
     * Sends successful acknowledgement.
     *
     * @param instructionId identifier of successful instruction. If {@code null}, acknowledgement will not be sent.
     * @param stream        the stream for sending acknowledgements
     */
    default void sendSuccessfulAck(String instructionId, StreamObserver<T> stream) {
        sendAck(instructionId, true, null, stream);
    }

    /**
     * Sends acknowledgement of unsupported instruction.
     *
     * @param instructionId identifier of successful instruction. If {@code null}, acknowledgement will not be sent.
     * @param source        the location where error occurred
     * @param stream        the stream for sending acknowledgements
     */
    default void sendUnsupportedInstruction(String instructionId, String source, StreamObserver<T> stream) {
        ErrorMessage unsupportedInstruction = ErrorMessage.newBuilder()
                                                          .setErrorCode(ErrorCode.UNSUPPORTED_INSTRUCTION.errorCode())
                                                          .setLocation(source)
                                                          .addDetails("Unsupported instruction")
                                                          .build();
        sendUnsuccessfulAck(instructionId, unsupportedInstruction, stream);
    }

    /**
     * Sends unsuccessful acknowledgement.
     *
     * @param instructionId identifier of successful instruction. If {@code null}, acknowledgement will not be sent.
     * @param error         the cause of the error
     * @param stream        the stream for sending acknowledgements
     */
    default void sendUnsuccessfulAck(String instructionId, ErrorMessage error, StreamObserver<T> stream) {
        sendAck(instructionId, false, error, stream);
    }

    /**
     * Sends an acknowledgement.
     *
     * @param instructionId identifier of successful instruction. If {@code null}, acknowledgement will not be sent.
     * @param success       {@code true} if acknowledgement is successful, {@code false} otherwise
     * @param error         the cause of the error. Provide {@code null} for successful acknowledgement
     * @param stream        the stream for sending acknowledgements
     */
    void sendAck(String instructionId, boolean success, ErrorMessage error, StreamObserver<T> stream);
}
