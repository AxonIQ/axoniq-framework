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

package io.axoniq.framework.axonserver.connector.shared;

import io.axoniq.axonserver.grpc.ErrorMessage;
import io.axoniq.framework.axonserver.connector.api.AxonServerException;
import io.axoniq.framework.axonserver.connector.api.command.AxonServerCommandDispatchException;
import io.axoniq.framework.axonserver.connector.api.command.AxonServerNonTransientRemoteCommandHandlingException;
import io.axoniq.framework.axonserver.connector.api.command.AxonServerRemoteCommandHandlingException;
import io.axoniq.framework.axonserver.connector.api.query.AxonServerNonTransientRemoteQueryHandlingException;
import io.axoniq.framework.axonserver.connector.api.query.AxonServerQueryDispatchException;
import io.axoniq.framework.axonserver.connector.api.query.AxonServerRemoteQueryHandlingException;
import org.axonframework.common.AxonException;
import org.axonframework.conversion.Converter;
import org.axonframework.eventsourcing.eventstore.EventStoreException;
import org.axonframework.messaging.commandhandling.CommandExecutionException;
import org.axonframework.messaging.commandhandling.NoHandlerForCommandException;
import org.axonframework.messaging.core.EventPublicationFailedException;
import org.axonframework.messaging.core.HandlerExecutionException;
import org.axonframework.messaging.queryhandling.NoHandlerForQueryException;
import org.axonframework.messaging.queryhandling.QueryExecutionException;
import org.axonframework.modelling.ConcurrencyException;
import org.jspecify.annotations.Nullable;

import java.util.function.Supplier;

/**
 * Converts exceptions and {@link ErrorCode error codes} received from Axon Server to framework exceptions.
 *
 * @author John Hendrikx
 * @since 5.1.0
 */
final class ExceptionFactory {

    /**
     * A reconstructed remote exception's local stack trace would only point at this gRPC deserialization code, not
     * at the actual failure on the remote handler, so it's suppressed to avoid misleading callers.
     */
    private static final boolean SUPPRESS_LOCAL_STACK_TRACE = false;

    /**
     * Converts the {@code throwable} to the relevant AxonException
     *
     * @param throwable the descriptor of the error
     * @return the Axon Framework exception
     */
    public static AxonException convert(ErrorCode errorCode,
                                        Throwable throwable) {
        return convert(errorCode, "", throwable);
    }

    /**
     * Converts the {@code source} and the {@code throwable} to the relevant AxonException
     *
     * @param source    the location that originally reported the error
     * @param throwable the descriptor of the error
     * @return the Axon Framework exception
     */
    public static AxonException convert(ErrorCode errorCode,
                                        String source,
                                        Throwable throwable) {
        return convert(
                errorCode,
                ExceptionConverter.convertToErrorMessage(source, null, throwable),
                () -> HandlerExecutionException.resolveDetails(throwable).orElse(null)
        );
    }

    /**
     * Converts the {@code errorMessage} to the relevant AxonException
     *
     * @param errorMessage the descriptor of the error
     * @param details      a supplier of (optional) application-specific details to be included in Exception, when
     *                     appropriate
     * @return the Axon Framework exception
     */
    public static AxonException convert(ErrorCode errorCode,
                                        ErrorMessage errorMessage,
                                        Supplier<@Nullable Object> details) {
        return convert(errorCode, errorMessage, details, null);
    }

    /**
     * Converts the {@code errorMessage} to the relevant AxonException
     *
     * @param errorMessage the descriptor of the error
     * @param details      a supplier of (optional) application-specific details to be included in Exception, when
     *                     appropriate
     * @param converter    the {@link Converter} to attach to the resulting exception, so that {@code details} not
     *                     already of the type requested by a caller can be converted lazily, on request, or
     *                     {@code null} if no such conversion is available
     * @return the Axon Framework exception
     */
    public static AxonException convert(ErrorCode errorCode,
                                        ErrorMessage errorMessage,
                                        @Nullable Supplier<@Nullable Object> details,
                                        @Nullable Converter converter) {
        if (details == null) {
            // safeguard to prevent NullPointerException
            return convert(errorCode, errorMessage);
        }
        return resolve(errorCode, errorMessage, details, converter);
    }

    /**
     * Converts the {@code errorMessage} to the relevant AxonException
     *
     * @param errorMessage the descriptor of the error
     * @return the Axon Framework exception
     */
    public static AxonException convert(ErrorCode errorCode,
                                        ErrorMessage errorMessage) {
        return resolve(errorCode, errorMessage, () -> null, null);
    }

    private static AxonException resolve(ErrorCode errorCode,
                                         ErrorMessage errorMessage,
                                         @Nullable Supplier<@Nullable Object> details,
                                         @Nullable Converter converter) {
        Supplier<@Nullable Object> safeDetails = details != null ? details : () -> null;

        return switch (errorCode) {

            // --- Generic / infrastructure ---
            case AUTHENTICATION_TOKEN_MISSING,
                 AUTHENTICATION_INVALID_TOKEN,
                 UNSUPPORTED_INSTRUCTION,
                 INSTRUCTION_ACK_ERROR,
                 INSTRUCTION_EXECUTION_ERROR,
                 CONNECTION_FAILED,
                 GRPC_MESSAGE_TOO_LARGE,
                 OTHER -> new AxonServerException(errorCode.errorCode(), errorMessage);

            // --- Event publishing ---
            case INVALID_EVENT_SEQUENCE -> new ConcurrencyException(
                    errorMessage.getMessage(),
                    new AxonServerException(errorCode.errorCode(), errorMessage)
            );

            case NO_EVENT_STORE_MASTER_AVAILABLE, EVENT_PAYLOAD_TOO_LARGE -> new EventPublicationFailedException(
                    errorMessage.getMessage(),
                    new AxonServerException(errorCode.errorCode(), errorMessage)
            );

            // --- Command ---
            case NO_HANDLER_FOR_COMMAND -> new NoHandlerForCommandException(errorMessage.getMessage());

            case COMMAND_EXECUTION_ERROR -> new CommandExecutionException(
                    errorMessage.getMessage(),
                    new AxonServerRemoteCommandHandlingException(errorCode.errorCode(), errorMessage),
                    safeDetails.get(),
                    converter,
                    SUPPRESS_LOCAL_STACK_TRACE
            );

            case COMMAND_EXECUTION_NON_TRANSIENT_ERROR -> new CommandExecutionException(
                    errorMessage.getMessage(),
                    new AxonServerNonTransientRemoteCommandHandlingException(errorCode.errorCode(), errorMessage),
                    safeDetails.get(),
                    converter,
                    SUPPRESS_LOCAL_STACK_TRACE
            );

            case COMMAND_DISPATCH_ERROR -> new AxonServerCommandDispatchException(errorCode.errorCode(), errorMessage);

            case CONCURRENCY_EXCEPTION -> new ConcurrencyException(
                    errorMessage.getMessage(),
                    new AxonServerRemoteCommandHandlingException(errorCode.errorCode(), errorMessage)
            );

            // --- Query ---
            case NO_HANDLER_FOR_QUERY -> new NoHandlerForQueryException(errorMessage.getMessage());

            case QUERY_EXECUTION_ERROR -> new QueryExecutionException(
                    errorMessage.getMessage(),
                    new AxonServerRemoteQueryHandlingException(errorCode.errorCode(), errorMessage),
                    safeDetails.get(),
                    converter,
                    SUPPRESS_LOCAL_STACK_TRACE
            );

            case QUERY_EXECUTION_NON_TRANSIENT_ERROR -> new QueryExecutionException(
                    errorMessage.getMessage(),
                    new AxonServerNonTransientRemoteQueryHandlingException(errorCode.errorCode(), errorMessage),
                    safeDetails.get(),
                    converter,
                    SUPPRESS_LOCAL_STACK_TRACE
            );

            case QUERY_DISPATCH_ERROR -> new AxonServerQueryDispatchException(errorCode.errorCode(), errorMessage);

            // --- Event store / internal ---
            case DATAFILE_READ_ERROR,
                 INDEX_READ_ERROR,
                 DATAFILE_WRITE_ERROR,
                 INDEX_WRITE_ERROR,
                 DIRECTORY_CREATION_FAILED,
                 VALIDATION_FAILED,
                 TRANSACTION_ROLLED_BACK -> new EventStoreException(
                    errorMessage.getMessage(),
                    new AxonServerException(errorCode.errorCode(), errorMessage)
            );
        };
    }

    private ExceptionFactory() {
        // do not instantiate
    }
}
