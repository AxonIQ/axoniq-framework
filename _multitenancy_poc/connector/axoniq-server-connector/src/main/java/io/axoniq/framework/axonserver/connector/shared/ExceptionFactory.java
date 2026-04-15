package io.axoniq.framework.axonserver.connector.shared;

import java.util.function.Supplier;

import org.axonframework.common.AxonException;
import org.axonframework.eventsourcing.eventstore.EventStoreException;
import org.axonframework.messaging.commandhandling.CommandExecutionException;
import org.axonframework.messaging.commandhandling.NoHandlerForCommandException;
import org.axonframework.messaging.core.EventPublicationFailedException;
import org.axonframework.messaging.core.HandlerExecutionException;
import org.axonframework.messaging.queryhandling.NoHandlerForQueryException;
import org.axonframework.messaging.queryhandling.QueryExecutionException;
import org.axonframework.modelling.ConcurrencyException;

import io.axoniq.axonserver.grpc.ErrorMessage;
import io.axoniq.framework.axonserver.connector.api.AxonServerException;
import io.axoniq.framework.axonserver.connector.api.command.AxonServerCommandDispatchException;
import io.axoniq.framework.axonserver.connector.api.command.AxonServerNonTransientRemoteCommandHandlingException;
import io.axoniq.framework.axonserver.connector.api.command.AxonServerRemoteCommandHandlingException;
import io.axoniq.framework.axonserver.connector.api.query.AxonServerNonTransientRemoteQueryHandlingException;
import io.axoniq.framework.axonserver.connector.api.query.AxonServerQueryDispatchException;
import io.axoniq.framework.axonserver.connector.api.query.AxonServerRemoteQueryHandlingException;

/**
 * Converts exceptions and {@link ErrorCode error codes} received from Axon Server
 * to framework exceptions.
 *
 * @author John Hendrikx
 * @since 5.1.0
 */
final class ExceptionFactory {

    /**
     * Converts the {@code throwable} to the relevant AxonException
     *
     * @param throwable the descriptor of the error
     * @return the Axon Framework exception
     */
    public static AxonException convert(ErrorCode errorCode, Throwable throwable) {
        return convert(errorCode, "", throwable);
    }

    /**
     * Converts the {@code source} and the {@code throwable} to the relevant AxonException
     *
     * @param source    The location that originally reported the error
     * @param throwable the descriptor of the error
     * @return the Axon Framework exception
     */
    public static AxonException convert(ErrorCode errorCode, String source, Throwable throwable) {
        return convert(errorCode, ExceptionConverter.convertToErrorMessage(source, null, throwable),
                       () -> HandlerExecutionException.resolveDetails(throwable).orElse(null));
    }

    /**
     * Converts the {@code errorMessage} to the relevant AxonException
     *
     * @param errorMessage the descriptor of the error
     * @param details      a supplier of (optional) application-specific details to be included in Exception, when
     *                     appropriate
     * @return the Axon Framework exception
     */
    public static AxonException convert(ErrorCode errorCode, ErrorMessage errorMessage, Supplier<Object> details) {
        if (details == null) {
            // safeguard to prevent NullPointerException
            return convert(errorCode, errorMessage);
        }

        return resolve(errorCode, errorMessage, details);
    }

    /**
     * Converts the {@code errorMessage} to the relevant AxonException
     *
     * @param errorMessage the descriptor of the error
     * @return the Axon Framework exception
     */
    public static AxonException convert(ErrorCode errorCode, ErrorMessage errorMessage) {
        return resolve(errorCode, errorMessage, () -> null);
    }

    private static AxonException resolve(ErrorCode errorCode, ErrorMessage errorMessage, Supplier<Object> details) {
        Supplier<Object> safeDetails = details != null ? details : () -> null;

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
                safeDetails.get()
            );

            case COMMAND_EXECUTION_NON_TRANSIENT_ERROR -> new CommandExecutionException(
                errorMessage.getMessage(),
                new AxonServerNonTransientRemoteCommandHandlingException(errorCode.errorCode(), errorMessage),
                safeDetails.get()
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
                safeDetails.get()
            );

            case QUERY_EXECUTION_NON_TRANSIENT_ERROR -> new QueryExecutionException(
                errorMessage.getMessage(),
                new AxonServerNonTransientRemoteQueryHandlingException(errorCode.errorCode(), errorMessage),
                safeDetails.get()
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
