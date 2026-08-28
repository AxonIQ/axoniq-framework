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

import org.axonframework.common.ExceptionUtils;
import org.axonframework.messaging.commandhandling.CommandDispatchException;
import org.axonframework.messaging.commandhandling.NoHandlerForCommandException;

/**
 * The kinds of failure a {@link CommandDispatchReply} can report, and the classification a receiving member needs to
 * reconstruct an equivalent exception.
 * <p>
 * Failures cross the wire as a code rather than a serialized exception. Serializing exceptions would couple both ends
 * to the same classes and versions, and would make the reply format a deserialization surface. A code plus a message
 * conveys what the dispatching side actually needs: which kind of failure occurred, and whether retrying it can help.
 *
 * @author Allard Buijze
 * @since 5.4.0
 */
public enum CommandErrorCode {

    /**
     * The receiving member had no handler for the command. Reconstructed as a
     * {@link NoHandlerForCommandException}, which is transient: the handler may yet appear, for instance because the
     * member is still starting up.
     */
    NO_HANDLER_FOR_COMMAND,

    /**
     * The handler on the receiving member threw. Reconstructed as a
     * {@link org.axonframework.messaging.commandhandling.CommandExecutionException} wrapping a transient
     * {@link org.axonframework.messaging.core.RemoteHandlingException}, so a
     * {@link org.axonframework.messaging.core.retry.RetryScheduler} may retry it.
     */
    COMMAND_EXECUTION_ERROR,

    /**
     * The handler on the receiving member threw an exception it marked as non-transient. Reconstructed as a
     * {@link org.axonframework.messaging.commandhandling.CommandExecutionException} wrapping a
     * {@link org.axonframework.messaging.core.RemoteNonTransientHandlingException}, so a
     * {@link org.axonframework.messaging.core.retry.RetryScheduler} does not retry it.
     */
    COMMAND_EXECUTION_NON_TRANSIENT_ERROR,

    /**
     * The command reached the receiving member but never reached a handler there — it could not be read, for
     * instance. Reconstructed as a {@link CommandDispatchException}.
     */
    COMMAND_DISPATCH_ERROR;

    /**
     * Classifies the given {@code cause}, raised while receiving or handling a command, into the code to report it
     * under.
     * <p>
     * The two specific kinds are recognised first, because both say something the generic execution codes do not: no
     * handler exists yet, and the command never reached a handler at all. Only what is left is an execution failure,
     * split by whether the application declared it worth retrying.
     *
     * @param cause the exception raised while receiving or handling a command
     * @return {@link #NO_HANDLER_FOR_COMMAND} when no handler was found, {@link #COMMAND_DISPATCH_ERROR} when the
     * command never reached a handler, {@link #COMMAND_EXECUTION_NON_TRANSIENT_ERROR} when the given {@code cause} is
     * explicitly non-transient, and {@link #COMMAND_EXECUTION_ERROR} otherwise
     */
    public static CommandErrorCode classify(Throwable cause) {
        if (ExceptionUtils.findException(cause, NoHandlerForCommandException.class).isPresent()) {
            return NO_HANDLER_FOR_COMMAND;
        }
        if (ExceptionUtils.findException(cause, CommandDispatchException.class).isPresent()) {
            return COMMAND_DISPATCH_ERROR;
        }
        return ExceptionUtils.isExplicitlyNonTransient(cause)
                ? COMMAND_EXECUTION_NON_TRANSIENT_ERROR
                : COMMAND_EXECUTION_ERROR;
    }
}
