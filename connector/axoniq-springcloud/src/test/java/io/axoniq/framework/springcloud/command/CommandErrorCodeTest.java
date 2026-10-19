/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.springcloud.command;

import org.axonframework.common.AxonNonTransientException;
import org.axonframework.messaging.commandhandling.CommandDispatchException;
import org.axonframework.messaging.commandhandling.CommandExecutionException;
import org.axonframework.messaging.commandhandling.NoHandlerForCommandException;
import org.junit.jupiter.api.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the classification a failure crossing the wire is reported under.
 *
 * @author Allard Buijze
 */
class CommandErrorCodeTest {

    @Nested
    class Classifying {

        @Test
        void reportsAMissingHandlerAsSuch() {
            // given
            Throwable cause = new NoHandlerForCommandException("No handler for [Course.Create].");

            // when
            CommandErrorCode code = CommandErrorCode.classify(cause);

            // then
            assertThat(code).isEqualTo(CommandErrorCode.NO_HANDLER_FOR_COMMAND);
        }

        @Test
        void reportsACommandThatNeverReachedAHandlerAsADispatchError() {
            // given
            Throwable cause = new CommandDispatchException("Could not read incoming command.");

            // when
            CommandErrorCode code = CommandErrorCode.classify(cause);

            // then
            assertThat(code).isEqualTo(CommandErrorCode.COMMAND_DISPATCH_ERROR);
        }

        @Test
        void reportsAHandlerFailureAsATransientExecutionError() {
            // given
            Throwable cause = new CommandExecutionException("The handler rejected the command.", null);

            // when
            CommandErrorCode code = CommandErrorCode.classify(cause);

            // then
            assertThat(code).isEqualTo(CommandErrorCode.COMMAND_EXECUTION_ERROR);
        }

        @Test
        void reportsAnExplicitlyNonTransientFailureAsSuch() {
            // given
            Throwable cause = new NonTransientFailure("This will fail again.");

            // when
            CommandErrorCode code = CommandErrorCode.classify(cause);

            // then
            assertThat(code).isEqualTo(CommandErrorCode.COMMAND_EXECUTION_NON_TRANSIENT_ERROR);
        }
    }

    @Nested
    class Precedence {

        @Test
        void recognisesAMissingHandlerWrappedInADispatchFailure() {
            // A member that resolved a handler and then lost it wraps one in the other. The missing handler is the
            // more specific of the two, and is transient in a way a dispatch error is not, so it must win.
            // given
            Throwable cause = new CommandDispatchException(
                    "Could not dispatch the command.",
                    new NoHandlerForCommandException("No handler for [Course.Create].")
            );

            // when
            CommandErrorCode code = CommandErrorCode.classify(cause);

            // then
            assertThat(code).isEqualTo(CommandErrorCode.NO_HANDLER_FOR_COMMAND);
        }

        @Test
        void recognisesADispatchFailureNestedBelowAnUnrelatedException() {
            // given
            Throwable cause = new IllegalStateException(
                    "Receiving the command failed.",
                    new CommandDispatchException("Could not read incoming command.")
            );

            // when
            CommandErrorCode code = CommandErrorCode.classify(cause);

            // then
            assertThat(code).isEqualTo(CommandErrorCode.COMMAND_DISPATCH_ERROR);
        }

        @Test
        void classifiesAnUnreachableMemberAsADispatchError() {
            // MemberUnreachableException is a CommandDispatchException, so a member reporting one it caught locally
            // still reports a dispatch error. What must not happen is the reverse: the code a member reports is
            // reconstructed as a plain CommandDispatchException on the dispatching side, so that the member is not
            // taken out of the routing ring for a failure it merely described.
            // given
            Throwable cause = new MemberUnreachableException("Could not send command to [http://member-2].");

            // when
            CommandErrorCode code = CommandErrorCode.classify(cause);

            // then
            assertThat(code).isEqualTo(CommandErrorCode.COMMAND_DISPATCH_ERROR);
        }
    }

    private static class NonTransientFailure extends AxonNonTransientException {

        NonTransientFailure(String message) {
            super(message);
        }
    }
}
