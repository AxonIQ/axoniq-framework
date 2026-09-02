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

import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests how a {@link CommandDispatchReply} reports whether it carries a failure, which is what decides how the
 * dispatching side reconstructs the outcome.
 *
 * @author Allard Buijze
 */
class CommandDispatchReplyTest {

    @Nested
    class ReportingAFailure {

        @Test
        void isAnErrorWhenAnErrorCodeIsCarried() {
            // given
            CommandDispatchReply reply = CommandDispatchReply.error(
                    "reply-1", "command-1", CommandErrorCode.COMMAND_EXECUTION_ERROR,
                    "The course is full.", "UNIVERSITY[node-a]", List.of("The course is full."), null, null
            );

            // when / then
            assertThat(reply.isError()).isTrue();
        }

        @Test
        void isNotAnErrorWhenNoErrorCodeIsCarried() {
            // given — a successful reply, carrying a result rather than a failure
            CommandDispatchReply reply = CommandDispatchReply.result(
                    "reply-1", "command-1", "university.CourseCreated#1.0.0", "e30=", Map.of()
            );

            // when / then
            assertThat(reply.isError()).isFalse();
        }

        @Test
        void isNotAnErrorWhenTheHandlerProducedNoResult() {
            // given — a handler that returned nothing is still a success
            CommandDispatchReply reply = CommandDispatchReply.noResult("reply-1", "command-1");

            // when / then
            assertThat(reply.isError()).isFalse();
        }
    }
}
