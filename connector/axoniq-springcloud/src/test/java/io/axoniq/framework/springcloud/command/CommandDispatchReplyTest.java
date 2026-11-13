/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.springcloud.command;

import org.junit.jupiter.api.*;

import java.util.HashMap;
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
    class CarryingMetadata {

        @Test
        void keepsAnEntryWithoutAValue() {
            // given metadata permits null values, which Map.copyOf rejects
            Map<String, String> metadata = new HashMap<>();
            metadata.put("tenant", null);

            // when
            CommandDispatchReply reply = CommandDispatchReply.result("reply-1", "command-1", null, null, metadata);

            // then
            assertThat(reply.metadata()).containsEntry("tenant", null);
        }

        @Test
        void doesNotFollowChangesToTheMapItWasGiven() {
            // given
            Map<String, String> metadata = new HashMap<>();
            metadata.put("tenant", "acme");
            CommandDispatchReply reply = CommandDispatchReply.result("reply-1", "command-1", null, null, metadata);

            // when
            metadata.put("tenant", "other");

            // then
            assertThat(reply.metadata()).containsEntry("tenant", "acme");
        }

        @Test
        void readsAReplyThatOmitsMetadataAltogether() {
            // when
            CommandDispatchReply reply = CommandDispatchReply.result("reply-1", "command-1", null, null, null);

            // then
            assertThat(reply.metadata()).isEmpty();
        }
    }

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
            assertThat(reply.hasError()).isTrue();
        }

        @Test
        void isNotAnErrorWhenNoErrorCodeIsCarried() {
            // given — a successful reply, carrying a result rather than a failure
            CommandDispatchReply reply = CommandDispatchReply.result(
                    "reply-1", "command-1", "university.CourseCreated#1.0.0", "e30=", Map.of()
            );

            // when / then
            assertThat(reply.hasError()).isFalse();
        }

        @Test
        void isNotAnErrorWhenTheHandlerProducedNoResult() {
            // given — a handler that returned nothing is still a success
            CommandDispatchReply reply = CommandDispatchReply.noResult("reply-1", "command-1");

            // when / then
            assertThat(reply.hasError()).isFalse();
        }
    }
}
