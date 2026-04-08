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

package org.axonframework.deadline;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.axonframework.common.ObjectUtils;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageTestSuite;
import org.axonframework.messaging.core.MessageType;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test class validating the {@link GenericDeadlineMessage}.
 *
 * @author Steven van Beelen
 */
class GenericDeadlineMessageTest extends MessageTestSuite<DeadlineMessage> {

    private static final String TEST_DEADLINE_NAME = "deadlineName";
    private static final Instant TEST_TIMESTAMP = Instant.now();

    @Override
    protected DeadlineMessage buildDefaultMessage() {
        Message delegate =
                new GenericMessage(TEST_IDENTIFIER, TEST_TYPE, TEST_PAYLOAD, TEST_PAYLOAD_TYPE, TEST_METADATA);
        return new GenericDeadlineMessage(TEST_DEADLINE_NAME, delegate, () -> TEST_TIMESTAMP);
    }

    @Override
    protected <P> DeadlineMessage buildMessage(@Nullable P payload) {
        return new GenericDeadlineMessage(TEST_DEADLINE_NAME,
                                          new MessageType(ObjectUtils.nullSafeTypeOf(payload)),
                                          payload);
    }

    @Override
    protected void validateDefaultMessage(@NonNull DeadlineMessage result) {
        assertThat(TEST_DEADLINE_NAME).isEqualTo(result.getDeadlineName());
        assertThat(TEST_TIMESTAMP).isEqualTo(result.timestamp());
    }

    @Override
    protected void validateMessageSpecifics(@NonNull DeadlineMessage actual, @NonNull DeadlineMessage result) {
        assertThat(actual.getDeadlineName()).isEqualTo(result.getDeadlineName());
        assertThat(actual.timestamp()).isEqualTo(result.timestamp());
    }
}