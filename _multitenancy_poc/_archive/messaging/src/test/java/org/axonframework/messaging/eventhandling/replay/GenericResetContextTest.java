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

package org.axonframework.messaging.eventhandling.replay;

import org.jspecify.annotations.Nullable;
import org.axonframework.common.ObjectUtils;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.MessageTestSuite;
import org.axonframework.messaging.core.MessageType;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class validating the {@link GenericResetContext}.
 *
 * @author Steven van Beelen
 */
class GenericResetContextTest extends MessageTestSuite<ResetContext> {

    @Override
    protected ResetContext buildDefaultMessage() {
        return new GenericResetContext(new GenericMessage(
                TEST_IDENTIFIER, TEST_TYPE, TEST_PAYLOAD, TEST_PAYLOAD_TYPE, TEST_METADATA
        ));
    }

    @Override
    protected <P> ResetContext buildMessage(@Nullable P payload) {
        return new GenericResetContext(new MessageType(ObjectUtils.nullSafeTypeOf(payload)), payload);
    }

    @Test
    void describeType() {
        assertEquals("GenericResetContext", new GenericResetContext(TEST_TYPE, TEST_PAYLOAD).describeType());
    }
}
