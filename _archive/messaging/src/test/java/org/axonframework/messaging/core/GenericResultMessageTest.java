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

package org.axonframework.messaging.core;

import org.jspecify.annotations.Nullable;
import org.axonframework.common.ObjectUtils;
import org.junit.jupiter.api.*;

import static org.axonframework.messaging.core.GenericResultMessage.asResultMessage;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests {@link GenericResultMessage}.
 *
 * @author Milan Savic
 */
class GenericResultMessageTest extends MessageTestSuite<ResultMessage> {

    @Override
    protected ResultMessage buildDefaultMessage() {
        return new GenericResultMessage(new GenericMessage(
                TEST_IDENTIFIER, TEST_TYPE, TEST_PAYLOAD, TEST_PAYLOAD_TYPE, TEST_METADATA
        ));
    }

    @Override
    protected <P> ResultMessage buildMessage(@Nullable P payload) {
        return new GenericResultMessage(new MessageType(ObjectUtils.nullSafeTypeOf(payload)), payload);
    }

    @Test
    void exceptionalResult() {
        Throwable t = new Throwable("oops");
        ResultMessage resultMessage = asResultMessage(t);
        try {
            resultMessage.payload();
        } catch (IllegalPayloadAccessException ipae) {
            assertEquals(t, ipae.getCause());
        }
    }
}
