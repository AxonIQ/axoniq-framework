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

package org.axonframework.messaging.unitofwork;

import org.axonframework.messaging.core.GenericResultMessage;
import org.axonframework.messaging.core.ResultMessage;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Rene de Waele
 */
class ExecutionResultTest {

    @Test
    void normalExecutionResult() {
        Object resultPayload = new Object();
        ResultMessage result = GenericResultMessage.asResultMessage(resultPayload);
        ExecutionResult subject = new ExecutionResult(result);
        assertSame(result, subject.getResult());
        assertFalse(subject.isExceptionResult());
        assertNull(subject.getExceptionResult());
    }

    @Test
    void uncheckedExceptionResult() {
        RuntimeException mockException = new RuntimeException();
        ResultMessage resultMessage = GenericResultMessage.asResultMessage(mockException);
        ExecutionResult subject = new ExecutionResult(resultMessage);
        assertTrue(subject.isExceptionResult());
        assertSame(mockException, subject.getExceptionResult());
        assertSame(mockException, subject.getResult().payload());
    }

    @Test
    void checkedExceptionResult() {
        Exception mockException = new Exception();
        ResultMessage resultMessage = GenericResultMessage.asResultMessage(mockException);
        ExecutionResult subject = new ExecutionResult(resultMessage);
        assertTrue(subject.isExceptionResult());
        assertSame(mockException, subject.getExceptionResult());
        assertSame(mockException, subject.getResult().payload());
    }
}