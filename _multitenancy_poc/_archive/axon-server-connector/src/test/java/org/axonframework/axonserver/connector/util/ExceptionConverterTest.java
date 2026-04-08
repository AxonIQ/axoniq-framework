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

package org.axonframework.axonserver.connector.util;

import io.axoniq.axonserver.grpc.ErrorMessage;
import org.axonframework.axonserver.connector.ErrorCode;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Author: marc
 */
class ExceptionConverterTest {
    @Test
    void convertToErrorMessageNullClientAndErrorCode() {
        ErrorMessage result = ExceptionConverter.convertToErrorMessage(null, null,
                                                                       new RuntimeException(
                                                                    "Something went wrong"));
        assertEquals("", result.getLocation());
        assertEquals("", result.getErrorCode());
    }

    @Test
    void convertToErrorMessageNonNullClientAndErrorCode() {
        ErrorMessage result = ExceptionConverter.convertToErrorMessage("Client", ErrorCode.QUERY_EXECUTION_ERROR,
                                                                       new RuntimeException(
                                                                    "Something went wrong"));
        assertEquals("Client", result.getLocation());
        assertEquals("AXONIQ-5001", result.getErrorCode());
    }
}