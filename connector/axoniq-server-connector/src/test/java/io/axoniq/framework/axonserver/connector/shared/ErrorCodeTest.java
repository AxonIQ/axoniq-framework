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

package io.axoniq.framework.axonserver.connector.shared;

import io.axoniq.axonserver.grpc.ErrorMessage;
import io.axoniq.framework.axonserver.connector.api.AxonServerException;
import io.axoniq.framework.axonserver.connector.api.command.AxonServerNonTransientRemoteCommandHandlingException;
import io.axoniq.framework.axonserver.connector.api.query.AxonServerNonTransientRemoteQueryHandlingException;

import org.axonframework.messaging.commandhandling.CommandExecutionException;
import org.axonframework.common.AxonException;
import org.axonframework.messaging.queryhandling.QueryExecutionException;
import org.axonframework.conversion.ConversionException;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;


/**
 * Author: marc
 */
class ErrorCodeTest {

    @Test
    void convert4002FromCodeAndMessage() {
        ErrorCode errorCode = ErrorCode.getFromCode("AXONIQ-4002");
        AxonException exception = ExceptionFactory.convert(errorCode, ErrorMessage.newBuilder().setMessage("myMessage").build(),
                                                    () -> "myCustomObject");
        assertTrue(exception instanceof CommandExecutionException);
        assertEquals("myMessage", exception.getMessage());
        assertEquals("myCustomObject", ((CommandExecutionException) exception).getDetails().orElse("null"));
    }

    @Test
    void convertUnknownFromCodeAndMessage() {
        ErrorCode errorCode = ErrorCode.getFromCode("????????");
        AxonException exception = ExceptionFactory.convert(errorCode, ErrorMessage.newBuilder().setMessage("myMessage").build());
        assertTrue(exception instanceof AxonServerException);
        assertEquals("myMessage", exception.getMessage());
    }

    @Test
    void convertWithoutSource() {
        RuntimeException exception = new RuntimeException("oops");
        AxonException axonException = ExceptionFactory.convert(ErrorCode.getFromCode("AXONIQ-4002"), exception);
        assertEquals(exception.getMessage(), axonException.getMessage());
    }

    @Test
    void convert4005FromCodeAndMessage() {
        ErrorCode errorCode = ErrorCode.getFromCode("AXONIQ-4005");
        AxonException exception = ExceptionFactory.convert(errorCode, ErrorMessage.newBuilder().setMessage("myMessage").build(),
                                                    () -> "myCustomObject");
        assertTrue(exception instanceof CommandExecutionException);
        assertTrue(exception.getCause() instanceof AxonServerNonTransientRemoteCommandHandlingException);
        assertEquals("myMessage", exception.getMessage());
        assertEquals("myCustomObject", ((CommandExecutionException) exception).getDetails().orElse("null"));
    }

    @Test
    void convert5003FromCodeAndMessage() {
        ErrorCode errorCode = ErrorCode.getFromCode("AXONIQ-5003");
        AxonException exception = ExceptionFactory.convert(errorCode, ErrorMessage.newBuilder().setMessage("myMessage").build(),
                                                    () -> "myCustomObject");
        assertTrue(exception instanceof QueryExecutionException);
        assertTrue(exception.getCause() instanceof AxonServerNonTransientRemoteQueryHandlingException);
        assertEquals("myMessage", exception.getMessage());
        assertEquals("myCustomObject", ((QueryExecutionException) exception).getDetails().orElse("null"));
    }

    @Test
    void queryExecutionErrorCodeFromNonTransientException() {
        ErrorCode errorCode = ErrorCode.getQueryExecutionErrorCode(new ConversionException("Fake exception"));
        assertEquals(ErrorCode.QUERY_EXECUTION_NON_TRANSIENT_ERROR, errorCode);
    }

    @Test
    void queryExecutionErrorCodeFromRuntimeException() {
        ErrorCode errorCode = ErrorCode.getQueryExecutionErrorCode(new RuntimeException("Fake exception"));
        assertEquals(ErrorCode.QUERY_EXECUTION_ERROR, errorCode);
    }

    @Test
    void commandExecutionErrorCodeFromNonTransientException() {
        ErrorCode errorCode = ErrorCode.getCommandExecutionErrorCode(new ConversionException("Fake exception"));
        assertEquals(ErrorCode.COMMAND_EXECUTION_NON_TRANSIENT_ERROR, errorCode);
    }

    @Test
    void commandExecutionErrorCodeFromRuntimeException() {
        ErrorCode errorCode = ErrorCode.getCommandExecutionErrorCode(new RuntimeException("Fake exception"));
        assertEquals(ErrorCode.COMMAND_EXECUTION_ERROR, errorCode);
    }
}
