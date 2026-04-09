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

import static java.util.Arrays.stream;

import java.util.Objects;

import org.axonframework.common.ExceptionUtils;

/**
 * Converts an Axon Server Error to the relevant Axon framework exception.
 *
 * @author Marc Gathier
 * @since 4.0
 */
public enum ErrorCode {

    // Generic errors processing client request
    AUTHENTICATION_TOKEN_MISSING("AXONIQ-1000"),
    AUTHENTICATION_INVALID_TOKEN("AXONIQ-1001"),
    UNSUPPORTED_INSTRUCTION("AXONIQ-1002"),
    INSTRUCTION_ACK_ERROR("AXONIQ-1003"),
    INSTRUCTION_EXECUTION_ERROR("AXONIQ-1004"),

    // Event publishing errors
    INVALID_EVENT_SEQUENCE("AXONIQ-2000"),
    NO_EVENT_STORE_MASTER_AVAILABLE("AXONIQ-2100"),
    EVENT_PAYLOAD_TOO_LARGE("AXONIQ-2001"),

    // Communication errors
    CONNECTION_FAILED("AXONIQ-3001"),
    GRPC_MESSAGE_TOO_LARGE("AXONIQ-3002"),

    // Command errors
    NO_HANDLER_FOR_COMMAND("AXONIQ-4000"),
    COMMAND_EXECUTION_ERROR("AXONIQ-4002"),
    COMMAND_DISPATCH_ERROR("AXONIQ-4003"),
    CONCURRENCY_EXCEPTION("AXONIQ-4004"),
    COMMAND_EXECUTION_NON_TRANSIENT_ERROR("AXONIQ-4005"),

    // Query errors
    NO_HANDLER_FOR_QUERY("AXONIQ-5000"),
    QUERY_EXECUTION_ERROR("AXONIQ-5001"),
    QUERY_DISPATCH_ERROR("AXONIQ-5002"),
    QUERY_EXECUTION_NON_TRANSIENT_ERROR("AXONIQ-5003"),

    // Internal errors
    DATAFILE_READ_ERROR("AXONIQ-9000"),
    INDEX_READ_ERROR("AXONIQ-9001"),
    DATAFILE_WRITE_ERROR("AXONIQ-9100"),
    INDEX_WRITE_ERROR("AXONIQ-9101"),
    DIRECTORY_CREATION_FAILED("AXONIQ-9102"),
    VALIDATION_FAILED("AXONIQ-9200"),
    TRANSACTION_ROLLED_BACK("AXONIQ-9900"),

    // Default
    OTHER("AXONIQ-0001");

    private final String errorCode;

    /**
     * Converts the given code to an {@link ErrorCode} enum value. {@link ErrorCode#OTHER} is
     * returned if no match was found.
     *
     * @param code a code to attempt to convert
     * @return the matching {@link ErrorCode} or {@link ErrorCode#OTHER} if no match was found
     */
    public static ErrorCode getFromCode(String code) {
        return stream(values()).filter(value -> value.errorCode.equals(code)).findFirst().orElse(OTHER);
    }

    /**
     * Initializes the ErrorCode using the given {@code code}.
     *
     * @param errorCode the code of the error
     */
    ErrorCode(String errorCode) {
        this.errorCode = Objects.requireNonNull(errorCode, "errorCode");
    }

    /**
     * Returns the Axon Server code.
     *
     * @return the Axon Server code, never {@code null}
     */
    public String errorCode() {
        return errorCode;
    }

    /**
     * Returns a Query Execution ErrorCode variation based on the transiency of the given {@link Throwable}
     *
     * @param throwable The {@link Throwable} to inspect for transiency
     * @return {@link ErrorCode} variation
     */
    public static ErrorCode getQueryExecutionErrorCode(Throwable throwable) {
        if (ExceptionUtils.isExplicitlyNonTransient(throwable)) {
            return ErrorCode.QUERY_EXECUTION_NON_TRANSIENT_ERROR;
        } else {
            return ErrorCode.QUERY_EXECUTION_ERROR;
        }
    }

    /**
     * Returns an Command Execution ErrorCode variation based on the transiency of the given {@link Throwable}
     *
     * @param throwable The {@link Throwable} to inspect for transiency
     * @return {@link ErrorCode} variation
     */
    public static ErrorCode getCommandExecutionErrorCode(Throwable throwable) {
        if (ExceptionUtils.isExplicitlyNonTransient(throwable)) {
            return ErrorCode.COMMAND_EXECUTION_NON_TRANSIENT_ERROR;
        } else {
            return ErrorCode.COMMAND_EXECUTION_ERROR;
        }
    }
}
