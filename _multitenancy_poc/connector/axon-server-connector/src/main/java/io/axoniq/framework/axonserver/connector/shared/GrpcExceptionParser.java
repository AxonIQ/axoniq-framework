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

package io.axoniq.framework.axonserver.connector.shared;

import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

/**
 * Converts GRPC Exceptions to {@link RuntimeException}s.
 *
 * @author Marc Gathier
 * @since 4.0
 */
public final class GrpcExceptionParser {

    private static final Metadata.Key<String> ERROR_CODE_KEY =
            Metadata.Key.of("AxonIQ-ErrorCode", Metadata.ASCII_STRING_MARSHALLER);

    /**
     * Convert the give {@code exception} into a {@link RuntimeException}.
     *
     * @param exception the {@link Throwable} to base the {@link RuntimeException}.
     * @return the {@link RuntimeException} based on the given {@code exception}
     */
    public static RuntimeException parse(Throwable exception) {
        String code = "AXONIQ-0001";
        if (exception instanceof StatusRuntimeException statusRuntimeException) {
            if (Status.Code.UNIMPLEMENTED.equals(statusRuntimeException.getStatus().getCode())) {
                return new UnsupportedOperationException(exception.getMessage(), exception);
            }
            Metadata trailer = statusRuntimeException.getTrailers();
            String errorCode = trailer == null ? null : trailer.get(ERROR_CODE_KEY);
            if (errorCode != null) {
                code = errorCode;
            }
        }

        return ExceptionFactory.convert(
                ErrorCode.getFromCode(code),
                exception
        );
    }

    private GrpcExceptionParser() {
        // Utility class, prevent instantiation.
    }
}
