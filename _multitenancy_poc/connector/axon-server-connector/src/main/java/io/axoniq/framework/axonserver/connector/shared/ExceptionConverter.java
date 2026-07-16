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

import com.google.protobuf.ByteString;
import io.axoniq.axonserver.grpc.ErrorMessage;
import io.axoniq.axonserver.grpc.SerializedObject;

import org.jspecify.annotations.Nullable;
import org.axonframework.common.AxonException;

import java.util.Optional;
import java.util.function.Predicate;

import static org.axonframework.common.ObjectUtils.getOrDefault;

/**
 * Utility class used to serializer {@link Throwable}s into {@link ErrorMessage}s.
 *
 * @author Marc Gathier
 * @since 4.0
 */
public final class ExceptionConverter {

    /**
     * Converts the given error information into an {@link AxonException}.
     *
     * @param errorCode     The error code identifying the type of exception to convert to.
     * @param errorMessage  The {@link ErrorMessage} containing details of the error.
     * @param payload       An optional {@link SerializedObject} representing additional error data, which may
     *                      be used in the exception conversion.
     * @return An instance of {@link AxonException} representing the given error information.
     */
    public static AxonException convertToAxonException(String errorCode, ErrorMessage errorMessage, SerializedObject payload) {
        return ExceptionFactory.convert(
                ErrorCode.getFromCode(errorCode),
                errorMessage,
                () -> Optional.ofNullable(payload).map(SerializedObject::getData)
                        .filter(Predicate.not(ByteString::isEmpty))
                        .map(ByteString::toByteArray)
                        .orElse(null)
        );
    }

    /**
     * Serializes a given {@link Throwable} into an {@link ErrorMessage}.
     *
     * @param clientLocation the name of the client were the {@link ErrorMessage} originates from
     * @param errorCode The error code identifying the type of action that resulted in an error, if known
     * @param t              the {@link Throwable} to base this {@link ErrorMessage} on
     * @return the {@link ErrorMessage} originating from the given {@code clientLocation} and based on the
     * {@link Throwable}
     */
    public static ErrorMessage convertToErrorMessage(String clientLocation, @Nullable ErrorCode errorCode,
                                                     Throwable t) {
        ErrorMessage.Builder builder =
                ErrorMessage.newBuilder()
                            .setLocation(getOrDefault(clientLocation, ""))
                            .setMessage(t.getMessage() == null ? t.getClass().getName() : t.getMessage());
        if (errorCode != null) {
            builder.setErrorCode(errorCode.errorCode());
        }
        builder.addDetails(t.getMessage() == null ? t.getClass().getName() : t.getMessage());
        while (t.getCause() != null) {
            t = t.getCause();
            builder.addDetails(t.getMessage() == null ? t.getClass().getName() : t.getMessage());
        }
        return builder.build();
    }

    private ExceptionConverter() {
        // Utility class
    }
}
