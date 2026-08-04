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
import org.axonframework.common.AxonException;
import org.axonframework.common.annotation.Internal;
import org.axonframework.conversion.ConversionException;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.core.HandlerExecutionException;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.function.Predicate;

import static org.axonframework.common.ObjectUtils.getOrDefault;

/**
 * Utility class used to serialize {@link Throwable Throwables} into {@link ErrorMessage ErrorMessages}, and to
 * reconstruct {@link AxonException AxonExceptions} from error information received over the wire.
 * <p>
 * This class is used by the command and query connector implementations; it is not meant to be used directly by
 * application code.
 *
 * @author Marc Gathier
 * @since 4.0.0
 */
@Internal
public final class ExceptionConverter {

    private static final Logger logger = LoggerFactory.getLogger(ExceptionConverter.class);

    /**
     * Converts the given error information into an {@link AxonException}, attaching the given {@code converter} so that
     * the resulting exception's details can be converted, on request, into whatever type the caller needs (see
     * {@code HandlerExecutionException#getDetails(Class)}).
     * <p>
     * The {@code payload}'s raw bytes are not deserialized here. Deserialization is deferred until a caller asks for a
     * specific details type, decoupling the thrower's details class (and version) from the receiver's.
     *
     * @param errorCode    the error code identifying the type of exception to convert to
     * @param errorMessage the {@link ErrorMessage} containing details of the error
     * @param payload      an optional {@link SerializedObject} representing additional error data, which may be used in
     *                     the exception conversion
     * @param converter    the {@link Converter} to attach to the resulting exception, so that details can be converted
     *                     lazily, or {@code null} if no such conversion is available
     * @return an instance of {@link AxonException} representing the given error information
     */
    public static AxonException convertToAxonException(String errorCode,
                                                       ErrorMessage errorMessage,
                                                       SerializedObject payload,
                                                       @Nullable Converter converter) {
        return ExceptionFactory.convert(
                ErrorCode.getFromCode(errorCode),
                errorMessage,
                () -> Optional.of(payload)
                              .map(SerializedObject::getData)
                              .filter(Predicate.not(ByteString::isEmpty))
                              .map(ByteString::toByteArray)
                              .orElse(null),
                converter
        );
    }

    /**
     * Serializes a given {@link Throwable} into an {@link ErrorMessage}.
     *
     * @param clientLocation the name of the client were the {@link ErrorMessage} originates from
     * @param errorCode      the error code identifying the type of action that resulted in an error, if known
     * @param t              the {@link Throwable} to base this {@link ErrorMessage} on
     * @return the {@link ErrorMessage} originating from the given {@code clientLocation} and based on the
     * {@link Throwable}
     */
    public static ErrorMessage convertToErrorMessage(String clientLocation,
                                                     @Nullable ErrorCode errorCode,
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

    /**
     * Converts the application-specific details carried by a {@link HandlerExecutionException} in the cause chain of
     * the given {@code cause}, if any, into a {@link SerializedObject} suitable for putting on the wire.
     * <p>
     * The {@code type}/{@code revision} on the resulting {@link SerializedObject} carry the runtime class name of the
     * details object purely for wire-level observability/tracing. They are not used to reconstruct the details on the
     * receiving side, since reconstruction is driven entirely by the type the receiver asks for.
     *
     * @param cause     the exception to resolve application-specific details from
     * @param converter the {@link Converter} used to serialize the details into bytes, or {@code null} if none is
     *                  available
     * @return a {@link SerializedObject} carrying the serialized details, or {@code null} if there are no details, or
     * the details could not be serialized
     */
    public static @Nullable SerializedObject convertDetails(Throwable cause,
                                                            @Nullable Converter converter) {
        Object details = HandlerExecutionException.resolveDetails(cause).orElse(null);
        if (details == null) {
            return null;
        }

        byte[] data;
        if (details instanceof byte[] rawDetails) {
            data = rawDetails;
        } else if (converter == null) {
            logger.debug("Cannot converter exception details of type [{}] due to given null Converter.",
                         details.getClass().getName());
            return null;
        } else {
            try {
                data = converter.convert(details, byte[].class);
            } catch (ConversionException e) {
                logger.debug("Could not serialize exception details of type [{}]; omitting them from the response.",
                             details.getClass().getName(), e);
                return null;
            }
            if (data == null) {
                return null;
            }
        }

        return SerializedObject.newBuilder()
                               .setType(details.getClass().getName())
                               .setData(ByteString.copyFrom(data))
                               .build();
    }

    private ExceptionConverter() {
        // Utility class
    }
}
