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
import org.axonframework.conversion.ChainingContentTypeConverter;
import org.axonframework.conversion.ConversionException;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.commandhandling.CommandExecutionException;
import org.junit.jupiter.api.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

/**
 * Test case validating the {@link ExceptionConverter}.
 */
class ExceptionConverterTest {

    @Test
    void convertToErrorMessageNullClientAndErrorCode() {
        ErrorMessage result = ExceptionConverter.convertToErrorMessage(null, null,
                                                                       new RuntimeException(
                                                                    "Something went wrong"));
        assertThat(result.getLocation()).isEmpty();
        assertThat(result.getErrorCode()).isEmpty();
    }

    @Test
    void convertToErrorMessageNonNullClientAndErrorCode() {
        ErrorMessage result = ExceptionConverter.convertToErrorMessage("Client", ErrorCode.QUERY_EXECUTION_ERROR,
                                                                       new RuntimeException(
                                                                    "Something went wrong"));
        assertThat(result.getLocation()).isEqualTo("Client");
        assertThat(result.getErrorCode()).isEqualTo("AXONIQ-5001");
    }

    @Nested
    class SerializeDetails {

        @Test
        void returnsNullWhenNoDetailsPresent() {
            SerializedObject result = ExceptionConverter.convertDetails(new RuntimeException("boom"),
                                                                        mock(Converter.class));

            assertThat(result).isNull();
        }

        @Test
        void returnsNullWhenDetailsPresentButNoConverterAvailable() {
            var cause = new CommandExecutionException("boom", null, "some details");

            SerializedObject result = ExceptionConverter.convertDetails(cause, null);

            assertThat(result).isNull();
        }

        @Test
        void returnsPopulatedSerializedObjectWhenConverterAvailable() {
            var cause = new CommandExecutionException("boom", null, "some details");
            Converter converter = mock(Converter.class);
            when(converter.convert("some details", byte[].class)).thenReturn("some details".getBytes());

            SerializedObject result = ExceptionConverter.convertDetails(cause, converter);

            assertThat(result).isNotNull();
            assertThat(result.getType()).isEqualTo(String.class.getName());
            assertThat(result.getData().toStringUtf8()).isEqualTo("some details");
        }

        @Test
        void returnsRawBytesDetailsWithoutConverter() {
            byte[] rawDetails = "raw bytes".getBytes();
            var cause = new CommandExecutionException("boom", null, rawDetails);

            SerializedObject result = ExceptionConverter.convertDetails(cause, null);

            assertThat(result).isNotNull();
            assertThat(result.getData().toByteArray()).isEqualTo(rawDetails);
        }

        @Test
        void returnsNullWhenConversionFails() {
            var cause = new CommandExecutionException("boom", null, "some details");
            Converter converter = mock(Converter.class);
            when(converter.convert("some details", byte[].class))
                    .thenThrow(new ConversionException("cannot convert"));

            SerializedObject result = ExceptionConverter.convertDetails(cause, converter);

            assertThat(result).isNull();
        }
    }

    @Nested
    class ConvertToAxonExceptionWithConverter {

        @Test
        void reconstructsDetailsLazilyThroughAttachedConverter() {
            var payload = SerializedObject.newBuilder()
                                          .setType(String.class.getName())
                                          .setData(ByteString.copyFromUtf8("raw"))
                                          .build();
            Converter converter = new ChainingContentTypeConverter();

            AxonException result = ExceptionConverter.convertToAxonException(
                    ErrorCode.COMMAND_EXECUTION_ERROR.errorCode(), ErrorMessage.newBuilder().setMessage("boom").build(),
                    payload, converter
            );

            assertThat(result).isInstanceOf(CommandExecutionException.class);
            String details = ((CommandExecutionException) result).getDetails(String.class).orElse(null);
            assertThat(details).isEqualTo("raw");
        }

        @Test
        void threeArgOverloadYieldsExceptionThatThrowsWithoutConverterOnMismatchedType() {
            var payload = SerializedObject.newBuilder()
                                          .setType(String.class.getName())
                                          .setData(ByteString.copyFromUtf8("raw"))
                                          .build();

            AxonException result = ExceptionConverter.convertToAxonException(
                    ErrorCode.COMMAND_EXECUTION_ERROR.errorCode(), ErrorMessage.newBuilder().setMessage("boom").build(),
                    payload
            );

            assertThat(result).isInstanceOf(CommandExecutionException.class);
            var commandExecutionException = (CommandExecutionException) result;
            assertThatThrownBy(() -> commandExecutionException.getDetails(Integer.class))
                    .isInstanceOf(ConversionException.class);
        }
    }
}