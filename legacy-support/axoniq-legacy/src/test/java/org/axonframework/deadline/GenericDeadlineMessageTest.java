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

package org.axonframework.deadline;

import org.axonframework.common.ObjectUtils;
import org.axonframework.common.TypeReference;
import org.axonframework.conversion.ChainingContentTypeConverter;
import org.axonframework.conversion.ConversionException;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageTestSuite;
import org.axonframework.messaging.core.MessageType;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

/**
 * Test class validating the {@link GenericDeadlineMessage}.
 *
 * @author Steven van Beelen
 */
class GenericDeadlineMessageTest extends MessageTestSuite<DeadlineMessage> {

    private static final String TEST_DEADLINE_NAME = "deadlineName";
    private static final Instant TEST_TIMESTAMP = Instant.now();

    @Override
    protected DeadlineMessage buildDefaultMessage() {
        Message delegate =
                new GenericMessage(TEST_IDENTIFIER, TEST_TYPE, TEST_PAYLOAD, TEST_PAYLOAD_TYPE, TEST_METADATA);
        return new GenericDeadlineMessage(TEST_DEADLINE_NAME, delegate, () -> TEST_TIMESTAMP);
    }

    @Override
    protected <P> DeadlineMessage buildMessage(@Nullable P payload) {
        return new GenericDeadlineMessage(TEST_DEADLINE_NAME,
                                          new MessageType(ObjectUtils.nullSafeTypeOf(payload)),
                                          payload);
    }

    @Override
    protected void validateDefaultMessage(@NonNull DeadlineMessage result) {
        assertThat(TEST_DEADLINE_NAME).isEqualTo(result.getDeadlineName());
        assertThat(TEST_TIMESTAMP).isEqualTo(result.timestamp());
    }

    @Override
    protected void validateMessageSpecifics(@NonNull DeadlineMessage actual, @NonNull DeadlineMessage result) {
        assertThat(actual.getDeadlineName()).isEqualTo(result.getDeadlineName());
        assertThat(actual.timestamp()).isEqualTo(result.timestamp());
    }

    @Nested
    class WithConverter {

        private static final String STRING_PAYLOAD = "payload";
        private static final byte[] BYTE_PAYLOAD = STRING_PAYLOAD.getBytes(StandardCharsets.UTF_8);

        private Converter converter;

        @BeforeEach
        void setUp() {
            converter = spy(new ChainingContentTypeConverter());
        }

        @AfterEach
        void tearDown() {
            verifyNoMoreInteractions(converter);
        }

        @Test
        void payloadAsClassReturnsPayloadWithoutConversionOnSameType() {
            // given
            GenericDeadlineMessage message = deadlineMessage(STRING_PAYLOAD).withConverter(converter);

            // when
            String result = message.payloadAs(String.class);

            // then
            assertThat(result).isEqualTo(STRING_PAYLOAD);
        }

        @Test
        void payloadAsClassInvokesConverterOnDifferentType() {
            // given
            GenericDeadlineMessage message = deadlineMessage(BYTE_PAYLOAD).withConverter(converter);

            // when
            String result = message.payloadAs(String.class);

            // then
            assertThat(result).isEqualTo(STRING_PAYLOAD);
            verify(converter).convert(BYTE_PAYLOAD, (Type) String.class);
        }

        @Test
        void payloadAsClassFailsWithConversionExceptionWithoutConverter() {
            // given
            GenericDeadlineMessage message = deadlineMessage(BYTE_PAYLOAD);

            // when / then
            assertThatThrownBy(() -> message.payloadAs(Integer.class)).isInstanceOf(ConversionException.class);
        }

        @Test
        void payloadAsTypeRefInvokesConverter() {
            // given
            GenericDeadlineMessage message = deadlineMessage(BYTE_PAYLOAD).withConverter(converter);

            // when
            String result = message.payloadAs(new TypeReference<String>() {
            });

            // then
            assertThat(result).isEqualTo(STRING_PAYLOAD);
            verify(converter).convert(BYTE_PAYLOAD, (Type) String.class);
        }

        @Test
        void payloadAsCachesConversion() {
            // given
            GenericDeadlineMessage message = deadlineMessage(BYTE_PAYLOAD).withConverter(converter);

            // when
            String result = message.payloadAs(String.class);
            String secondResult = message.payloadAs(String.class);

            // then
            assertThat(result).isEqualTo(STRING_PAYLOAD);
            assertThat(secondResult).isEqualTo(STRING_PAYLOAD);
            verify(converter, times(1)).convert(BYTE_PAYLOAD, (Type) String.class);
        }

        @Test
        void withConverterReturnsNewInstanceOfSameConcreteType() {
            // given
            GenericDeadlineMessage original = deadlineMessage(BYTE_PAYLOAD);

            // when
            GenericDeadlineMessage result = original.withConverter(converter);

            // then
            assertThat(result)
                    .isInstanceOf(GenericDeadlineMessage.class)
                    .isNotSameAs(original);
        }

        @Test
        void withConverterPreservesMembers() {
            // given
            GenericDeadlineMessage original = deadlineMessage(BYTE_PAYLOAD);

            // when
            GenericDeadlineMessage result = original.withConverter(converter);

            // then
            assertThat(result).isNotSameAs(original);
            assertThat(result.getDeadlineName()).isEqualTo(original.getDeadlineName());
            assertThat(result.identifier()).isEqualTo(original.identifier());
            assertThat(result.type()).isEqualTo(original.type());
            assertThat(result.payload()).isEqualTo(original.payload());
            assertThat(result.payloadType()).isEqualTo(original.payloadType());
            assertThat(result.metadata()).isEqualTo(original.metadata());
            assertThat(result.timestamp()).isEqualTo(original.timestamp());
        }

        private static GenericDeadlineMessage deadlineMessage(Object payload) {
            return new GenericDeadlineMessage(TEST_DEADLINE_NAME, new MessageType(payload.getClass()), payload);
        }
    }
}
