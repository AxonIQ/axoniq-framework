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

package org.axonframework.conversion.upcasting.event;

import com.fasterxml.jackson.databind.JsonNode;
import org.axonframework.conversion.SerializedType;
import org.axonframework.conversion.Serializer;
import org.axonframework.conversion.TestSerializer;
import org.junit.jupiter.params.*;
import org.junit.jupiter.params.provider.*;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit test class validating the {@link UpcastedEventRepresentation}.
 *
 * @author Steven van Beelen
 */
class UpcastedEventRepresentationTest {

    private static final String SOURCE_METHOD_NAME = "serializer";

    @SuppressWarnings("unused") // Used by parameterized test "testContentType"
    private static Stream<Arguments> serializer() {
        return Stream.of(
                Arguments.of(TestSerializer.JACKSON.getSerializer()),
                Arguments.of(TestSerializer.JACKSON_ONLY_ACCEPT_CONSTRUCTOR_PARAMETERS.getSerializer())
        );
    }

    @ParameterizedTest
    @MethodSource(SOURCE_METHOD_NAME)
    void contentType(Serializer serializer) {
        Class<JsonNode> expectedContentType = JsonNode.class;

        UpcastedEventRepresentation<JsonNode> testSubject = new UpcastedEventRepresentation<>(
                mock(SerializedType.class), mock(IntermediateEventRepresentation.class),
                o -> o, m -> m, expectedContentType, serializer.getConverter()
        );

        assertEquals(expectedContentType, testSubject.getContentType());
    }
}