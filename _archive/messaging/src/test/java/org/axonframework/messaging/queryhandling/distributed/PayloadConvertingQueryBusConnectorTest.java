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

package org.axonframework.messaging.queryhandling.distributed;

import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.conversion.DelegatingMessageConverter;
import org.axonframework.conversion.Converter;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class PayloadConvertingQueryBusConnectorTest {

    private static final String ORIGINAL_PAYLOAD = "original";
    private static final byte[] CONVERTED_PAYLOAD = ORIGINAL_PAYLOAD.getBytes();
    private static final MessageType QUERY_TYPE = new MessageType("TestQuery");
    private static final MessageType RESPONSE_TYPE = new MessageType("TestResponse");

    private QueryBusConnector mockDelegate;
    private Converter mockConverter;
    private PayloadConvertingQueryBusConnector testSubject;

    @BeforeEach
    void setUp() {
        mockDelegate = mock(QueryBusConnector.class);
        mockConverter = mock(Converter.class);
        testSubject = new PayloadConvertingQueryBusConnector(
                mockDelegate, new DelegatingMessageConverter(mockConverter), byte[].class
        );
    }

    @Test
    void constructorRequiresNonNullDelegate() {
        //noinspection DataFlowIssue
        assertThrows(NullPointerException.class, () -> new PayloadConvertingQueryBusConnector(
                null, new DelegatingMessageConverter(mockConverter), byte[].class
        ));
    }

    @Test
    void constructorRequiresNonNullConverter() {
        //noinspection DataFlowIssue
        assertThrows(NullPointerException.class,
                     () -> new PayloadConvertingQueryBusConnector(mockDelegate, null, byte[].class));
    }

    @Test
    void constructorRequiresNonNullTargetType() {
        //noinspection DataFlowIssue
        assertThrows(NullPointerException.class, () -> new PayloadConvertingQueryBusConnector(
                mockDelegate, new DelegatingMessageConverter(mockConverter), null
        ));
    }
}
