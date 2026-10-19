/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.queryhandling.distributed;

import org.axonframework.conversion.Converter;
import org.axonframework.messaging.core.conversion.DelegatingMessageConverter;
import org.junit.jupiter.api.*;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

class PayloadConvertingQueryBusConnectorTest {

    private static final String ORIGINAL_PAYLOAD = "original";

    private QueryBusConnector mockDelegate;
    private Converter mockConverter;

    @BeforeEach
    void setUp() {
        mockDelegate = mock(QueryBusConnector.class);
        mockConverter = mock(Converter.class);
    }

    @Test
    void constructorRequiresNonNullDelegate() {
        //noinspection DataFlowIssue
        assertThatThrownBy(() -> new PayloadConvertingQueryBusConnector(
                null, new DelegatingMessageConverter(mockConverter), byte[].class
        )).isInstanceOf(NullPointerException.class);
    }

    @Test
    void constructorRequiresNonNullConverter() {
        //noinspection DataFlowIssue
        assertThatThrownBy(() -> new PayloadConvertingQueryBusConnector(
                mockDelegate, null, byte[].class
        )).isInstanceOf(NullPointerException.class);
    }

    @Test
    void constructorRequiresNonNullTargetType() {
        //noinspection DataFlowIssue
        assertThatThrownBy(() -> new PayloadConvertingQueryBusConnector(
                mockDelegate, new DelegatingMessageConverter(mockConverter), null
        )).isInstanceOf(NullPointerException.class);
    }
}
