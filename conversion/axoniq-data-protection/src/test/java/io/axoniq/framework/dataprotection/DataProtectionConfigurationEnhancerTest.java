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
package io.axoniq.framework.dataprotection;

import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.DecoratorDefinition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Test class validating the {@link DataProtectionConfigurationEnhancer}.
 *
 * @author Steven van Beelen
 */
@ExtendWith(MockitoExtension.class)
class DataProtectionConfigurationEnhancerTest {

    private DataProtectionConfigurationEnhancer testSubject;

    @Mock
    private ComponentRegistry componentRegistry;

    @BeforeEach
    void setUp() {
        testSubject = new DataProtectionConfigurationEnhancer();
    }

    @Test
    void enhanceRegistersDecoratorForCryptoEngine() {
        testSubject.enhance(componentRegistry);

        verify(componentRegistry).registerDecorator(any(DecoratorDefinition.class));
    }

    @Test
    void orderReturnsDefaultValue() {
        assertEquals(0, testSubject.order());
    }
}
