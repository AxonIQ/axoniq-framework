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

package org.axonframework.common.property;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;


public abstract class AbstractPropertyAccessStrategyTest<T> {

    @Test
    void getValue() {
        final Property<T> actualProperty = getProperty(regularPropertyName());
        assertNotNull(actualProperty);
        assertNotNull(actualProperty.<String>getValue(propertyHoldingInstance()));
    }

    @Test
    void getValue_BogusProperty() {
        assertNull(getProperty(unknownPropertyName()));
    }

    @Test
    void getValue_ExceptionOnAccess() {
        Property<T> property = getProperty(exceptionPropertyName());

        assertThrows(PropertyAccessException.class, () -> property.getValue(propertyHoldingInstance()));
    }

    @Test
    void voidReturnTypeRejected() {
        Property property = getProperty(voidPropertyName());
        assertNull(property, "void methods should not be accepted as property");
    }

    protected abstract String voidPropertyName();

    protected abstract String exceptionPropertyName();
    protected abstract String regularPropertyName();
    protected abstract String unknownPropertyName();

    protected abstract T propertyHoldingInstance();

    protected abstract Property<T> getProperty(String property);
}
