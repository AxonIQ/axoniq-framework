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

import org.jspecify.annotations.Nullable;

import static java.lang.String.format;
import static java.util.Locale.ENGLISH;

/**
 * BeanPropertyAccessStrategy implementation that uses JavaBean style property access. This means that for any given
 * property 'property', a method "getProperty" is expected to provide the property value
 *
 * @author Maxim Fedorov
 * @author Allard Buijze
 * @since 2.0
 */
public class BeanPropertyAccessStrategy extends AbstractMethodPropertyAccessStrategy {

    @Override
    protected String getterName(@Nullable String property) {
return format(ENGLISH, "get%S%s", property.charAt(0), property.substring(1));
    }

    @Override
    protected int getPriority() {
        return 0;
    }
}
