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

package org.axonframework.test.matchers;

import java.lang.reflect.Field;

/**
 * FieldFilter implementation that accepts all Fields.
 *
 * @author Allard Buijze
 * @since 2.4.1
 */
public class AllFieldsFilter implements FieldFilter {

    private static final AllFieldsFilter INSTANCE = new AllFieldsFilter();

    @Override
    public boolean accept(Field field) {
        return true;
    }

    private AllFieldsFilter() {
    }

    /**
     * Returns the (singleton) instance of the AllFieldsFilter
     *
     * @return an AllFieldsFilter instance
     */
    public static AllFieldsFilter instance() {
        return INSTANCE;
    }
}
