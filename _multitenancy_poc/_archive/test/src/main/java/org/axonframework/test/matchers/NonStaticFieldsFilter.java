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
import java.lang.reflect.Modifier;

/**
 * FieldFilter implementation that only accepts non-static Fields.
 *
 * @author bliessens
 * @since 3.3
 */
public class NonStaticFieldsFilter implements FieldFilter {

    private static final NonStaticFieldsFilter INSTANCE = new NonStaticFieldsFilter();

    public static NonStaticFieldsFilter instance() {
        return INSTANCE;
    }

    private NonStaticFieldsFilter() {
    }

    @Override
    public boolean accept(Field field) {
        return !Modifier.isStatic(field.getModifiers());
    }

}
