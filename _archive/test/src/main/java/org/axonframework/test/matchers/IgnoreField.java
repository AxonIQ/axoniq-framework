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

import org.axonframework.test.FixtureExecutionException;

import java.lang.reflect.Field;

/**
 * FieldFilter implementation that rejects a given Field
 *
 * @author Allard Buijze
 * @since 2.4.1
 */
public class IgnoreField implements FieldFilter {

    private Field ignoredField;

    /**
     * Initialize an instance that ignores the given {@code field}
     *
     * @param field The field to ignore
     */
    public IgnoreField(Field field) {
        this.ignoredField = field;
    }

    /**
     * Initialize an instance that ignores the a field with given {@code fieldName}, which is declared on the
     * given
     * {@code clazz}.
     *
     * @param clazz     The type that declares the field
     * @param fieldName The name of the field
     * @throws FixtureExecutionException when the given fieldName is not declared on given clazz.
     */
    public IgnoreField(Class<?> clazz, String fieldName) {
        try {
            ignoredField = clazz.getDeclaredField(fieldName);
        } catch (NoSuchFieldException e) {
            throw new FixtureExecutionException("The given field does not exist", e);
        }
    }

    @Override
    public boolean accept(Field field) {
        return !field.equals(ignoredField);
    }
}
