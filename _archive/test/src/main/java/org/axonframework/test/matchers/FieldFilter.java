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
 * The FieldFilter indicates whether any given Field should be accepted for processing or not.
 *
 * @author Allard Buijze
 * @since 2.4.1
 */
@FunctionalInterface
public interface FieldFilter {

    /**
     * Indicates whether the given {@code field} should be accepted for processing, or skipped/ignored.
     *
     * @param field The field to evaluate
     * @return {@code true} when the field should be processed, otherwise {@code false}
     */
    boolean accept(Field field);
}
