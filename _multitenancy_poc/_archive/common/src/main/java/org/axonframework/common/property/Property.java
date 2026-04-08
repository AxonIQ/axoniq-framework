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

/**
 * Interface describing a mechanism that can read a predefined property from a given instance.
 *
 * @param <T> The type of object defining this property
 * @author Maxim Fedorov
 * @author Allard Buijze
 * @since 2.0
 */
@FunctionalInterface
public interface Property<T> {

    /**
     * Returns the value of the property on given {@code target}.
     *
     * @param target The instance to get the property value from
     * @param <V>    The type of value expected
     * @return the property value on {@code target}
     */
    <V> @Nullable V getValue(T target);
}
