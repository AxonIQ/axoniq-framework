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

package org.axonframework.common;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Utility methods for operations on lists.
 *
 * @author Stefan Andjelkovic
 * @since 4.4
 */
public final class ListUtils {

    private ListUtils() {
        // prevent instantiation
    }

    /**
     * Returns a new list containing unique elements from the given {@code list}. Original list is not modified.
     *
     * @param list original list that will not be modified
     * @param <E>  the type of elements in the list
     * @return List with unique elements
     */
    public static <E> List<E> distinct(final List<E> list) {
        return list.stream().distinct().collect(Collectors.toList());
    }
}
