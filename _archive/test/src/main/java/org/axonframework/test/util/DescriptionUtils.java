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

package org.axonframework.test.util;

import org.hamcrest.Description;

import java.util.List;

/**
 * Utility class for creating a description.
 *
 * @author Allard Buijze
 * @since 1.1
 */
public abstract class DescriptionUtils {

    private DescriptionUtils() {
    }

    /**
     * Describe the contents of the given {@code list} in the given {@code description}.
     *
     * @param list        The list to describe
     * @param description The description to describe to
     */
    public static void describe(List<?> list, Description description) {
        int counter = 0;
        description.appendText("List with ");
        for (Object item : list) {
            description.appendText("<")
                       .appendText(item != null ? item.toString() : "null")
                       .appendText(">");
            if (counter == list.size() - 2) {
                description.appendText(" and ");
            } else if (counter < list.size() - 2) {
                description.appendText(", ");
            }
            counter++;
        }
    }
}
