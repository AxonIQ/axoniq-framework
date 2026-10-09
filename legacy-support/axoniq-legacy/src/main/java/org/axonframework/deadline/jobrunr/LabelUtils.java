/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */

package org.axonframework.deadline.jobrunr;

import org.axonframework.common.digest.Digester;
import org.axonframework.deadline.StoredDeadlineConverter;
import org.axonframework.messaging.ScopeDescriptor;

import java.util.Objects;

/**
 * Utility class to create the labels of the JobRunr jobs that fire deadlines. JobRunr limits a label's length, so a
 * longer label is replaced by its MD5 hash.
 *
 * @author Gerard Klijs
 * @since 4.8.0
 */
public abstract class LabelUtils {

    private static final int MAX_LENGTH = 44;

    private LabelUtils() {
        //prevent instantiation
    }

    /**
     * Returns the given {@code input} as a label: unchanged if it is at most 44 characters long, and as its MD5 hash
     * otherwise.
     *
     * @param input the text to turn into a label
     * @return the label for the given {@code input}
     */
    public static String getLabel(String input) {
        if (input.length() <= MAX_LENGTH) {
            return input;
        } else {
            return Digester.md5Hex(input);
        }
    }

    private static String getScopeLabel(StoredDeadlineConverter converter, ScopeDescriptor scope) {
        return getLabel(Objects.requireNonNull(converter.toStored(scope, String.class)));
    }

    /**
     * Creates a label from a scope and a deadline name, using the stored form of the scope.
     *
     * @param converter    the converter producing the stored form of the given {@code scope}
     * @param deadlineName the name of the deadline
     * @param scope        the {@link ScopeDescriptor} of the deadline
     * @return the label combining the deadline name and the scope
     */
    public static String getCombinedLabel(StoredDeadlineConverter converter, String deadlineName,
                                          ScopeDescriptor scope) {
        String scopeLabel = getScopeLabel(converter, scope);
        return getLabel(deadlineName + scopeLabel);
    }
}
