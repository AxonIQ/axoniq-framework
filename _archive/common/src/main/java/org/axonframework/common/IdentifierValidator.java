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

import java.util.Map;
import java.util.WeakHashMap;

import static org.axonframework.common.ReflectionUtils.declaringClass;

/**
 * Validates the structure of an object passed as Aggregate Identifier. These objects need to (properly) override
 * the {@code toString} method. Two equal identifiers must always produce the same {@code toString} values,
 * even between JVM restarts. Typically, this also means {@code equals} and {@code hashCode} need to be
 * implemented.
 * <p/>
 * For optimization purposes, this validator keeps a white-list of all aggregate types that have passed validation.
 * This
 * reduces the amount of reflection for types that have been already inspected.
 *
 * @author Allard Buijze
 * @since 2.0
 */
public final class IdentifierValidator {

    private static final IdentifierValidator INSTANCE = new IdentifierValidator();
    private static final Object NULL = new Object();

    private final Map<Class<?>, Object> whiteList = new WeakHashMap<>();

    private IdentifierValidator() {
        // Singleton, prevent construction
    }

    /**
     * Returns the singleton instance of the IdentifierValidator.
     *
     * @return the IdentifierValidator instance
     */
    public static IdentifierValidator getInstance() {
        return INSTANCE;
    }

    /**
     * Indicates whether or not the given {@code identifierType} is safe to use as aggregate identifier
     *
     * @param identifierType The class of the identifier
     * @return {@code true} if the identifier is valid, {@code false} otherwise
     */
    public boolean isValidIdentifier(Class<?> identifierType) {
        if (!whiteList.containsKey(identifierType)) {
            if (Object.class.equals(declaringClass(identifierType, "toString"))) {
                return false;
            }
            whiteList.put(identifierType, NULL);
        }
        return true;
    }
}
