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

package io.axoniq.framework.extension.dataprotection.internal.utils;

import java.util.function.Supplier;

/**
 * Utility class (inspired by Springs Assert class) for doing assertions on parameters and object state. To remove the
 * need for explicit dependencies on Spring, the functionality of that class is migrated to this class.
 *
 * @author Allard Buijze
 * @since 0.3
 */
public abstract class Assert {

    private Assert() {
        // utility class
    }

    /**
     * Asserts that the value of {@code state} is true. If not, an IllegalStateException is thrown.
     *
     * @param state           the state validation expression
     * @param messageSupplier Supplier of the exception message if state evaluates to false
     */
    public static void state(boolean state, Supplier<String> messageSupplier) {
        if (!state) {
            throw new IllegalStateException(messageSupplier.get());
        }
    }

    /**
     * Asserts that the given {@code expression} is true. If not, an IllegalArgumentException is thrown.
     *
     * @param expression      the state validation expression
     * @param messageSupplier Supplier of the exception message if the expression evaluates to false
     */
    public static void isTrue(boolean expression, Supplier<String> messageSupplier) {
        if (!expression) {
            throw new IllegalArgumentException(messageSupplier.get());
        }
    }

    /**
     * Asserts that the given {@code expression} is false. If not, an IllegalArgumentException is thrown.
     *
     * @param expression      the state validation expression
     * @param messageSupplier Supplier of the exception message if the expression evaluates to true
     */
    public static void isFalse(boolean expression, Supplier<String> messageSupplier) {
        if (expression) {
            throw new IllegalArgumentException(messageSupplier.get());
        }
    }

    /**
     * Assert that the given {@code value} is not {@code null}. If not, an IllegalArgumentException is
     * thrown.
     *
     * @param value           the value not to be {@code null}
     * @param messageSupplier Supplier of the exception message if the assertion fails
     */
    public static void notNull(Object value, Supplier<String> messageSupplier) {
        isTrue(value != null, messageSupplier);
    }

}
