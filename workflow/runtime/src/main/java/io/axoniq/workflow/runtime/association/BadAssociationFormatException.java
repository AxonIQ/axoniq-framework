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
package io.axoniq.workflow.runtime.association;

import jakarta.annotation.Nonnull;

import java.util.Set;

/**
 * Exception thrown when an association string is in the wrong format or uses unsupported operators.
 * @since 1.0.0
 * @author Simon Zambrovski
 */
public class BadAssociationFormatException extends RuntimeException {

    /**
     * Constructs the exception.
     *
     * @param message message describing the error.
     */
    BadAssociationFormatException(String message) {
        super(message);
    }

    /**
     * Constructs the exception.
     *
     * @param operators       list of supported operators.
     * @param conditionString condition string that caused the exception.
     * @return bad association format exception.
     */
    public static BadAssociationFormatException unsupportedOperator(
            @Nonnull Set<String> operators,
            @Nonnull String conditionString) {
        return new BadAssociationFormatException(
                "Illegal operator used in annotated start condition string "
                        + conditionString + ". Supported operators are "
                        + String.join(", ", operators)
        );
    }


    /**
     * Constructs the exception.
     *
     * @param operator        found operator.
     * @param conditionString condition string that caused the exception.
     * @return bad association format exception.
     */
    public static BadAssociationFormatException wrongFormat(
            @Nonnull String operator,
            @Nonnull String conditionString
    ) {
        throw new BadAssociationFormatException(
                "Illegal format in start condition string "
                        + conditionString + ". It should be <key>"
                        + operator + "<value>");
    }
}
