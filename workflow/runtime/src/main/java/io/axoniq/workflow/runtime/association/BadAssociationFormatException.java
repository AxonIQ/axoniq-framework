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
package io.axoniq.workflow.runtime.association;

import jakarta.annotation.Nonnull;
import org.axonframework.common.annotation.Internal;

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
    @Internal
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
                "Illegal operator used in association string "
                        + conditionString + ". Supported operators are "
                        + String.join(", ", operators)
        );
    }


    /**
     * Constructs the exception.
     *
     * @param conditionString condition string that caused the exception.
     * @return bad association format exception.
     */
    public static BadAssociationFormatException wrongFormat(@Nonnull String conditionString) {
        throw new BadAssociationFormatException(
                "Illegal format in association string "
                        + conditionString + ". It should be <qualifier>:<path><operator><value>");
    }

    /**
     * Constructs the exception for unsupported qualifiers.
     *
     * @param qualifiers      supported qualifiers
     * @param conditionString condition string that caused the exception
     * @return bad association format exception
     */
    public static BadAssociationFormatException unsupportedQualifier(
            @Nonnull Set<String> qualifiers,
            @Nonnull String conditionString
    ) {
        return new BadAssociationFormatException(
                "Illegal qualifier used in association string "
                        + conditionString + ". Supported qualifiers are "
                        + String.join(", ", qualifiers)
        );
    }
}
