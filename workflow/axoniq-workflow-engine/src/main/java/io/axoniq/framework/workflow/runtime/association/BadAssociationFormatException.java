/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.association;

import org.axonframework.common.annotation.Internal;

import java.util.Set;

/**
 * Exception thrown when an association string is in the wrong format or uses unsupported operators.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
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
            Set<String> operators,
            String conditionString) {
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
    public static BadAssociationFormatException wrongFormat(String conditionString) {
        return new BadAssociationFormatException(
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
            Set<String> qualifiers,
            String conditionString
    ) {
        return new BadAssociationFormatException(
                "Illegal qualifier used in association string "
                        + conditionString + ". Supported qualifiers are "
                        + String.join(", ", qualifiers)
        );
    }
}
