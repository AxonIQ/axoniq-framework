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

package org.axonframework.messaging.core.annotation;

import org.axonframework.common.AxonConfigurationException;

import java.lang.reflect.Member;

/**
 * Thrown when an {@link MessageHandler} annotated method was found that does not conform to the rules that apply to
 * those methods.
 *
 * @author Allard Buijze
 * @since 2.0.0
 */
public class UnsupportedHandlerException extends AxonConfigurationException {

    private final Member violatingMethod;

    /**
     * Initialize the exception with a {@code message} and the {@code violatingMethod}.
     *
     * @param message         a descriptive message of the violation
     * @param violatingMethod the method that violates the rules of annotated Event Handlers
     */
    public UnsupportedHandlerException(String message, Member violatingMethod) {
        super(message);
        this.violatingMethod = violatingMethod;
    }

    /**
     * A reference to the method that violated the event handler rules.
     *
     * @return the method that violated the event handler rules
     */
    public Member getViolatingMethod() {
        return violatingMethod;
    }
}
