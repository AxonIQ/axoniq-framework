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

package io.axoniq.framework.messaging.deadletter;

import io.axoniq.license.entitlement.AxoniqAddon;

/**
 * {@link AxoniqAddon} implementation for the Axoniq Framework Sequenced Dead-Letter Queue. Allows the module to be
 * detected and logged at startup, and to be included in the license entitlement system.
 *
 * @author Mateusz Nowak
 * @since 5.1.0
 */
public class SequencedDeadLetterAxoniqAddon implements AxoniqAddon {

    public static final String IDENTIFIER = "framework.dead-letter";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public String name() {
        return "Axoniq Framework - Sequenced Dead-Letter Queue";
    }
}
