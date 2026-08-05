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

package io.axoniq.framework.messaging.multitenancy;

import io.axoniq.license.entitlement.AxoniqAddon;

/**
 * {@link AxoniqAddon} implementation for the Axoniq Framework Multi-tenancy. Allows the module to be
 * detected and logged at startup, and to be included in the license entitlement system.
 *
 * @author Mitchell Herrijgers
 * @since 5.1.0
 */
public class MultiTenancyAxoniqAddon implements AxoniqAddon {

    static final String IDENTIFIER = "framework.multi_tenancy";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public String name() {
        return "Axoniq Framework - Multi-tenancy";
    }
}
