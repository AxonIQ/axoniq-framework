/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.license.entitlement.AxoniqAddon;

/**
 * Definition of the {@link AxoniqAddon} for licensing, loaded through SPI during boot to log its existence to the user,
 * and warn that a license is needed.
 *
 * @author Mitchell Herrijgers
 * @since 5.4.0
 */
public class WorkflowAxoniqAddon implements AxoniqAddon {

    static final String IDENTIFIER = "framework.workflow";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public String name() {
        return "Axoniq Framework - Workflow";
    }
}
