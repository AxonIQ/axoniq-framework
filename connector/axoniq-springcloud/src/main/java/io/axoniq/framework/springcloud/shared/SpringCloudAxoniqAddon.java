/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.springcloud.shared;

import io.axoniq.license.entitlement.AxoniqAddon;

/**
 * The {@link AxoniqAddon} identifying the Spring Cloud connector, so that it is detected and logged at start-up and
 * accounted for by the license entitlement system.
 * <p>
 * Registered with the entitlement system by
 * {@link io.axoniq.framework.springcloud.command.SpringCloudCommandBusConnector} and
 * {@link io.axoniq.framework.springcloud.query.SpringCloudQueryBusConnector} on construction, and discoverable
 * through {@code META-INF/services/io.axoniq.license.entitlement.AxoniqAddon}.
 *
 * @author Allard Buijze
 * @since 5.4.0
 */
public class SpringCloudAxoniqAddon implements AxoniqAddon {

    /**
     * The identifier this addon is claimed under.
     */
    @SuppressWarnings("java:S1845") // ignore potential naming conflict with identifier()
    public static final String IDENTIFIER = "framework.spring_cloud";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public String name() {
        return "Axoniq Framework - Spring Cloud";
    }
}
