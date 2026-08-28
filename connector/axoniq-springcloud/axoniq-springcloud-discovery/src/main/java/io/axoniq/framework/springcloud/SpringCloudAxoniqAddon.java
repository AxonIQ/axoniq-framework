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

package io.axoniq.framework.springcloud;

import io.axoniq.license.entitlement.AxoniqAddon;

/**
 * The {@link AxoniqAddon} identifying the Spring Cloud connector, so that it is detected and logged at start-up and
 * accounted for by the licence entitlement system.
 * <p>
 * Registered with the entitlement system by {@link SpringCloudCommandBusConnector} on construction, and discoverable
 * through {@code META-INF/services/io.axoniq.license.entitlement.AxoniqAddon}.
 *
 * @author Allard Buijze
 * @since 5.4.0
 */
public class SpringCloudAxoniqAddon implements AxoniqAddon {

    /**
     * The identifier this addon is claimed under.
     */
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
