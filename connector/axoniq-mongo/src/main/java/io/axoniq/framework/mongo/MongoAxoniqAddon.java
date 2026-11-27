/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.mongo;

import io.axoniq.framework.mongo.token.MongoTokenStore;
import io.axoniq.license.entitlement.AxoniqAddon;

/**
 * {@link AxoniqAddon} for the Axoniq MongoDB module, making its components part of the license entitlement system and
 * detectable at startup.
 * <p>
 * The {@link MongoTokenStore} registers this addon upon construction, so using it requires the MongoDB
 * entitlement.
 *
 * @author Steven van Beelen
 * @since 5.5.0
 */
public class MongoAxoniqAddon implements AxoniqAddon {

    /**
     * The identifier of this addon within the entitlement system.
     */
    public static final String IDENTIFIER = "framework.mongo";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public String name() {
        return "Axoniq Framework - MongoDB";
    }
}
